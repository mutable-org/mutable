#include "parse/Sema.hpp"

#include <cstdint>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/io/Reader.hpp>
#include <mutable/Options.hpp>
#include <sstream>
#include <unordered_map>


using namespace m;
using namespace m::ast;


std::unique_ptr<DatabaseCommand> Sema::analyze(std::unique_ptr<ast::Command> ast)
{
    (*this)(*ast); // perform semantic analysis
    if (command_)
        command_->ast(std::move(ast)); // move AST into DatabaseCommand instance
    return std::move(command_);
}

bool Sema::is_nested() const
{
    return contexts_.size() > 1;
}

/*----------------------------------------------------------------------------------------------------------------------
 * Sema Designator Helpers
 *--------------------------------------------------------------------------------------------------------------------*/

std::unique_ptr<Designator> Sema::create_designator(const char *name, Token tok, const Expr &target)
{
    auto new_designator = std::make_unique<Designator>(tok, Token(), Token(tok.pos, name, TK_IDENTIFIER));
    new_designator->type_ = target.type();
    new_designator->target_ = &target;
    return new_designator;
}

std::unique_ptr<Designator> Sema::create_designator(const Expr &name, const Expr &target, bool drop_table_name)
{
    auto &C = Catalog::Get();

    std::unique_ptr<Designator> new_designator;
    if (auto d = cast<const Designator>(&name)) {
        Token table_name = drop_table_name ? Token() : d->table_name; // possibly drop table name
        new_designator = std::make_unique<Designator>(d->tok, table_name, d->attr_name); // copy of `name`
    } else {
        oss.str("");
        oss << name; // stringify `name`
        Token tok(target.tok.pos, C.pool(oss.str().c_str()), TK_DEC_INT); // fresh identifier
        new_designator = std::make_unique<Designator>(tok);
    }

    new_designator->type_ = target.type();
    new_designator->target_ = &target;
    return new_designator;
}

void Sema::replace_by_fresh_designator_to(std::unique_ptr<Expr> &to_replace, const Expr &target)
{
    auto new_designator = create_designator(*to_replace, target);
    to_replace = std::move(new_designator);
}


/*----------------------------------------------------------------------------------------------------------------------
 * Other Sema Helpers
 *--------------------------------------------------------------------------------------------------------------------*/

const char * Sema::make_unique_id_from_binding_path(context_stack_t::reverse_iterator current_ctx,
                                                    context_stack_t::reverse_iterator binding_ctx)
{
    if (current_ctx == binding_ctx) return nullptr;

    oss.str("");
    for (auto it = current_ctx; it != binding_ctx; ++it) {
        if (it != current_ctx) oss << '.';
        M_insist((*it)->alias, "nested queries must have an alias");
        oss << (*it)->alias;
    }

    auto &C = Catalog::Get();
    return C.pool(oss.str().c_str());
}

bool Sema::is_composable_of(const ast::Expr &expr,
                            const std::vector<std::reference_wrapper<ast::Expr>> components)
{
    auto recurse = overloaded {
        [&](const ast::Designator &d) -> bool {
            return d.contains_free_variables() or d.is_identifier(); // identifiers are implicitly composable, as they never refer into a table XXX do we have to check the target?
        },
        [&](const ast::FnApplicationExpr &e) -> bool {
            if (not is_composable_of(*e.fn, components)) return false;
            for (auto &arg : e.args) {
                if (not is_composable_of(*arg, components))
                    return false;
            }
            return true;
        },
        [&](const ast::UnaryExpr &e) -> bool { return is_composable_of(*e.expr, components); },
        [&](const ast::BinaryExpr &e) -> bool {
            return is_composable_of(*e.lhs, components) and is_composable_of(*e.rhs, components);
        },
        [](auto&) -> bool { return true; },
    };

    for (auto c : components)
        if (expr == c.get()) return true; // syntactically equivalent to a component
    return visit(recurse, expr, m::tag<m::ast::ConstASTExprVisitor>()); // attempt to recursively compose expr
}

void Sema::compose_of(std::unique_ptr<ast::Expr> &ptr, const std::vector<std::reference_wrapper<ast::Expr>> components)
{
    auto recurse = overloaded {
        [&](const ast::Designator &d) -> bool {
            return d.contains_free_variables() or d.is_identifier(); // identifiers are implicitly composable, as they never refer into a table XXX do we have to check the target?
        },
        [&](const ast::FnApplicationExpr &e) -> bool {
            if (not is_composable_of(*e.fn, components)) return false;
            for (auto &arg : e.args) {
                if (not is_composable_of(*arg, components))
                    return false;
            }
            return true;
        },
        [&](const ast::UnaryExpr &e) -> bool { return is_composable_of(*e.expr, components); },
        [&](const ast::BinaryExpr &e) -> bool {
            return is_composable_of(*e.lhs, components) and is_composable_of(*e.rhs, components);
        },
        [](auto&) -> bool { return true; },
    };

    for (auto c : components) {
        if (*ptr == c.get())
            replace_by_fresh_designator_to(/* to_replace= */ ptr, /* target= */ c.get());
    }
    visit(recurse, *ptr, m::tag<m::ast::ConstASTExprVisitor>()); // attempt to recursively compose expr
}

/*===== Expr =========================================================================================================*/

void Sema::operator()(ErrorExpr &e)
{
    e.type_ = Type::Get_Error();
}

void Sema::operator()(Designator &e)
{
    Catalog &C = Catalog::Get();
    SemaContext *current_ctx = &get_context();

    oss.str("");
    oss << e;
    auto pooled_name = C.pool(oss.str().c_str());

    /*----- In a stage after SELECT, check whether the `Designator` refers to a value produced by SELECT. -----*/
    if (current_ctx->stage > SemaContext::S_Select) {
        auto [begin, end] = current_ctx->results.equal_range(pooled_name);
        if (std::distance(begin, end) > 1) {
            diag.e(e.tok.pos) << "Designator " << e << " is ambiguous, multiple occurrences in SELECT clause.\n";
            e.type_ = Type::Get_Error();
            return;
        } else if (std::distance(begin, end) == 1) {
            SemaContext::result_t &result = begin->second;
            if (auto d = cast<Designator>(&result.expr()); d and not result.alias) // target is a designator w/o
                e.table_name.text = d->table_name.text;                            // explicit alias
            e.type_ = result.expr().type();
            e.target_ = &result.expr();
            return;
        }
    }

    /*----- In a stage after GROUP BY, check whether the entire expression refers to a grouping key. -----*/
    if (current_ctx->stage > SemaContext::S_GroupBy and not current_ctx->grouping_keys.empty()) {
        auto [begin, end] = current_ctx->grouping_keys.equal_range(pooled_name);
        if (std::distance(begin, end) > 1) {
            diag.e(e.tok.pos) << "Designator " << e << " is ambiguous, multiple occurrences in GROUP BY clause.\n";
            e.type_ = Type::Get_Error();
            return;
        } else if (std::distance(begin, end) == 1) {
            auto &referenced_expr = begin->second.get();
            e.type_ = referenced_expr.type();
            if (auto pt = cast<const PrimitiveType>(e.type()))
                e.type_ = pt->as_scalar();
            else
                M_insist(e.type()->is_error(), "grouping expression must be of primitive type");
            e.target_ = &referenced_expr;
            return;
        }
    }

    /*----- Designator was neither a reference to a SELECT or GROUP BY expression. -----*/
    decltype(contexts_)::reverse_iterator found_ctx; // the context where the designator is found
    bool is_result = false;

    /* If the designator references an attribute of a table, search for it. */
    if (e.table_name) {
        /* Find the source table first and then locate the target inside this table. */
        SemaContext::source_type src;

        /* Search all contexts, starting with the innermost and advancing outwards. */
        auto it = contexts_.rbegin();
        for (auto end = contexts_.rend(); it != end; ++it) {
            try {
                src = (*it)->sources.at(e.table_name.text).first;
                break;
            } catch (std::out_of_range) {
                /* The source is not found in this context so iterate over the entire stack. */
            }
        }

        if (it == contexts_.rend()) {
            diag.e(e.table_name.pos) << "Source table " << e.table_name.text
                                     << " not found. Maybe you forgot to specify it in the FROM clause?\n";
            e.type_ = Type::Get_Error();
            return;
        }
        found_ctx = it;

        /* Find the target inside the source table. */
        Designator::target_type target;
        if (auto ref = std::get_if<std::reference_wrapper<const Table>>(&src)) {
            const Table &tbl = ref->get();
            /* Find the attribute inside the table. */
            try {
                target = &tbl.at(e.attr_name.text); // we found an attribute of that name in the source tables
            } catch (std::out_of_range) {
                diag.e(e.attr_name.pos) << "Table " << e.table_name.text << " has no attribute " << e.attr_name.text
                                        << ".\n";
                e.type_ = Type::Get_Error();
                return;
            }
        } else if (auto T = std::get_if<SemaContext::named_expr_table>(&src)) {
            const SemaContext::named_expr_table &tbl = *T;
            /* Find expression inside named expression table. */
            auto [begin, end] = tbl.equal_range(e.attr_name.text);
            if (begin == end) {
                diag.e(e.attr_name.pos) << "Source " << e.table_name.text << " has no attribute " << e.attr_name.text
                                        << ".\n";
                e.type_ = Type::Get_Error();
                return;
            } else if (std::distance(begin, end) > 1) {
                diag.e(e.attr_name.pos) << "Source " << e.table_name.text << " has multiple attributes "
                                        << e.attr_name.text << ".\n";
                e.type_ = Type::Get_Error();
                return;
            } else {
                target = &begin->second.first.get();
            }
        } else {
            M_unreachable("invalid variant");
        }
        e.target_ = target;
        e.set_binding_depth(std::distance(contexts_.rbegin(), found_ctx));
        e.unique_id_ = make_unique_id_from_binding_path(contexts_.rbegin(), found_ctx);
    } else {
        /* No table name was specified.  The designator references either a result or a named expression.  Search the
         * named expressions first, because they overrule attribute names. */
        if (auto [begin, end] = current_ctx->results.equal_range(e.attr_name.text);
            current_ctx->stage > SemaContext::S_Select and std::distance(begin, end) >= 1)
        {
            /* Found a named expression. */
            if (std::distance(begin, end) > 1) {
                diag.e(e.attr_name.pos) << "Attribute specifier " << e.attr_name.text << " is ambiguous.\n";
                e.type_ = Type::Get_Error();
                return;
            } else {
                M_insist(std::distance(begin, end) == 1);
                SemaContext::result_t &result = begin->second;
                e.target_ = &result.expr();
                if (auto d = cast<Designator>(&result.expr()); d and d->attr_name.text == e.attr_name.text)
                    e.table_name.text = d->table_name.text;
                e.set_binding_depth(0); // bound by the current (innermost) context ⇒ bound variable
                is_result = true;
                found_ctx = contexts_.rbegin(); // iterator to the current context
            }
        } else {
            /* Since no table was explicitly specified, we must search *all* sources for the attribute. */
            Designator::target_type target;
            const char *alias = nullptr;

            /* Search all contexts, starting with the innermost and advancing outwards. */
            for (auto it = contexts_.rbegin(), end = contexts_.rend(); it != end; ++it) {
                for (auto &src : (*it)->sources) {
                    if (auto ref = std::get_if<std::reference_wrapper<const Table>>(&src.second.first)) {
                        const Table &tbl = ref->get();
                        try {
                            const Attribute &A = tbl.at(e.attr_name.text);
                            if (not std::holds_alternative<std::monostate>(target)) {
                                /* ambiguous attribute name */
                                diag.e(e.attr_name.pos) << "Attribute specifier " << e.attr_name.text
                                                        << " is ambiguous.\n";
                                // TODO print names of conflicting tables
                                e.type_ = Type::Get_Error();
                                return;
                            } else {
                                target = &A; // we found an attribute of that name in the source tables
                                alias = src.first;
                                found_ctx = it;
                            }
                        } catch (std::out_of_range) {
                            /* This source table has no attribute of that name.  OK, continue. */
                        }
                    } else if (auto T = std::get_if<SemaContext::named_expr_table>(&src.second.first)) {
                        const SemaContext::named_expr_table &tbl = *T;
                        auto [begin, end] = tbl.equal_range(e.attr_name.text);
                        if (begin == end) {
                            /* This source table has no attribute of that name.  OK, continue. */
                        } else if (std::distance(begin, end) > 1) {
                            diag.e(e.attr_name.pos) << "Attribute specifier " << e.attr_name.text << " is ambiguous.\n";
                            e.type_ = Type::Get_Error();
                            return;
                        } else {
                            M_insist(std::distance(begin, end) == 1);
                            if (not std::holds_alternative<std::monostate>(target)) {
                                /* ambiguous attribute name */
                                diag.e(e.attr_name.pos) << "Attribute specifier " << e.attr_name.text
                                                        << " is ambiguous.\n";
                                // TODO print names of conflicting tables
                                e.type_ = Type::Get_Error();
                                return;
                            } else {
                                target = &begin->second.first.get(); // we found an attribute of that name in the source tables
                                alias = src.first;
                                found_ctx = it;
                            }
                        }
                    } else {
                        M_unreachable("invalid variant");
                    }
                }
                /* If we found target of the designator, abort searching contexts further outside. */
                if (not std::holds_alternative<std::monostate>(target))
                    break;
            }

            /* If this designator could not be resolved, emit an error and abort further semantic analysis. */
            if (std::holds_alternative<std::monostate>(target)) {
                diag.e(e.attr_name.pos) << "Attribute " << e.attr_name.text << " not found.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            e.target_ = target;
            e.table_name.text = alias; // set the deduced table name of this designator
            e.set_binding_depth(std::distance(contexts_.rbegin(), found_ctx));
            e.unique_id_ = make_unique_id_from_binding_path(contexts_.rbegin(), found_ctx);
        }
    }

    /* Compute the type of this designator based on the referenced source. */
    M_insist(e.target_.index() != 0);
    struct get_type {
        const Type * operator()(std::monostate&) const { M_unreachable("target not set"); }
        const Type * operator()(const Attribute *attr) const { return attr->type; }
        const Type * operator()(const Expr *expr) const { return expr->type_; }
    };
    const PrimitiveType *pt = cast<const PrimitiveType>(std::visit(get_type(), e.target_));
    e.type_ = pt;

    if (not is_result)
        e.type_ = pt->as_vectorial();

    /* Check if any context between current context and found context is in stage `S_FROM`. */
    for (auto it = contexts_.rbegin(); it != found_ctx; ++it) {
        if ((*it)->stage == SemaContext::S_From) {
            /* The designator is correlated and occurs in a nested query in the FROM. Emit an error. */
            diag.e(e.attr_name.pos) << "Correlated attributes are not allowed in the FROM clause.\n";
            e.type_ = Type::Get_Error();
            return;
        }
    }

    switch ((*found_ctx)->stage) {
        default:
            M_unreachable("designator not allowed in this stage");

        case SemaContext::S_From:
            /* The designator is correlated and occurs in a nested query in the FROM. Emit an error. */
            diag.e(e.attr_name.pos) << "Correlated attributes are not allowed in the FROM clause.\n";
            e.type_ = Type::Get_Error();
            return;

        case SemaContext::S_Where:
        case SemaContext::S_GroupBy:
        case SemaContext::S_Having:
        case SemaContext::S_Select:
        case SemaContext::S_OrderBy:
            /* The type of the attribute remains unchanged.  Nothing to be done. */
            break;
    }
}

void Sema::operator()(Constant &e)
{
    int base = 8; // for integers
    switch (e.tok.type) {
        default:
            M_unreachable("a constant must be one of the types below");

        case TK_Null:
            e.type_ = Type::Get_None();
            break;

        case TK_STRING_LITERAL:
            e.type_ = Type::Get_Char(Type::TY_Scalar, interpret(e.tok.text).length());
            break;

        case TK_DATE: {
            int year, month, day;
            sscanf(e.tok.text, "d'%d-%d-%d'", &year, &month, &day);
            if (year == 0) {
                diag.e(e.tok.pos) << e << " has invalid year (after year -1 (1 BC) follows year 1 (1 AD)).\n";
                e.type_ = Type::Get_Error();
                return;
            }
            if (month < 1 or month > 12) {
                diag.e(e.tok.pos) << e << " has invalid month.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            if (day < 1 or (month == 2 and day > 29)
                or ((month == 4 or month == 6 or month == 9 or month == 11) and day > 30)
                or ((month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12)
                    and day > 31)) {
                diag.e(e.tok.pos) << e << " has invalid day.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            e.type_ = Type::Get_Date(Type::TY_Scalar);
            break;
        }

        case TK_DATE_TIME: {
            int year, month, day, hour, minute, second;
            sscanf(e.tok.text, "d'%d-%d-%d %d:%d:%d'", &year, &month, &day, &hour, &minute, &second);
            if (year == 0) {
                diag.e(e.tok.pos) << e << " has invalid year (after year -1 (1 BC) follows year 1 (1 AD)).\n";
                e.type_ = Type::Get_Error();
                return;
            }
            if (month < 1 or month > 12) {
                diag.e(e.tok.pos) << e << " has invalid month.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            if (day < 1 or (month == 2 and day > 29)
                or ((month == 4 or month == 6 or month == 9 or month == 11) and day > 30)
                or ((month == 1 or month == 3 or month == 5 or month == 7 or month == 8 or month == 10 or month == 12)
                    and day > 31)) {
                diag.e(e.tok.pos) << e << " has invalid day.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            M_insist(hour >= 0);
            if (hour > 23) {
                diag.e(e.tok.pos) << e << " has invalid hour.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            M_insist(minute >= 0);
            if (minute > 59) {
                diag.e(e.tok.pos) << e << " has invalid minute.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            M_insist(second >= 0);
            if (second > 59) {
                diag.e(e.tok.pos) << e << " has invalid second.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            e.type_ = Type::Get_Datetime(Type::TY_Scalar);
            break;
        }

        case TK_True:
        case TK_False:
            e.type_ = Type::Get_Boolean(Type::TY_Scalar);
            break;

        case TK_HEX_INT:
            base += 6;
        case TK_DEC_INT:
            base += 2;
        case TK_OCT_INT: {
            int64_t value = strtol(e.tok.text, nullptr, base);
            if (value == int32_t(value))
                e.type_ = Type::Get_Integer(Type::TY_Scalar, 4);
            else
                e.type_ = Type::Get_Integer(Type::TY_Scalar, 8);
            break;
        }

        case TK_DEC_FLOAT:
        case TK_HEX_FLOAT:
            e.type_ = Type::Get_Double(Type::TY_Scalar); // TODO: 32-bit floating-point constants
            break;
    }
}

void Sema::operator()(FnApplicationExpr &e)
{
    SemaContext &Ctx = get_context();
    Catalog &C = Catalog::Get();

    /* Analyze function name. */
    auto d = cast<Designator>(e.fn.get());
    if (not d or not d->is_identifier()) {
        diag.e(d->attr_name.pos) << *d << " is not a valid function.\n";
        d->type_ = e.type_ = Type::Get_Error();
        return;
    }
    M_insist(bool(d));
    M_insist(not d->type_, "This identifier has already been analyzed.");

    /* Analyze arguments. */
    for (auto &arg : e.args)
        (*this)(*arg);

    /* Lookup the function. */
    if (C.has_database_in_use()) {
        const auto &DB = C.get_database_in_use();
        try {
            e.func_ = DB.get_function(d->attr_name.text);
        } catch (std::out_of_range) {
            diag.e(d->attr_name.pos) << "Function " << d->attr_name.text << " is not defined in database " << DB.name
                << ".\n";
            e.type_ = Type::Get_Error();
            return;
        }
    } else {
        try {
            e.func_ = C.get_function(d->attr_name.text);
        } catch (std::out_of_range) {
            diag.e(d->attr_name.pos) << "Function " << d->attr_name.text << " is not defined.\n";
            e.type_ = Type::Get_Error();
            return;
        }
    }
    M_insist(e.func_);

    /* Infer the type of the function.  Functions are defined in an abstract way, where the type of the parameters is
     * not specified.  We must infer the parameter types and the return type of the function. */
    switch (e.func_->fnid) {
        default:
            M_unreachable("Function not implemented");

        case Function::FN_UDF:
            diag.e(d->attr_name.pos) << "User-defined functions are not yet supported.\n";
            d->type_ = e.type_ = Type::Get_Error();
            return;

        case Function::FN_MIN:
        case Function::FN_MAX:
        case Function::FN_SUM:
        case Function::FN_AVG: {
            if (e.args.size() == 0) {
                diag.e(d->attr_name.pos) << "Missing argument for aggregate " << *d << ".\n";
                d->type_ = e.type_ = Type::Get_Error();
                return;
            }
            if (e.args.size() > 1) {
                diag.e(d->attr_name.pos) << "Too many arguments for aggregate " << *d << ".\n";
                d->type_ = e.type_ = Type::Get_Error();
                return;
            }
            M_insist(e.args.size() == 1);
            auto &arg = *e.args[0];
            if (arg.type()->is_error()) {
                /* skip argument of error type */
                d->type_ = e.type_ = Type::Get_Error();
                return;
            }
            if (not arg.type()->is_numeric()) {
                /* invalid argument type */
                diag.e(d->attr_name.pos) << "Argument of aggregate function must be of numeric type.\n";
                d->type_ = e.type_ = Type::Get_Error();
                return;
            }
            M_insist(arg.type()->is_numeric());
            const Numeric *arg_type = cast<const Numeric>(arg.type());
            if (not arg_type->is_vectorial()) {
                diag.w(d->attr_name.pos) << "Argument of aggregate is not of vectorial type.  "
                                            "(Aggregates over scalars are discouraged.)\n";
            }

            switch (e.func_->fnid) {
                default:
                    M_unreachable("Invalid function");

                case Function::FN_MIN:
                case Function::FN_MAX: {
                    /* MIN/MAX maintain type */
                    e.type_ = arg_type->as_scalar();
                    d->type_ = Type::Get_Function(e.type_, { arg.type() });
                    break;
                }

                case Function::FN_AVG: {
                    /* AVG always uses double precision floating-point */
                    e.type_ = Type::Get_Double(Type::TY_Scalar);
                    d->type_ = Type::Get_Function(e.type_, { arg.type() });
                    break;
                }

                case Function::FN_SUM: {
                    /* SUM can overflow.  Always assume type of highest precision. */
                    switch (arg_type->kind) {
                        case Numeric::N_Int:
                            e.type_ = Type::Get_Integer(Type::TY_Scalar, 8);
                            break;

                        case Numeric::N_Float:
                            e.type_ = Type::Get_Double(Type::TY_Scalar);
                            break;

                        case Numeric::N_Decimal:
                            e.type_ = Type::Get_Decimal(Type::TY_Scalar, Numeric::MAX_DECIMAL_PRECISION,
                                                        arg_type->scale);
                            break;
                    }
                    d->type_ = Type::Get_Function(e.type(), { e.type() });
                    break;
                }
            }
            break;
        }

        case Function::FN_COUNT: {
            if (e.args.size() > 1) {
                diag.e(d->attr_name.pos) << "Too many arguments for aggregate " << *d << ".\n";
                e.type_ = Type::Get_Error();
                return;
            }

            /* TODO If argument is given, check whether it can be NULL.  If not, COUNT(arg) == COUNT(*) */

            e.type_ = Type::Get_Integer(Type::TY_Scalar, 8);
            d->type_ = Type::Get_Function(e.type_, {});
            break;
        }

        case Function::FN_ISNULL: {
            if (e.args.size() == 0) {
                diag.e(d->attr_name.pos) << "Missing argument for aggregate " << *d << ".\n";
                e.type_ = Type::Get_Error();
                return;
            }
            if (e.args.size() > 1) {
                diag.e(d->attr_name.pos) << "Too many arguments for aggregate " << *d << ".\n";
                e.type_ = Type::Get_Error();
                return;
            }
            M_insist(e.args.size() == 1);
            auto &arg = *e.args[0];

            if (arg.type()->is_error()) {
                e.type_ = Type::Get_Error();
                return;
            }
            const PrimitiveType *arg_type = cast<const PrimitiveType>(arg.type());
            if (not arg_type) {
                diag.e(d->attr_name.pos) << "Function ISNULL can only be applied to expressions of primitive type.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            d->type_ = Type::Get_Function(Type::Get_Boolean(arg_type->category), { arg.type() });
            e.type_= Type::Get_Boolean(arg_type->category);
            break;
        }
    }

    M_insist(d->type_);
    M_insist(d->type()->is_error() or cast<const FnType>(d->type()));
    M_insist(e.type_);
    M_insist(not e.type()->is_error());
    M_insist(e.type()->is_primitive());

    switch (Ctx.stage) {
        case SemaContext::S_From:
            M_unreachable("Function application in FROM clause is impossible");

        case SemaContext::S_Where:
            if (e.func_->is_aggregate()) {
                diag.e(d->attr_name.pos) << "Aggregate functions are not allowed in WHERE clause.\n";
                return;
            }
            break;

        case SemaContext::S_GroupBy:
            if (e.func_->is_aggregate()) {
                diag.e(d->attr_name.pos) << "Aggregate functions are not allowed in GROUP BY clause.\n";
                return;
            }
            break;

        case SemaContext::S_Having:
            /* nothing to be done */
            break;

        case SemaContext::S_OrderBy:
            /* TODO */
            break;

        case SemaContext::S_Select:
            /* TODO */
            break;

        case SemaContext::S_Limit:
            /* TODO */
            break;
    }
}

void Sema::operator()(UnaryExpr &e)
{
    /* Analyze sub-expression. */
    (*this)(*e.expr);

    /* If the sub-expression is erroneous, so is this expression. */
    if (e.expr->type()->is_error()) {
        e.type_ = Type::Get_Error();
        return;
    }

    switch (e.op().type) {
        default:
            M_unreachable("invalid unary expression");

        case TK_Not:
            if (not e.expr->type()->is_boolean()) {
                diag.e(e.op().pos) << "Invalid expression " << e << " must be boolean.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            break;

        case TK_PLUS:
        case TK_MINUS:
        case TK_TILDE:
            if (not e.expr->type()->is_numeric()) {
                diag.e(e.op().pos) << "Invalid expression " << e << " must be numeric.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            break;
    }

    e.type_ = e.expr->type();
}

void Sema::operator()(BinaryExpr &e)
{
    /* Analyze sub-expressions. */
    (*this)(*e.lhs);
    (*this)(*e.rhs);

    /* If at least one of the sub-expressions is erroneous, so is this expression. */
    if (e.lhs->type()->is_error() or e.rhs->type()->is_error()) {
        e.type_ = Type::Get_Error();
        return;
    }

    /* Validate that lhs and rhs are compatible with binary operator. */
    switch (e.op().type) {
        default:
            M_unreachable("Invalid binary operator.");

        /* Arithmetic operations are only valid for numeric types.  Compute the type of the binary expression that is
         * precise enough.  */
        case TK_PLUS:
        case TK_MINUS:
        case TK_ASTERISK:
        case TK_SLASH:
        case TK_PERCENT: {
            /* Verify that both operands are of numeric type. */
            const Numeric *ty_lhs = cast<const Numeric>(e.lhs->type());
            const Numeric *ty_rhs = cast<const Numeric>(e.rhs->type());
            if (not ty_lhs or not ty_rhs) {
                diag.e(e.op().pos) << "Invalid expression " << e << ", operands must be of numeric type.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            M_insist(ty_lhs);
            M_insist(ty_rhs);

            /* Compute type of the binary expression. */
            e.type_ = e.common_operand_type = arithmetic_join(ty_lhs, ty_rhs);
            break;
        }

        case TK_DOTDOT: {
            /* Concatenation of two strings. */
            auto ty_lhs = cast<const CharacterSequence>(e.lhs->type());
            auto ty_rhs = cast<const CharacterSequence>(e.rhs->type());
            if (not ty_lhs or not ty_rhs) {
                diag.e(e.op().pos) << "Invalid expression " << e << ", concatenation requires string operands.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            M_insist(ty_lhs);
            M_insist(ty_rhs);

            /* Scalar and scalar yield a scalar.  Otherwise, expression yields a vectorial. */
            Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

            e.type_ = Type::Get_Char(c, ty_lhs->length + ty_rhs->length);
            break;
        }

        case TK_LESS:
        case TK_LESS_EQUAL:
        case TK_GREATER:
        case TK_GREATER_EQUAL: {
            if (auto ty_lhs = cast<const Numeric>(e.lhs->type())) {
                /* Verify that both operands are of numeric type. */
                auto ty_rhs = cast<const Numeric>(e.rhs->type());
                if (not ty_lhs or not ty_rhs) {
                    diag.e(e.op().pos) << "Invalid expression " << e << ", both operands must be of numeric type.\n";
                    e.type_ = Type::Get_Error();
                    return;
                }
                M_insist(ty_lhs);
                M_insist(ty_rhs);

                /* Scalar and scalar yield a scalar.  Otherwise, expression yields a vectorial. */
                Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

                /* Comparisons always have boolean type. */
                e.type_ = Type::Get_Boolean(c);
                e.common_operand_type = arithmetic_join(ty_lhs, ty_rhs);
            } else if (auto ty_lhs = cast<const CharacterSequence>(e.lhs->type())) {
                /* Verify that both operands are character sequences. */
                auto ty_rhs = cast<const CharacterSequence>(e.rhs->type());
                if (not ty_lhs or not ty_rhs) {
                    diag.e(e.op().pos) << "Invalid expression " << e << ", both operands must be strings.\n";
                    e.type_ = Type::Get_Error();
                    return;
                }
                M_insist(ty_lhs);
                M_insist(ty_rhs);

                /* Scalar and scalar yield a scalar.  Otherwise, expression yields a vectorial. */
                Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

                /* Comparisons always have boolean type. */
                e.type_ = Type::Get_Boolean(c);
            } else if (auto ty_lhs = cast<const Date>(e.lhs->type())) {
                /* Verify that both operands are dates. */
                auto ty_rhs = cast<const Date>(e.rhs->type());
                if (not ty_lhs or not ty_rhs) {
                    diag.e(e.op().pos) << "Invalid expression " << e << ", both operands must be dates.\n";
                    e.type_ = Type::Get_Error();
                    return;
                }
                M_insist(ty_lhs);
                M_insist(ty_rhs);

                /* Scalar and scalar yield a scalar.  Otherwise, expression yields a vectorial. */
                Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

                /* Comparisons always have boolean type. */
                e.type_ = Type::Get_Boolean(c);
            } else if (auto ty_lhs = cast<const DateTime>(e.lhs->type())) {
                /* Verify that both operands are datetimes. */
                auto ty_rhs = cast<const DateTime>(e.rhs->type());
                if (not ty_lhs or not ty_rhs) {
                    diag.e(e.op().pos) << "Invalid expression " << e << ", both operands must be datetimes.\n";
                    e.type_ = Type::Get_Error();
                    return;
                }
                M_insist(ty_lhs);
                M_insist(ty_rhs);

                /* Scalar and scalar yield a scalar.  Otherwise, expression yields a vectorial. */
                Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

                /* Comparisons always have boolean type. */
                e.type_ = Type::Get_Boolean(c);
            } else {
                diag.e(e.op().pos) << "Invalid expression " << e << ", operator not supported for given operands.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            break;
        }

        case TK_EQUAL:
        case TK_BANG_EQUAL: {
            if (not is_comparable(e.lhs->type(), e.rhs->type())) {
                diag.e(e.op().pos) << "Invalid expression " << e << ", operands are incomparable.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            const PrimitiveType *ty_lhs = as<const PrimitiveType>(e.lhs->type());
            const PrimitiveType *ty_rhs = as<const PrimitiveType>(e.rhs->type());

            /* Scalar and scalar yield a scalar.  Otherwise, expression yields a vectorial. */
            Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

            /* Comparisons always have boolean type. */
            e.type_ = Type::Get_Boolean(c);
            if (auto ty_lhs = cast<const Numeric>(e.lhs->type()))
                e.common_operand_type = arithmetic_join(ty_lhs, as<const Numeric>(e.rhs->type()));
            break;
        }

        case TK_Like: {
            auto ty_lhs = cast<const CharacterSequence>(e.lhs->type());
            auto ty_rhs = cast<const CharacterSequence>(e.rhs->type());
            if (not ty_lhs or not ty_rhs) {
                diag.e(e.op().pos) << "Invalid expression " << e << ", operands must be character sequences.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            M_insist(ty_lhs);
            M_insist(ty_rhs);

            /* Scalar and scalar yield a scalar.  Otherwise, expression yields a vectorial. */
            Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

            /* Comparisons always have boolean type. */
            e.type_ = Type::Get_Boolean(c);
            break;
        }

        case TK_And:
        case TK_Or: {
            const Boolean *ty_lhs = cast<const Boolean>(e.lhs->type());
            const Boolean *ty_rhs = cast<const Boolean>(e.rhs->type());

            /* Both operands must be of boolean type. */
            if (not ty_lhs or not ty_rhs) {
                diag.e(e.op().pos) << "Invalid expression " << e << ", operands must be of boolean type.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            /* Scalar and scalar yield a scalar.  Otherwise, expression yields a vectorial. */
            Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

            /* Logical operators always have boolean type. */
            e.type_ = Type::Get_Boolean(c);
            break;
        }
    }
}

void Sema::operator()(QueryExpr &e)
{
    M_insist(is<SelectStmt>(*e.query), "nested statements are always select statements");

    SemaContext &Ctx = get_context();

    /* Evaluate the nested statement in a fresh sema context. */
    push_context(*e.query, e.alias());
    (*this)(*e.query);
    M_insist(not contexts_.empty());
    SemaContext inner_ctx = pop_context();

    /* TODO an EXISTS operator allows multiple results */
    if (1 != inner_ctx.results.size()) {
        diag.e(e.tok.pos) << "Invalid expression:\n" << e << ",\nnested statement must return a single column.\n";
        e.type_ = Type::Get_Error();
        return;
    }
    M_insist(1 == inner_ctx.results.size());
    Expr &res = inner_ctx.results.begin()->second.expr();

    if (not res.type()->is_primitive()) {
        diag.e(e.tok.pos) << "Invalid expression:\n" << e << ",\nnested statement must return a primitive value.\n";
        e.type_ = Type::Get_Error();
        return;
    }
    auto *pt = as<const PrimitiveType>(res.type_);
    e.type_ = pt;

    switch (Ctx.stage) {
        default: {
            diag.e(e.tok.pos) << "Nested statements are not allowed in this stage.\n";
            e.type_ = Type::Get_Error();
            return;
        }
        case SemaContext::S_Where:
        case SemaContext::S_Having:
            /* TODO The result must not be a single scalar value in general. */

        case SemaContext::S_Select: {
            /* The result of the nested query must be a single scalar value. */

            if (not pt->is_scalar()) {
                diag.e(e.tok.pos) << "Invalid expression:\n" << e
                                   << ",\nnested statement must return a scalar value.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            auto is_fn = is<FnApplicationExpr>(res);
            auto is_const = res.is_constant();
            auto &q = as<const SelectStmt>(*e.query);
            /* The result is a single value iff it is a constant and there is no from clause or
             * iff it is an aggregate and there is no group_by clause. */
            if (not(is_const and not q.from) and not(is_fn and not q.group_by)) {
                diag.e(e.tok.pos) << "Invalid expression:\n" << e
                                   << ",\nnested statement must return a single value.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            break;
        }
    }
}

/*===== Clause =======================================================================================================*/

void Sema::operator()(ErrorClause&)
{
    /* nothing to be done */
}

void Sema::operator()(SelectClause &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_Select;
    Catalog &C = Catalog::Get();

    bool has_vectorial = false;
    bool has_scalar = false;
    uint64_t const_counter = 0;
    M_insist(Ctx.results.empty());
    unsigned result_counter = 0;

    if (c.select_all) {
        /* Expand the `SELECT *` by creating dummy expressions for all accessible values of all sources. */
        auto &stmt = as<const SelectStmt>(Ctx.stmt);

        if (stmt.group_by) {
            /* If the statement contains a GROUP BY clause, we must include all grouping keys in the result. */
            auto &group_by = as<const GroupByClause>(*stmt.group_by);
            has_scalar = has_scalar or not group_by.group_by.empty();
            for (auto &[expr, alias] : group_by.group_by) {
                std::unique_ptr<Designator> d;
                if (alias) { // alias was given
                    d = create_designator(alias.text, expr->tok, *expr);
                } else if (auto D = cast<const ast::Designator>(expr.get())) { // no alias, but designator -> keep name
                    d = create_designator(D->attr_name.text, D->tok, *D);
                } else { // no designator, no alias -> derive name
                    std::ostringstream oss;
                    oss << *expr;
                    d = create_designator(C.pool(oss.str().c_str()), expr->tok, *expr);
                }
                if (auto ty = cast<const PrimitiveType>(d->type()))
                    d->type_ = ty->as_scalar();
                else
                    M_insist(d->type()->is_error(), "grouping key must be of primitive type");
                const char *attr_name = d->attr_name.text;
                auto &ref = c.expanded_select_all.emplace_back(std::move(d));
                (*this)(*ref);
                Ctx.results.emplace(attr_name, SemaContext::result_t(*ref, result_counter++, alias.text));
            }
        } else if (stmt.having) {
            /* A statement with a HAVING clause but without a GROUP BY clause may only have literals in its SELECT
             * clause.  Therefore, '*' has no meaning and we should emit a warning. */
            diag.w(c.select_all.pos) << "The '*' has no meaning in this query.  Did you forget the GROUP BY clause?.\n";
        } else {
            /* The '*' in the SELECT clause selects all attributes of all sources. */
            for (auto &[src_name, src] : Ctx.sorted_sources()) {
                if (auto ref = std::get_if<std::reference_wrapper<const Table>>(&src)) {
                    /* The source is a database table. */
                    auto &tbl = ref->get();
                    for (auto &attr : tbl) {
                        auto d = create_designator(
                            /* pos=        */ c.select_all.pos,
                            /* table_name= */ src_name,
                            /* attr_name=  */ attr.name,
                            /* target=     */ &attr,
                            /* type=       */ attr.type
                        );
                        auto &ref = c.expanded_select_all.emplace_back(std::move(d));
                        (*this)(*ref);
                        Ctx.results.emplace(attr.name, SemaContext::result_t(*ref, result_counter++));
                    }
                    has_vectorial = true;
                } else {
                    /* The source is a nested query. */
                    auto &named_exprs = std::get<SemaContext::named_expr_table>(src);
                    std::vector<std::unique_ptr<Expr>> expanded_select_all(named_exprs.size());
                    for (auto &[name, expr_w_pos] : named_exprs) {
                        auto &[expr, pos] = expr_w_pos;
                        auto d = create_designator(
                            /* pos=        */ c.select_all.pos,
                            /* table_name= */ src_name,
                            /* attr_name=  */ name,
                            /* target=     */ &expr.get(),
                            /* type=       */ expr.get().type()
                        );
                        auto &ref = (expanded_select_all[pos] = std::move(d));
                        (*this)(*ref);
                        if (auto pt = cast<const PrimitiveType>(ref->type())) {
                            has_scalar = has_scalar or pt->is_scalar();
                            has_vectorial = has_vectorial or pt->is_vectorial();
                        } else {
                            M_insist(ref->type()->is_error(), "result of nested query must be of primitive type");
                        }
                        Ctx.results.emplace(name, SemaContext::result_t(*ref, result_counter + pos));
                    }
                    result_counter += named_exprs.size();
                    for (auto &e : expanded_select_all) c.expanded_select_all.emplace_back(std::move(e));
                }
            }
        }
    }

    for (auto it = c.select.begin(), end = c.select.end(); it != end; ++it) {
        auto &select_expr = *it->first;
        auto alias = it->second;

        (*this)(select_expr); // recursively analyze select expression
        if (select_expr.contains_free_variables() and not is<QueryExpr>(select_expr))
            diag.e(select_expr.tok.pos) << select_expr << " contains free variables (not yet supported).\n";

        if (select_expr.type()->is_error()) continue;

        /* Expressions *must* be scalar when we have grouping. */
        if (auto pt = cast<const PrimitiveType>(select_expr.type()); Ctx.needs_grouping and pt and pt->is_vectorial()) {
            diag.e(select_expr.tok.pos) << select_expr << " is not scalar.\n";
            continue;
        }

        /* Constants and scalar values of nested queries can be broadcast from scalar to vectorial.  We collect the
         * scalar/vector-ness information of each expression in the SELECT clause. */
        if (not select_expr.is_constant() and not is<QueryExpr>(select_expr)) {
            auto pt = as<const PrimitiveType>(select_expr.type());
            has_vectorial = has_vectorial or pt->is_vectorial();
            has_scalar = has_scalar or pt->is_scalar();
        }

        if (alias) { // SELECT expression has alias?
            /* Expression with alias. */
            Ctx.results.emplace(alias.text, SemaContext::result_t(select_expr, result_counter++, alias.text));
            auto pred = [&](const std::pair<std::unique_ptr<Expr>, Token> &sel) {
                return sel.second.text == alias.text;
            };
            if (auto num = std::count_if(c.select.begin(), it, pred)) {
                /* Found ambiguous alias which is only allowed without accessing it. This is checked via the `Ctx`
                 * in which the ambiguous alias is contained. However, make alias unique for later accessing steps. */
                oss.str("");
                oss << alias.text << "$" << num;
                alias.text = C.pool(oss.str().c_str());
            }
        } else if (auto d = cast<Designator>(&select_expr)) {
            /* Expression is a designator.  Simply reuse the name without table prefix. */
            Ctx.results.emplace(d->attr_name.text, SemaContext::result_t(*d, result_counter++));
        } else {
            M_insist(not is<Designator>(select_expr));
            /* Expression without alias.  Print expression as string to get a name.  Use '$const' as prefix for
             * constants. */
            oss.str("");
            if (select_expr.is_constant())
                oss << "$const" << const_counter++;
            else
                oss << select_expr;
            Ctx.results.emplace(C.pool(oss.str().c_str()), SemaContext::result_t(select_expr, result_counter++));
        }
    }

    if (has_vectorial and has_scalar)
        diag.e(c.tok.pos) << "SELECT clause with mixed scalar and vectorial values is forbidden.\n";
}

void Sema::operator()(FromClause &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_From;

    Catalog &C = Catalog::Get();
    const auto &DB = C.get_database_in_use();

    M_insist(Ctx.sources.empty());
    unsigned source_counter = 0;

    /* Check whether the source tables in the FROM clause exist in the database.  Add the source tables to the current
     * context, using their alias if provided (e.g. FROM src AS alias). */
    for (auto &src: c.from) {
        if (auto name = std::get_if<Token>(&src.source)) {
            try {
                const Table &T = DB.get_table(name->text);
                Token table_name = src.alias ? src.alias : *name; // FROM name AS alias ?
                auto res = Ctx.sources.emplace(table_name.text, std::make_pair(std::ref(T), source_counter++));
                /* Check if the table name is already in use in other contexts. */
                bool unique = true;
                for (std::size_t i = 0; i < contexts_.size() - 1; ++i) {
                    if (contexts_[i]->stage == SemaContext::S_From) continue;
                    if (contexts_[i]->sources.find(table_name.text) != contexts_[i]->sources.end()) {
                        unique = false;
                        break;
                    }
                }
                if (not res.second or not unique)
                    diag.e(table_name.pos) << "Table name " << table_name.text << " already in use.\n";
                src.table_ = &T;
            } catch (std::out_of_range) {
                diag.e(name->pos) << "No table " << name->text << " in database " << DB.name << ".\n";
                return;
            }
        } else if (auto stmt = std::get_if<Stmt*>(&src.source)) {
            M_insist(is<SelectStmt>(*stmt), "nested statements are always select statements");

            /* Evaluate the nested statement in a fresh sema context. */
            push_context(**stmt, src.alias.text);
            (*this)(**stmt);
            M_insist(not contexts_.empty());
            SemaContext inner_ctx = pop_context();

            SemaContext::named_expr_table results;
            for (auto &[name, res] : inner_ctx.results)
                results.emplace(name, std::make_pair(std::ref(res.expr()), res.order));

            /* Add the results of the nested statement to the list of sources. */
            auto res = Ctx.sources.emplace(src.alias.text, std::make_pair(std::move(results), source_counter++));
            /* Convert scalar results to vectorials. */
            for (auto &[_, result] : inner_ctx.results)
                result.expr().type_ = as<const PrimitiveType>(result.expr().type())->as_vectorial();
            /* Check if the table name is already in use in other contexts. */
            bool unique = true;
            for (std::size_t i = 0; i < contexts_.size() - 1; ++i) {
                if (contexts_[i]->stage == SemaContext::S_From) continue;
                if (contexts_[i]->sources.find(src.alias.text) != contexts_[i]->sources.end()) {
                    unique = false;
                    break;
                }
            }
            if (not res.second or not unique) {
                diag.e(src.alias.pos) << "Table name " << src.alias.text << " already in use.\n";
                return;
            }
        } else {
            M_unreachable("invalid variant");
        }
    }
}

void Sema::operator()(WhereClause &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_Where;

    /* Analyze expression. */
    (*this)(*c.where);

    if (c.where->type()->is_error())
        return; /* nothing to be done */

    const Boolean *ty = cast<const Boolean>(c.where->type());

    /* WHERE condition must be of boolean type. */
    if (not ty) {
        diag.e(c.tok.pos) << "The expression in the WHERE clause must be of boolean type.\n";
        return;
    }
}

void Sema::operator()(GroupByClause &c)
{
    Catalog &C = Catalog::Get();
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_GroupBy;

    Ctx.needs_grouping = true;
    for (auto &[expr, alias] : c.group_by) {
        (*this)(*expr);

        /* Skip errors. */
        if (expr->type()->is_error())
            continue;

        if (expr->contains_free_variables())
            diag.e(expr->tok.pos) << *expr << " contains free variable(s) (not yet supported).\n";

        const PrimitiveType *pt = cast<const PrimitiveType>(expr->type());

        /* Can only group by expressions of primitive type. */
        if (not pt) {
            diag.e(c.tok.pos) << "Cannot group by " << *expr << ", has invalid type.\n";
            continue;
        }

        /* Can only group by vectorials.  The expression in the GROUP BY clause must be evaluated per tuple. */
        if (not pt->is_vectorial()) {
            diag.e(c.tok.pos) << "Cannot group by " << *expr << ".  Expressions in the GROUP BY clause must be "
                                 "vectorial, i.e. they must depend on each row separately.\n";
            continue;
        }

        /* Add expression to list of grouping keys. */
        if (alias) {
            Ctx.grouping_keys.emplace(alias.text, *expr);
        } else if (auto d = cast<Designator>(expr.get())) {
            Ctx.grouping_keys.emplace(d->attr_name.text, *expr);
        } else {
            oss.str("");
            oss << *expr;
            Ctx.grouping_keys.emplace(C.pool(oss.str().c_str()), *expr);
        }
    }
}

void Sema::operator()(HavingClause &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_Having;
    Ctx.needs_grouping = true;

    (*this)(*c.having);

    /* Skip errors. */
    if (c.having->type()->is_error())
        return;

    const Boolean *ty = cast<const Boolean>(c.having->type());

    /* HAVING condition must be of boolean type. */
    if (not ty) {
        diag.e(c.tok.pos) << "The expression in the HAVING clause must be of boolean type.\n";
        return;
    }

    if (not ty->is_scalar()) {
        diag.e(c.tok.pos) << "The expression in the HAVING clause must be scalar.\n";
        return;
    }

    /* TODO The HAVING clause must be a conjunction or disjunction of aggregates or comparisons of grouping keys. */
}

void Sema::operator()(OrderByClause &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_OrderBy;

    /* Analyze all ordering expressions. */
    for (auto &o : c.order_by) {
        auto &e = o.first;
        (*this)(*e);

        if (e->type()->is_error()) continue;
        if (e->contains_free_variables())
            diag.e(e->tok.pos) << *e << " contains free variable(s) (not yet supported).\n";

        auto pt = as<const PrimitiveType>(e->type());

        if (Ctx.needs_grouping) { // w/ grouping
            /* If we grouped, the grouping keys now have scalar type. */
            if (pt->is_vectorial())
                diag.e(c.tok.pos) << "Cannot order by " << *e << ", expression must be scalar.\n";
        } else { // w/o grouping
            /* If we did not group, the ordering expressions must be vectorial. */
            if (pt->is_scalar())
                diag.e(c.tok.pos) << "Cannot order by " << *e << ", expression must be vectorial.\n";
        }
    }
}

void Sema::operator()(LimitClause &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_Limit;

    /* TODO limit only makes sense when SELECT is vectorial and not scalar */

    errno = 0;
    strtoull(c.limit.text, nullptr, 0);
    if (errno == EINVAL)
        diag.e(c.limit.pos) << "Invalid value for LIMIT.\n";
    else if (errno == ERANGE)
        diag.e(c.limit.pos) << "Value of LIMIT out of range.\n";
    else if (errno != 0)
        diag.e(c.limit.pos) << "Invalid LIMIT.\n";

    if (c.offset) {
        errno = 0;
        strtoull(c.offset.text, nullptr, 0);
        if (errno == EINVAL)
            diag.e(c.offset.pos) << "Invalid value for OFFSET.\n";
        else if (errno == ERANGE)
            diag.e(c.offset.pos) << "Value of OFFSET out of range.\n";
        else if (errno != 0)
            diag.e(c.offset.pos) << "Invalid OFFSET.\n";
    }
}


/*===== Instruction ==================================================================================================*/

void Sema::operator()(Instruction &I) {
    Catalog &C = Catalog::Get();
    try {
        command_ = C.create_instruction(I.name, I.args);
    } catch (std::invalid_argument) {
        diag.e(I.tok.pos) << "Instruction " << I.name << " unknown\n";
    }
}


/*===== Stmt =========================================================================================================*/

void Sema::operator()(ErrorStmt&)
{
    /* nothing to be done */
}

void Sema::operator()(EmptyStmt&)
{
    /* nothing to be done */
}

void Sema::operator()(CreateDatabaseStmt &s)
{
    RequireContext RCtx(this, s);
    Catalog &C = Catalog::Get();
    const char *db_name = s.database_name.text;

    try {
        C.get_database(db_name);
        diag.e(s.database_name.pos) << "Database " << db_name << " already exists.\n";
    } catch (std::out_of_range) {
        command_ = std::make_unique<CreateDatabase>(db_name);
    }
}

void Sema::operator()(UseDatabaseStmt &s)
{
    RequireContext RCtx(this, s);
    Catalog &C = Catalog::Get();
    const char *db_name = s.database_name.text;

    try {
        C.get_database(db_name);
    } catch (std::out_of_range) {
        diag.e(s.database_name.pos) << "Database " << db_name << " does not exist.\n";
        return;
    }

    command_ = std::make_unique<UseDatabase>(db_name);
}

void Sema::operator()(CreateTableStmt &s)
{
    RequireContext RCtx(this, s);
    Catalog &C = Catalog::Get();

    if (not C.has_database_in_use()) {
        diag.err() << "No database selected.\n";
        return;
    }
    auto &DB = C.get_database_in_use();
    const char *table_name = s.table_name.text;
    std::unique_ptr<Table> T = std::make_unique<Table>(table_name);

    /* Add the newly declared table to the list of sources of the sema context.  We need to add the table to the sema
     * context so that semantic analysis of `CHECK` expressions can resolve references to attributes of the same table.
     * */
    get_context().sources.emplace(table_name, std::make_pair(SemaContext::source_type(*T), 0U));

    /* Verify table does not yet exist. */
    try {
        DB.get_table(table_name);
        diag.e(s.table_name.pos) << "Table " << table_name << " already exists in database " << DB.name << ".\n";
    } catch (std::out_of_range) {
        /* nothing to be done */
    }

    /* Analyze attributes and add them to the new table. */
    bool has_primary_key = false;
    for (auto &attr : s.attributes) {
        const PrimitiveType *ty = cast<const PrimitiveType>(attr->type);
        if (not ty) {
            diag.e(attr->name.pos) << "Attribute " << attr->name.text << " cannot be defined with type " << *attr->type
                                   << ".\n";
            return;
        }
        attr->type = ty->as_vectorial(); // convert potentially scalar type to vectorial

        /* Before we check the constraints, we must add this newly declared attribute to its table, and hence to the
         * sema context. */
        try {
            T->push_back(attr->name.text, ty->as_vectorial());
        } catch (std::invalid_argument) {
            /* attribute name is a duplicate */
            diag.e(attr->name.pos) << "Attribute " << attr->name.text << " occurs multiple times in defintion of table "
                                   << table_name << ".\n";
        }

        /* Check constraint definitions. */
        bool has_reference = false; ///< at most one reference allowed per attribute
        bool is_unique = false, is_not_null = false;
        get_context().stage = SemaContext::S_Where;
        for (auto &c : attr->constraints) {
            if (is<PrimaryKeyConstraint>(c)) {
                if (has_primary_key)
                    diag.e(attr->name.pos) << "Duplicate definition of primary key as attribute " << attr->name.text
                                          << ".\n";
                has_primary_key = true;
                T->add_primary_key(attr->name.text);
            }

            if (is<UniqueConstraint>(c)) {
                if (is_unique)
                    diag.w(c->tok.pos) << "Duplicate definition of attribute " << attr->name.text << " as UNIQUE.\n";
                is_unique = true;
                T->at(attr->name.text).unique = true;
            }

            if (is<NotNullConstraint>(c)) {
                if (is_not_null)
                    diag.w(c->tok.pos) << "Duplicate definition of attribute " << attr->name.text << " as NOT NULL.\n";
                is_not_null = true;
                T->at(attr->name.text).not_nullable = true;
            }

            if (auto check = cast<CheckConditionConstraint>(c)) {
                /* Verify that the type of the condition is boolean. */
                /* TODO if the condition uses already mentioned attributes, we must add them to the sema context before
                 * invoking semantic analysis of the condition! */
                (*this)(*check->cond);
                auto ty = check->cond->type();
                if (not ty->is_boolean())
                    diag.e(check->tok.pos) << "Condition " << *check->cond << " is an invalid CHECK constraint.\n";
            }

            if (auto ref = cast<ReferenceConstraint>(c)) {
                if (has_reference)
                    diag.e(ref->tok.pos) << "Attribute " << attr->name.text << " must not have multiple references.\n";
                has_reference = true;

                /* Check that the referenced attribute exists. */
                try {
                    auto &ref_table = DB.get_table(ref->table_name.text);
                    try {
                        auto &ref_attr = ref_table.at(ref->attr_name.text);
                        if (attr->type != ref_attr.type)
                            diag.e(ref->attr_name.pos) << "Referenced attribute has different type.\n";
                        T->at(attr->name.text).reference = &ref_attr;
                    } catch (std::out_of_range) {
                        diag.e(ref->attr_name.pos) << "Invalid reference, attribute " << ref->attr_name.text
                                                  << " not found in table " << ref->table_name.text << ".\n";
                    }
                } catch (std::out_of_range) {
                    diag.e(ref->table_name.pos) << "Invalid reference, table " << ref->table_name.text
                                                << " not found.\n";
                }
            }
        }
    }

    if (not is_nested() and not diag.num_errors())
        command_ = std::make_unique<CreateTable>(std::move(T));
}

void Sema::operator()(SelectStmt &s)
{
    RequireContext RCtx(this, s);
    Catalog &C = Catalog::Get();

    if (s.from) {
        if (not C.has_database_in_use()) {
            diag.err() << "No database selected.\n";
            return;
        }
        (*this)(*s.from);
    }
    if (s.where) (*this)(*s.where);
    if (s.group_by) (*this)(*s.group_by);
    if (s.having) (*this)(*s.having);
    (*this)(*s.select);
    if (s.order_by) (*this)(*s.order_by);
    if (s.limit) (*this)(*s.limit);


    if (not is_nested() and not diag.num_errors())
        command_ = std::make_unique<QueryDatabase>();
}

void Sema::operator()(InsertStmt &s)
{
    RequireContext RCtx(this, s);
    Catalog &C = Catalog::Get();

    if (not C.has_database_in_use()) {
        diag.e(s.table_name.pos) << "No database in use.\n";
        return;
    }
    auto &DB = C.get_database_in_use();

    const Table *tbl;
    try {
        tbl = &DB.get_table(s.table_name.text);
    } catch (std::out_of_range) {
        diag.e(s.table_name.pos) << "Table " << s.table_name.text << " does not exist in database " << DB.name << ".\n";
        return;
    }

    /* Analyze values. */
    for (std::size_t i = 0; i != s.tuples.size(); ++i) {
        auto &t = s.tuples[i];
        if (t.empty())
            continue; // syntax error, already reported
        if (t.size() != tbl->num_attrs()) {
            diag.e(s.table_name.pos) << "Tuple " << (i + 1) << " has not enough values.\n";
            continue;
        }
        for (std::size_t j = 0; j != t.size(); ++j) {
            auto &v = t[j];
            auto &attr = tbl->at(j);
            switch (v.first) {
                case InsertStmt::I_Expr: {
                    (*this)(*v.second);
                    if (v.second->type()->is_error()) continue;
                    auto ty = as<const PrimitiveType>(v.second->type());
                    if (ty->is_boolean() and attr.type->is_boolean())
                        break;
                    if (ty->is_character_sequence() and attr.type->is_character_sequence())
                        break;
                    if (ty->is_date() and attr.type->is_date())
                        break;
                    if (ty->is_date_time() and attr.type->is_date_time())
                        break;
                    if (ty->is_numeric() and attr.type->is_numeric())
                        break;
                    diag.e(s.table_name.pos) << "Value " << *v.second << " is not valid for attribute "
                                             << attr.name << ".\n";
                    break;
                }

                case InsertStmt::I_Null: {
                    if (attr.not_nullable)
                        diag.e(s.table_name.pos) << "Value NULL is not valid for attribute " << attr.name
                                                 << " declared as NOT NULL.\n";
                    break;
                }

                case InsertStmt::I_Default:
                    /* TODO has default? */
                    break;
            }
        }
    }

    if (not is_nested() and not diag.num_errors())
        command_ = std::make_unique<InsertRecords>();
}

void Sema::operator()(UpdateStmt &s)
{
    RequireContext RCtx(this, s);
    /* TODO */
    (void) s;
    M_unreachable("Not implemented.");
}

void Sema::operator()(DeleteStmt &s)
{
    RequireContext RCtx(this, s);
    /* TODO */
    (void) s;
    M_unreachable("Not implemented.");
}

void Sema::operator()(DSVImportStmt &s)
{
    RequireContext RCtx(this, s);
    auto &C = Catalog::Get();

    if (not C.has_database_in_use()) {
        diag.e(s.table_name.pos) << "No database selected\n";
        return;
    }
    auto &DB = C.get_database_in_use();

    const Table *table = nullptr;
    try {
        table = &DB.get_table(s.table_name.text);
    } catch (std::out_of_range) {
        diag.e(s.table_name.pos) << "Table " << s.table_name.text << " does not exist in database " << DB.name << ".\n";
    }

    DSVReader::Config cfg;
    cfg.has_header = s.has_header;
    cfg.skip_header = s.skip_header;
    if (s.rows)
        cfg.num_rows = atoi(s.rows.text);

    /* If character was provided by user, check that length is equal to 1. */
#define SET_CHAR(NAME) \
    if (s.NAME) { \
        std::string NAME = interpret(s.NAME.text); \
        if (NAME.length() == 1) \
            cfg.NAME = NAME[0]; \
        else \
            diag.e(s.NAME.pos) << "Invalid " #NAME " character " << s.NAME.text << ". Must have length 1.\n"; \
    }
    SET_CHAR(delimiter);
    SET_CHAR(quote);
    SET_CHAR(escape);
#undef SET_CHAR

    /* Delimiter and quote character must be distinct. */
    if (cfg.delimiter == cfg.quote) {
        auto pos = s.delimiter ? s.delimiter.pos : s.quote.pos;
        diag.e(pos) << "The delimiter (" << cfg.delimiter << ") must differ from the quote character (" << cfg.quote
                    << ").\n";
    }

    /* Sanity check for skip header. */
    if (cfg.skip_header and not cfg.has_header) {
        if (not Options::Get().quiet)
            diag.n(s.path.pos) << "I will assume the existence of a header so I can skip it.\n";
    }

    /* Get filesystem path from path token by removing surrounding quotation marks. */
    std::filesystem::path path(std::string(s.path.text, 1, strlen(s.path.text) - 2));

    if (not diag.num_errors())
        command_ = std::make_unique<ImportDSV>(*table, path, std::move(cfg));
}
