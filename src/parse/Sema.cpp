#include "parse/Sema.hpp"

#include "catalog/Schema.hpp"
#include <cstdint>
#include <sstream>
#include <unordered_map>


using namespace db;


namespace {

/* Given two numeric types, compute the numeric type that is as least as precise as either of them. */
const Type * arithmetic_join(const Numeric *lhs, const Numeric *rhs)
{
    static constexpr double LOG_2_OF_10 = 3.321928094887362; ///> factor to convert count of decimal digits to binary digits

    /* Combining a vector with a scalar yields a vector. */
    Type::category_t category = std::max(lhs->category, rhs->category);

    /* N_Decimal is always "more precise" than N_Float.  N_Float is always more precise than N_Int.  */
    Numeric::kind_t kind = std::max(lhs->kind, rhs->kind);

    /* Compute the precision in bits. */
    unsigned precision_lhs, precision_rhs;
    switch (lhs->kind) {
        case Numeric::N_Int:     precision_lhs = 8 * lhs->precision; break;
        case Numeric::N_Float:   precision_lhs = lhs->precision; break;
        case Numeric::N_Decimal: precision_lhs = std::ceil(LOG_2_OF_10 * lhs->precision); break;
    }
    switch (rhs->kind) {
        case Numeric::N_Int:     precision_rhs = 8 * rhs->precision; break;
        case Numeric::N_Float:   precision_rhs = rhs->precision; break;
        case Numeric::N_Decimal: precision_rhs = std::ceil(LOG_2_OF_10 * rhs->precision); break;
    }
    int precision = std::max(precision_lhs, precision_rhs);
    int scale = std::max(lhs->scale, rhs->scale);

    switch (kind) {
        case Numeric::N_Int: return Type::Get_Integer(category, precision / 8);
        case Numeric::N_Float: {
            if (precision == 32) return Type::Get_Float(category);
            insist(precision == 64, "Illegal floating-point precision");
            return Type::Get_Double(category);
        }

        case Numeric::N_Decimal: return Type::Get_Decimal(category, precision / LOG_2_OF_10, scale);
    }
}

}

/*===== Expr =========================================================================================================*/

void Sema::operator()(Const<ErrorExpr> &e)
{
    e.type_ = Type::Get_Error();
}

void Sema::operator()(Const<Designator> &e)
{
    SemaContext &Ctx = get_context();
    bool is_result = false;

    /* If the designator references an attribute of a table, search for it. */
    if (e.table_name) {
        /* Find the source table first and then locate the target inside this table. */
        SemaContext::source_type src;
        try {
            src = Ctx.sources.at(e.table_name.text);
        } catch (std::out_of_range) {
            diag.e(e.table_name.pos) << "Source table " << e.table_name.text << " not found. "
                                        "Maybe you forgot to specify it in the FROM clause?\n";
            e.type_ = Type::Get_Error();
            return;
        }

        /* Find the target inside the source table. */
        Designator::target_type target;
        if (auto T = std::get_if<const Table*>(&src)) {
            const Table &tbl = **T;
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
            try {
                target = tbl.at(e.attr_name.text);
            } catch (std::out_of_range) {
                diag.e(e.attr_name.pos) << "Source " << e.table_name.text << " has no attribute " << e.attr_name.text
                                        << ".\n";
                e.type_ = Type::Get_Error();
                return;
            }
        } else {
            unreachable("invalid variant");
        }
        e.target_ = target;
    } else {
        /* No table name was specified.  The designator references either a result or a named expression.  Search the
         * named expressions first, because they overrule attribute names. */
        if (auto it = Ctx.results.find(e.attr_name.text); it != Ctx.results.end()) {
            /* Found a named expression. */
            e.target_ = it->second;
            is_result = true;
        } else {
            /* Since no table was explicitly specified, we must search *all* source tables for the attribute. */
            Designator::target_type target;
            const char *alias = nullptr;
            for (auto &src : Ctx.sources) {
                if (auto T = std::get_if<const Table*>(&src.second)) {
                    const Table &tbl = **T;
                    try {
                        const Attribute &A = tbl[e.attr_name.text];
                        if (not std::holds_alternative<std::monostate>(target)) {
                            /* ambiguous attribute name */
                            diag.e(e.attr_name.pos) << "Attribute specifier " << e.attr_name.text << " is ambiguous.\n";
                            // TODO print names of conflicting tables
                            e.type_ = Type::Get_Error();
                            return;
                        } else {
                            target = &A; // we found an attribute of that name in the source tables
                            alias = src.first;
                        }
                    } catch (std::out_of_range) {
                        /* This source table has no attribute of that name.  OK, continue. */
                    }
                } else if (auto T = std::get_if<SemaContext::named_expr_table>(&src.second)) {
                    const SemaContext::named_expr_table &tbl = *T;
                    try {
                        Expr *E = tbl.at(e.attr_name.text);
                        if (not std::holds_alternative<std::monostate>(target)) {
                            /* ambiguous attribute name */
                            diag.e(e.attr_name.pos) << "Attribute specifier " << e.attr_name.text << " is ambiguous.\n";
                            // TODO print names of conflicting tables
                            e.type_ = Type::Get_Error();
                            return;
                        } else {
                            target = E; // we found an attribute of that name in the source tables
                            alias = src.first;
                        }
                    } catch (std::out_of_range) {
                        /* This source table has no attribute of that name.  OK, continue. */
                    }
                } else {
                    unreachable("invalid variant");
                }
            }

            /* If this designator could not be resolved, emit an error and abort further semantic analysis. */
            if (std::holds_alternative<std::monostate>(target)) {
                diag.e(e.attr_name.pos) << "Attribute " << e.attr_name.text << " not found.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            e.target_ = target;
            e.table_name.text = alias; // set the deduced table name of this designator
        }
    }

    /* Compute the type of this designator based on the referenced source. */
    insist(e.target_.index() != 0);
    struct get_type {
        const Type * operator()(std::monostate&) const { unreachable("target not set"); }
        const Type * operator()(const Attribute *attr) const { return attr->type; }
        const Type * operator()(const Expr *expr) const { return expr->type_; }
    };
    const PrimitiveType *pt = cast<const PrimitiveType>(std::visit(get_type(), e.target_));
    e.type_ = pt;

    if (not is_result)
        e.type_ = pt->as_vectorial();

    switch (Ctx.stage) {
        default:
            unreachable("designator not allowed in this stage");

        case SemaContext::S_Where:
        case SemaContext::S_GroupBy:
            /* The type of the attribute remains unchanged.  Nothing to be done. */
            break;

        case SemaContext::S_Having:
        case SemaContext::S_OrderBy:
        case SemaContext::S_Select:
            /* Detect whether we grouped by this designator.  In that case, convert the type to scalar. */
            for (auto grp : Ctx.group_keys) {
                Designator *d = cast<Designator>(grp);
                if (d and d->target() == e.target()) {
                    /* The grouping key and this designator reference the same attribute. */
                    e.type_ = pt->as_scalar();
                    break;
                }
            }
            break;
    }
}

void Sema::operator()(Const<Constant> &e)
{
    int base = 8; // for integers
    switch (e.tok.type) {
        default:
            unreachable("a constant must be one of the types below");

        case TK_STRING_LITERAL:
            e.type_ = Type::Get_Char(Type::TY_Scalar, strlen(e.tok.text) - 2); // without quotes
            break;

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
            e.type_ = Type::Get_Float(Type::TY_Scalar); // XXX: Is it safe to always assume 32-bit floats?
            break;
    }
}

void Sema::operator()(Const<FnApplicationExpr> &e)
{
    SemaContext &Ctx = get_context();
    Catalog &C = Catalog::Get();
    const auto &DB = C.get_database_in_use(); // XXX can we assume a DB is selected?

    /* Analyze function name. */
    Designator *d = cast<Designator>(e.fn);
    if (not d or not d->is_identifier()) {
        diag.e(d->attr_name.pos) << *d << " is not a valid function.\n";
        d->type_ = e.type_ = Type::Get_Error();
        return;
    }
    insist(d);
    insist(not d->type_, "This identifier has already been analyzed.");

    /* Analyze arguments. */
    for (auto arg : e.args)
        (*this)(*arg);

    const Function *fn = nullptr;
    /* Test whether function is a standard function. */
    try {
        fn = C.get_function(d->attr_name.text);
    } catch (std::out_of_range) {
        /* If function is not a standard function, test whether it is a user-defined function. */
        try {
            fn = DB.get_function(d->attr_name.text);
        } catch (std::out_of_range) {
            diag.e(d->attr_name.pos) << "Function " << d->attr_name.text << " is not defined in database " << DB.name
                << ".\n";
            e.type_ = Type::Get_Error();
            return;
        }
    }
    insist(fn);

    /* Infer the type of the function.  Functions are defined in an abstract way, where the type of the parameters is
     * not specified.  We must infer the parameter types and the return type of the function. */
    switch (fn->fnid) {
        default:
            unreachable("Function not implemented");

        case Function::FN_UDF:
            diag.e(d->attr_name.pos) << "User-defined functions are not yet supported.\n";
            d->type_ = e.type_ = Type::Get_Error();
            return;

        case Function::FN_MIN:
        case Function::FN_MAX:
        case Function::FN_SUM:
        case Function::FN_AVG: {
            if (e.args.size() == 0) {
                diag.e(d->attr_name.pos) << "Missing argument for aggregate " << d << ".\n";
                d->type_ = e.type_ = Type::Get_Error();
                return;
            }
            if (e.args.size() > 1) {
                diag.e(d->attr_name.pos) << "Too many arguments for aggregate " << d << ".\n";
                d->type_ = e.type_ = Type::Get_Error();
                return;
            }
            insist(e.args.size() == 1);
            const Expr *arg = e.args[0];
            if (arg->type()->is_error()) {
                /* skip argument of error type */
                d->type_ = e.type_ = Type::Get_Error();
                return;
            }
            if (not arg->type()->is_numeric()) {
                /* invalid argument type */
                diag.e(d->attr_name.pos) << "Argument of aggregate function must be of numeric type.\n";
                d->type_ = e.type_ = Type::Get_Error();
                return;
            }
            insist(arg->type()->is_numeric());
            const Numeric *arg_type = cast<const Numeric>(arg->type());
            if (not arg_type->is_vectorial()) {
                diag.w(d->attr_name.pos) << "Argument of aggregate is not of vectorial type.  "
                                            "(Aggregates over scalars are discouraged.)\n";
            }

            switch (fn->fnid) {
                default:
                    unreachable("Invalid function");

                case Function::FN_MIN:
                case Function::FN_MAX:
                case Function::FN_AVG: {
                    /* MIN/MAX/AVG maintain type */
                    e.type_ = arg_type->as_scalar();
                    d->type_ = Type::Get_Function(e.type_, { arg->type() });
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
                diag.e(d->attr_name.pos) << "Too many arguments for aggregate " << d << ".\n";
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
                diag.e(d->attr_name.pos) << "Missing argument for aggregate " << d << ".\n";
                e.type_ = Type::Get_Error();
                return;
            }
            if (e.args.size() > 1) {
                diag.e(d->attr_name.pos) << "Too many arguments for aggregate " << d << ".\n";
                e.type_ = Type::Get_Error();
                return;
            }
            insist(e.args.size() == 1);
            const Expr *arg = e.args[0];

            if (arg->type()->is_error()) {
                e.type_ = Type::Get_Error();
                return;
            }
            const PrimitiveType *arg_type = cast<const PrimitiveType>(arg->type());
            if (not arg_type) {
                diag.e(d->attr_name.pos) << "Function ISNULL can only be applied to expressions of primitive type.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            d->type_ = Type::Get_Function(Type::Get_Boolean(arg_type->category), { arg->type() });
            e.type_= Type::Get_Boolean(arg_type->category);
            break;
        }
    }

    insist(d->type_);
    insist(d->type()->is_error() or cast<const FnType>(d->type()));
    insist(e.type_);
    insist(not e.type()->is_error());
    insist(e.type()->is_primitive());

    const PrimitiveType *ty = as<const PrimitiveType>(e.type());

    switch (Ctx.stage) {
        case SemaContext::S_From:
            unreachable("Function application in FROM clause is impossible");

        case SemaContext::S_Where:
            if (ty->is_scalar()) {
                diag.e(d->attr_name.pos) << "Aggregate functions are not allowed in WHERE clause.\n";
                return;
            }
            break;

        case SemaContext::S_GroupBy:
            if (ty->is_scalar()) {
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

void Sema::operator()(Const<UnaryExpr> &e)
{
    /* Analyze sub-expression. */
    (*this)(*e.expr);

    /* If the sub-expression is erroneous, so is this expression. */
    if (e.expr->type()->is_error()) {
        e.type_ = Type::Get_Error();
        return;
    }

    switch (e.op.type) {
        default:
            unreachable("invalid unary expression");

        case TK_Not:
            if (not e.expr->type()->is_boolean()) {
                diag.e(e.op.pos) << "Invalid expression " << e << " must be boolean.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            break;

        case TK_PLUS:
        case TK_MINUS:
        case TK_TILDE:
            if (not e.expr->type()->is_numeric()) {
                diag.e(e.op.pos) << "Invalid expression " << e << " must be numeric.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            break;
    }

    e.type_ = e.expr->type();
}

void Sema::operator()(Const<BinaryExpr> &e)
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
    switch (e.op.type) {
        default:
            unreachable("Invalid binary operator.");

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
                diag.e(e.op.pos) << "Invalid expression " << e << ", operands must be of numeric type.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            insist(ty_lhs);
            insist(ty_rhs);

            /* Compute type of the binary expression. */
            e.type_ = arithmetic_join(ty_lhs, ty_rhs);
            break;
        }

        case TK_LESS:
        case TK_LESS_EQUAL:
        case TK_GREATER:
        case TK_GREATER_EQUAL: {
            /* Verify that both operands are of numeric type. */
            const Numeric *ty_lhs = cast<const Numeric>(e.lhs->type());
            const Numeric *ty_rhs = cast<const Numeric>(e.rhs->type());
            if (not ty_lhs or not ty_rhs) {
                diag.e(e.op.pos) << "Invalid expression " << e << ", operands must be of numeric type.\n";
                e.type_ = Type::Get_Error();
                return;
            }
            insist(ty_lhs);
            insist(ty_rhs);

            /* Scalar and scalar yeild a scalar.  Otherwise, expression yields a vectorial. */
            Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

            /* Comparisons always have boolean type. */
            e.type_ = Type::Get_Boolean(c);
            break;
        }

        case TK_EQUAL:
        case TK_BANG_EQUAL: {
            if (e.lhs->type()->is_boolean() and e.rhs->type()->is_boolean()) goto ok;
            if (e.lhs->type()->is_character_sequence() and e.rhs->type()->is_character_sequence()) goto ok;
            if (e.lhs->type()->is_numeric() and e.rhs->type()->is_numeric()) goto ok;

            /* All other operand types are incomparable. */
            diag.e(e.op.pos) << "Invalid expression " << e << ", operands are incomparable.\n";
            e.type_ = Type::Get_Error();
            return;
ok:
            const PrimitiveType *ty_lhs = as<const PrimitiveType>(e.lhs->type());
            const PrimitiveType *ty_rhs = as<const PrimitiveType>(e.rhs->type());

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
                diag.e(e.op.pos) << "Invalid expression " << e << ", operands must be of boolean type.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            /* Scalar and scalar yeild a scalar.  Otherwise, expression yields a vectorial. */
            Type::category_t c = std::max(ty_lhs->category, ty_rhs->category);

            /* Logical operators always have boolean type. */
            e.type_ = Type::Get_Boolean(c);
            break;
        }
    }
}

/*===== Clause =======================================================================================================*/

void Sema::operator()(Const<ErrorClause>&)
{
    /* nothing to be done */
}

void Sema::operator()(Const<SelectClause> &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_Select;

    Catalog &C = Catalog::Get();
    const auto &DB = C.get_database_in_use();
    (void) DB;

    bool has_vector = false;
    bool has_scalar = false;

    for (auto s : c.select) {
        auto &e = *s.first;
        (*this)(e);

        if (e.type()->is_error()) continue;
        auto pt = as<const PrimitiveType>(e.type());
        has_vector = has_vector or pt->is_vectorial();
        has_scalar = has_scalar or pt->is_scalar();

        std::pair<decltype(SemaContext::results)::iterator, bool> res;
        if (s.second) {
            /* With alias. */
            res = Ctx.results.emplace(s.second.text, s.first);
        } else {
            /* Without alias.  Print expression as string to get a name. */
            std::ostringstream oss;
            oss << *s.first;
            res = Ctx.results.emplace(C.get_pool()(oss.str().c_str()), s.first);
        }
        if (not res.second)
            diag.e(s.second.pos) << "Attribute name " << s.second.text << " already used.\n";
    }

    if (has_vector and has_scalar) {
        diag.e(c.tok.pos) << "SELECT clause with mixed scalar and vectorial values is forbidden.\n";
    }
}

void Sema::operator()(Const<FromClause> &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_From;

    Catalog &C = Catalog::Get();
    const auto &DB = C.get_database_in_use();

    /* Check whether the source tables in the FROM clause exist in the database.  Add the source tables to the current
     * context, using their alias if provided (e.g. FROM src AS alias). */
    for (auto &table: c.from) {
        if (auto name = std::get_if<Token>(&table.source)) {
            try {
                const Table &T = DB.get_table(name->text);
                Token table_name = table.alias ? table.alias : *name; // FROM name AS alias ?
                auto res = Ctx.sources.emplace(table_name.text, &T);
                if (not res.second)
                    diag.e(table_name.pos) << "Table name " << table_name.text << " already in use.\n";
                table.table_ = &T;
            } catch (std::out_of_range) {
                diag.e(name->pos) << "No table " << name->text << " in database " << DB.name << ".\n";
                return;
            }
        } else if (auto stmt = std::get_if<Stmt*>(&table.source)) {
            insist(is<SelectStmt>(*stmt), "nested statements are always select statements");

            /* Evaluate the nested statement in a fresh sema context. */
            push_context();
            (*this)(**stmt);
            insist(not contexts_.empty());
            SemaContext inner_ctx = pop_context();

            /* Add the results of the nested statement to the list of sources. */
            auto res = Ctx.sources.emplace(table.alias.text, inner_ctx.results);
            if (not res.second) {
                diag.e(table.alias.pos) << "Table name " << table.alias.text << " already in use.\n";
                return;
            }
        } else {
            unreachable("invalid variant");
        }
    }
}

void Sema::operator()(Const<WhereClause> &c)
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

void Sema::operator()(Const<GroupByClause> &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_GroupBy;

    for (auto expr : c.group_by) {
        (*this)(*expr);

        /* Skip errors. */
        if (expr->type()->is_error())
            continue;

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
        Ctx.group_keys.push_back(expr);
    }
}

void Sema::operator()(Const<HavingClause> &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_Having;

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

void Sema::operator()(Const<OrderByClause> &c)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_OrderBy;

    /* Analyze all ordering expressions. */
    for (auto o : c.order_by) {
        Expr *e = o.first;
        (*this)(*e);

        if (e->type()->is_error()) continue;
        auto pt = as<const PrimitiveType>(e->type());

        if (Ctx.group_keys.empty()) {
            /* TODO Test: If we did not group, the ordering expressions must be vectorial. */
            if (not pt->is_vectorial())
                diag.e(c.tok.pos) << "Cannot order by " << *e << ", expression must be vectorial.\n";
        } else {
            /* If we grouped, the grouping keys now have scalar type.  First check that the ordering expressions is of
             * scalar type. */
            if (pt->is_scalar()) continue;

            /* If the expression is not of scalar type, check whether it is a grouping expression. */
            for (auto grp : Ctx.group_keys) {
                if (*grp == *e)
                    goto ok;
            }

            /* The expression is neither scalar nor a grouping expression. */
            diag.e(c.tok.pos) << "Cannot order by " << *e << ", expression must be scalar.\n";
ok:;
        }
    }
}

void Sema::operator()(Const<LimitClause>&)
{
    SemaContext &Ctx = get_context();
    Ctx.stage = SemaContext::S_Limit;

    /* TODO limit only makes sense when SELECT is vectorial and not scalar */
}

/*===== Stmt =========================================================================================================*/

void Sema::operator()(Const<ErrorStmt>&)
{
    /* nothing to be done */
}

void Sema::operator()(Const<EmptyStmt>&)
{
    /* nothing to be done */
}

void Sema::operator()(Const<CreateDatabaseStmt> &s)
{
    RequireContext RCtx(this);
    Catalog &C = Catalog::Get();
    const char *db_name = s.database_name.text;

    try {
        C.add_database(db_name);
        diag.out() << "Created database " << db_name << ".\n";
    } catch (std::invalid_argument) {
        diag.e(s.database_name.pos) << "Database " << db_name << " already exists.\n";
    }
}

void Sema::operator()(Const<UseDatabaseStmt> &s)
{
    RequireContext RCtx(this);
    Catalog &C = Catalog::Get();
    const char *db_name = s.database_name.text;

    try {
        auto &DB = C.get_database(db_name);
        C.set_database_in_use(DB);
        diag.out() << "Using database " << db_name << ".\n";
    } catch (std::out_of_range) {
        diag.e(s.database_name.pos) << "Database " << db_name << " not found.\n";
    }
}

void Sema::operator()(Const<CreateTableStmt> &s)
{
    RequireContext RCtx(this);
    Catalog &C = Catalog::Get();

    if (not C.has_database_in_use()) {
        diag.err() << "No database selected.\n";
        return;
    }
    auto &DB = C.get_database_in_use();
    const char *table_name = s.table_name.text;
    Table *T = new Table(table_name);

    /* Add the newly declared table to the list of sources of the sema context. */
    get_context().sources.emplace(table_name, T);

    /* Analyze attributes and add them to the new table. */
    bool error = false;
    bool has_primary_key = false;
    for (auto attr : s.attributes) {
        const PrimitiveType *ty = cast<const PrimitiveType>(attr->type);
        if (not ty) {
            diag.e(attr->name.pos) << "Attribute " << attr->name.text << " cannot be defined with type " << *attr->type
                               << ".\n";
            error = true;
        }
        attr->type = ty->as_vectorial(); // convert potentially scalar type to vectorial

        /* Before we check the constraints, we must add this newly declared attribute to its table, and hence to the
         * sema context. */
        try {
            T->push_back(ty->as_vectorial(), attr->name.text);
        } catch (std::invalid_argument) {
            diag.e(attr->name.pos) << "Attribute " << attr->name.text << " occurs multiple times in defintion of table "
                               << table_name << ".\n";
            error = true;
        }

        /* Check constraint definitions. */
        bool has_reference = false; ///< at most one reference allowed per attribute
        bool is_unique = false, is_not_null = false;
        get_context().stage = SemaContext::S_Where;
        for (auto c : attr->constraints) {
            if (is<PrimaryKeyConstraint>(c)) {
                if (has_primary_key) {
                    diag.e(attr->name.pos) << "Duplicate definition of primary key as attribute " << attr->name.text
                                          << ".\n";
                    error = true;
                }
                has_primary_key = true;
            }

            if (is<UniqueConstraint>(c)) {
                if (is_unique)
                    diag.w(c->tok.pos) << "Duplicate definition of attribute " << attr->name.text << " as UNIQUE.\n";
                is_unique = true;
            }

            if (is<NotNullConstraint>(c)) {
                if (is_not_null)
                    diag.w(c->tok.pos) << "Duplicate definition of attribute " << attr->name.text << " as UNIQUE.\n";
                is_not_null = true;
            }

            if (auto check = cast<CheckConditionConstraint>(c)) {
                /* Verify that the type of the condition is boolean. */
                /* TODO if the condition uses already mentioned attributes, we must add them to the sema context before
                 * invoking semantic analysis of the condition! */
                (*this)(*check->cond);
                auto ty = check->cond->type();
                if (not ty->is_boolean()) {
                    diag.e(c->tok.pos) << "Condition " << *check->cond << " is an invalid CHECK constraint.\n";
                    error = true;
                }
            }

            if (auto ref = cast<ReferenceConstraint>(c)) {
                if (has_reference) {
                    diag.e(c->tok.pos) << "Attribute " << attr->name.text << " must not have multiple references.\n";
                    error = true;
                }
                has_reference = true;

                /* Check that the referenced attribute exists. */
                try {
                    auto &ref_table = DB.get_table(ref->table_name.text);
                    try {
                        auto &ref_attr = ref_table.at(ref->attr_name.text);
                        if (attr->type != ref_attr.type) {
                            diag.e(ref->attr_name.pos) << "Referenced attribute has different type.\n";
                        }
                    } catch (std::out_of_range) {
                        diag.e(ref->attr_name.pos) << "Invalid reference, attribute " << ref->attr_name.text
                                                  << " not found in table " << ref->table_name.text << ".\n";
                        error = true;
                    }
                } catch (std::out_of_range) {
                    diag.e(ref->table_name.pos) << "Invalid reference, table " << ref->table_name.text
                                                << " not found.\n";
                        error = true;
                }
            }
        }
    }

    if (error) {
        delete T;
        return;
    }

    try {
        DB.add(T);
    } catch (std::invalid_argument) {
        diag.e(s.table_name.pos) << "Table " << table_name << " already exists in database " << DB.name << ".\n";
        delete T;
        return;
    }

    diag.out() << "Created table " << table_name << " in database " << DB.name << ".\n";
}

void Sema::operator()(Const<SelectStmt> &s)
{
    RequireContext RCtx(this);
    Catalog &C = Catalog::Get();

    if (not C.has_database_in_use()) {
        diag.err() << "No database selected.\n";
        return;
    }

    if (s.from) (*this)(*s.from);
    if (s.where) (*this)(*s.where);
    if (s.group_by) (*this)(*s.group_by);
    if (s.having) (*this)(*s.having);
    (*this)(*s.select);
    if (s.order_by) (*this)(*s.order_by);
    if (s.limit) (*this)(*s.limit);
}

void Sema::operator()(Const<InsertStmt> &s)
{
    RequireContext RCtx(this);
    /* TODO */
    (void) s;
    unreachable("Not implemented.");
}

void Sema::operator()(Const<UpdateStmt> &s)
{
    RequireContext RCtx(this);
    /* TODO */
    (void) s;
    unreachable("Not implemented.");
}

void Sema::operator()(Const<DeleteStmt> &s)
{
    RequireContext RCtx(this);
    /* TODO */
    (void) s;
    unreachable("Not implemented.");
}
