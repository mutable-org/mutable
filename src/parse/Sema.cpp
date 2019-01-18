#include "parse/Sema.hpp"

#include "catalog/Schema.hpp"
#include <cstdint>


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

    if (e.table_name) {
        /* Find the relation first and then locate the attribute inside this relation. */
        const Relation *R;
        try {
            R = Ctx.sources.at(e.table_name.text);
        } catch (std::out_of_range) {
            diag.e(e.table_name.pos) << "Table " << e.table_name.text << " not found. "
                                        "Maybe you forgot to specify it in the FROM clause?\n";
            e.type_ = Type::Get_Error();
            return;
        }

        /* Find the attribute inside the relation. */
        try {
            const Attribute &A = R->at(e.attr_name.text);
            e.type_ = A.type;
        } catch (std::out_of_range) {
            diag.e(e.attr_name.pos) << "Table " << e.table_name.text << " has no attribute " << e.attr_name.text
                                    << ".\n";
            e.type_ = Type::Get_Error();
            return;
        }
    } else {
        /* Since no relation was explicitly specified, we must search *all* source relations for the attribute. */
        const Attribute *the_attribute = nullptr;
        for (auto &entry : Ctx.sources) {
            const Relation &src = *entry.second;
            try {
                const Attribute &A = src[e.attr_name.text];
                if (the_attribute != nullptr) {
                    /* ambiguous attribute name */
                    diag.e(e.attr_name.pos) << "Attribute specifier " << e.attr_name.text << " is ambiguous; "
                                               "found in tables " << src.name << " and "
                                            << the_attribute->relation.name << ".\n";
                    e.type_ = Type::Get_Error();
                    return;
                } else {
                    the_attribute = &A; // we found an attribute of that name in the source relations
                }
            } catch (std::out_of_range) {
                /* This source relation has no attribute of that name.  OK, continue. */
            }
        }

        if (not the_attribute) {
            diag.e(e.attr_name.pos) << "Attribute " << e.attr_name.text << " not found.\n";
            e.type_ = Type::Get_Error();
            return;
        }

        e.type_ = the_attribute->type;
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

        case Function::FN_COUNT:
            unreachable("Not implemented.");

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
            const PrimitiveType *arg_type = cast<const PrimitiveType>(arg_type);
            if (not arg_type) {
                diag.e(d->attr_name.pos) << "Function ISNULL can only be applied to expressions of primitive type.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            d->type_ = Type::Get_Function(Type::Get_Boolean(Type::TY_Vector), { arg->type() });
            e.type_= Type::Get_Boolean(Type::TY_Vector);
            break;
        }

    }

    insist(d->type_);
    insist(d->type()->is_error() or cast<const FnType>(d->type()));
    insist(e.type_);
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

    /* Valid unary expressions are +e, -e, and ~e, where e has numeric type. */
    if (not e.expr->type()->is_numeric()) {
        diag.e(e.op.pos) << "Invalid expression " << e << ".\n";
        e.type_ = Type::Get_Error();
        return;
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

            /* Scalar and scalar yeild a scalar.  Otherwise, expression yields a vectorial. */
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
    Catalog &C = Catalog::Get();
    const auto &DB = C.get_database_in_use();
    (void) DB;

    for (auto s : c.select)
        (*this)(*s.first);

    /* TODO check whether expressions can be computed */
    /* TODO add (renamed) expressions to result relation. */
}

void Sema::operator()(Const<FromClause> &c)
{
    Catalog &C = Catalog::Get();
    SemaContext &Ctx = get_context();
    const auto &DB = C.get_database_in_use();

    /* Check whether the source tables in the FROM clause exist in the database.  Add the source tables to the current
     * context, using their alias if provided (e.g. FROM src AS alias). */
    for (auto &table: c.from) {
        try {
            const Relation &R = DB.get_relation(table.first.text);
            const char *table_name = table.second ? table.second.text : R.name;
            auto res = Ctx.sources.emplace(table_name, &R);
            if (not res.second) {
                Token tok = table.second ? table.second : table.first;
                diag.e(tok.pos) << "Table name " << table_name << " already in use.\n";
            }
        } catch (std::out_of_range) {
            diag.e(table.first.pos) << "No table " << table.first.text << " in database " << DB.name << ".\n";
            return;
        }
    }
}

void Sema::operator()(Const<WhereClause> &c)
{
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

    /* The expression must also be of vectorial type. */
    if (not ty->is_vectorial()) {
        diag.e(c.tok.pos) << "The expression in the WHERE clause must be vectorial, "
                             "i.e. it must depend on each row separately.\n";
        return;
    }
}

void Sema::operator()(Const<GroupByClause> &c)
{
    for (auto expr : c.group_by)
        (*this)(*expr);
}

void Sema::operator()(Const<HavingClause> &c)
{
    (*this)(*c.having);

    /* HAVING condition must be of boolean type. */
    if (not c.having->type()->is_error() and not c.having->type()->is_boolean()) {
        diag.e(c.tok.pos) << "The expression in the HAVING clause must be of boolean type.\n";
        return;
    }

    /* TODO The HAVING clause must be a conjunction or disjunction of aggregates or comparisons of grouping keys. */
}

void Sema::operator()(Const<OrderByClause> &c)
{
    if (not c.order_by.empty()) {
        /* Analyze all ordering expressions. */
        /* TODO If we grouped before, the ordering expressions must depend on a group key or an aggregate. */
        for (auto o : c.order_by)
            (*this)(*o.first);
    }
}

void Sema::operator()(Const<LimitClause>&)
{
    /* nothing to be done */
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
    Catalog &C = Catalog::Get();

    if (not C.has_database_in_use()) {
        diag.err() << "No database selected.\n";
        return;
    }
    auto &DB = C.get_database_in_use();
    const char *table_name = s.table_name.text;
    Relation *R = new Relation(table_name); // ownership of this relation is transferred to the schema

    /* Analyze attributes and add them to the new relation. */
    for (auto &A : s.attributes) {
        const PrimitiveType *ty = cast<const PrimitiveType>(A.second);
        if (not ty) {
            diag.e(A.first.pos) << "Attribute " << A.first.text << " cannot be defined with type " << *A.second
                                << ".\n";
            return;
        }

        try {
            R->push_back(ty->as_vectorial(), A.first.text);
        } catch (std::invalid_argument) {
            diag.e(A.first.pos) << "Attribute " << A.first.text << " occurs multiple times in defintion of table "
                                << table_name << ".\n";
            return;
        }
    }

    try {
        DB.add(R);
    } catch (std::invalid_argument) {
        diag.e(s.table_name.pos) << "Table " << table_name << " already exists in database " << DB.name << ".\n";
        delete R; // if table name is already used and transfer of ownership failed, clean up
        return;
    }

    diag.out() << "Created table " << table_name << " in database " << DB.name << ".\n";
}

void Sema::operator()(Const<SelectStmt> &s)
{
    push_context();

    Catalog &C = Catalog::Get();

    if (not C.has_database_in_use()) {
        diag.err() << "No database selected.\n";
        return;
    }

    (*this)(*s.from);
    (*this)(*s.select);

    if (s.where) (*this)(*s.where);
    if (s.group_by) (*this)(*s.group_by);
    if (s.having) (*this)(*s.having);
    if (s.order_by) (*this)(*s.order_by);
    if (s.limit) (*this)(*s.limit);

    /* Pop context from stack. */
    pop_context();
}

void Sema::operator()(Const<InsertStmt> &s)
{
    /* TODO */
    (void) s;
    unreachable("Not implemented.");
}

void Sema::operator()(Const<UpdateStmt> &s)
{
    /* TODO */
    (void) s;
    unreachable("Not implemented.");
}

void Sema::operator()(Const<DeleteStmt> &s)
{
    /* TODO */
    (void) s;
    unreachable("Not implemented.");
}
