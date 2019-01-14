#include "parse/Sema.hpp"

#include "catalog/Schema.hpp"
#include <cstdint>


using namespace db;


namespace {

/* Given two numeric types, compute the numeric type that is as least as precise as either of them. */
const Type * arithmetic_join(const Numeric *lhs, const Numeric *rhs)
{
    static constexpr double LOG_2_OF_10 = 3.321928094887362; ///> factor to convert count of decimal digits to binary digits

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
        case Numeric::N_Int: return Type::Get_Integer(precision / 8);
        case Numeric::N_Float: {
            if (precision == 32) return Type::Get_Float();
            insist(precision == 64, "Illegal floating-point precision");
            return Type::Get_Double();
        }

        case Numeric::N_Decimal: return Type::Get_Decimal(precision / LOG_2_OF_10, scale);
    }
}

}

/*===== Expressions ==================================================================================================*/
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
            e.type_ = Type::Get_Char(strlen(e.tok.text) - 2); // without quotes
            break;

        case TK_True:
        case TK_False:
            e.type_ = Type::Get_Boolean();
            break;

        case TK_HEX_INT:
            base += 6;
        case TK_DEC_INT:
            base += 2;
        case TK_OCT_INT: {
            int64_t value = strtol(e.tok.text, nullptr, base);
            if (value == int32_t(value))
                e.type_ = Type::Get_Integer(4);
            else
                e.type_ = Type::Get_Integer(8);
            break;
        }

        case TK_DEC_FLOAT:
        case TK_HEX_FLOAT:
            e.type_ = Type::Get_Float(); // XXX: Is it safe to always assume 32-bit floats?
            break;
    }
}

void Sema::operator()(Const<FnApplicationExpr> &e)
{
    /* TODO */
    unreachable("Not implemented.");
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
            if (not e.lhs->type()->is_numeric() or not e.rhs->type()->is_numeric()) {
                diag.e(e.op.pos) << "Invalid expression " << e << ", operands must be of numeric type.\n";
                e.type_ = Type::Get_Error();
                return;
            }

            /* Comparisons always have boolean type. */
            e.type_ = Type::Get_Boolean();
            break;
        }

        case TK_EQUAL:
        case TK_BANG_EQUAL: {
            if (e.lhs->type()->is_boolean() and e.rhs->type()->is_boolean()) goto ok;
            if (e.lhs->type()->is_character_sequence() and e.rhs->type()->is_character_sequence()) goto ok;
            if (e.lhs->type()->is_numeric() and e.rhs->type()->is_numeric()) goto ok;

            /* All other operand types are incomparable. */
            diag.e(e.op.pos) << "Invalid expression " << e << ", operands are incomparable.\n";
            return;

ok:
            /* Comparisons always have boolean type. */
            e.type_ = Type::Get_Boolean();
            break;
        }
    }
}

/*===== Statements ===================================================================================================*/
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
    Relation *R;
    try {
        R = &DB.add_relation(table_name);
    } catch (std::invalid_argument) {
        diag.e(s.table_name.pos) << "Table " << table_name << " already exists in database " << DB.name << ".\n";
        return;
    }

    /* At this point we know that the create table statement is syntactically correct.  Hence, we can expect valid
     * attribute names and types. */
    for (auto &A : s.attributes)
        R->push_back(A.second, A.first.text);

    diag.out() << "Created table " << table_name << " in database " << DB.name << ".\n";
}

void Sema::operator()(Const<SelectStmt> &s)
{
    Catalog &C = Catalog::Get();
    SemaContext &Ctx = push_context();

    if (not C.has_database_in_use()) {
        diag.err() << "No database selected.\n";
        return;
    }
    const auto &DB = C.get_database_in_use();

    /* Check whether the source tables in the FROM clause exist in the database.  Add the source tables to the current
     * context, using their alias if provided (e.g. FROM src AS alias). */
    for (auto &table: s.from) {
        try {
            const Relation &R = DB[table.first.text];
            const char *table_name = table.second ? table.second.text : R.name;
            Ctx.sources.emplace(table_name, &R);
        } catch (std::out_of_range) {
            diag.e(table.first.pos) << "No table " << table.first.text << " in database " << DB.name << ".\n";
            return;
        }
    }

    /* Analyze WHERE clause. */
    if (s.where) {
        (*this)(*s.where);
        if (not s.where->type()->is_error() and not s.where->type()->is_boolean())
            diag.err() << "The expression in the WHERE clause must be of boolean type.\n"; // TODO position
    }

    /* Analyze GROUP BY clause. */
    if (not s.group_by.empty()) {
        /* Analyze all grouping expressions. */
        for (auto expr : s.group_by)
            (*this)(*expr);
    }

    /* Analyze HAVING clause. */
    if (s.having) {
        (*this)(*s.having);
        /* TODO The HAVING clause must be a conjunction or disjunction of aggregates or comparisons of grouping keys. */
    }

    /* Analyze ORDER BY clause. */
    if (not s.order_by.empty()) {
        /* Analyze all ordering expressions. */
        /* TODO If we grouped before, the ordering expressions must depend on a group key or an aggregate. */
        for (auto o : s.order_by)
            (*this)(*o.first);
    }

    if (s.limit.first) {
        (*this)(*s.limit.first);
        if (s.limit.second)
            (*this)(*s.limit.second);
    }

    /* Pop context from stack. */
    pop_context();
}

void Sema::operator()(Const<InsertStmt> &s)
{
    /* TODO */
    unreachable("Not implemented.");
}

void Sema::operator()(Const<UpdateStmt> &s)
{
    /* TODO */
    unreachable("Not implemented.");
}

void Sema::operator()(Const<DeleteStmt> &s)
{
    /* TODO */
    unreachable("Not implemented.");
}
