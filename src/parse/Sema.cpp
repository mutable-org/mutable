#include "parse/Sema.hpp"

#include "catalog/Schema.hpp"
#include <cstdint>


using namespace db;


/*===== Expressions ==================================================================================================*/
void Sema::operator()(Const<ErrorExpr> &e)
{
    e.type_ = Type::Get_Error();
}

void Sema::operator()(Const<Designator> &e)
{
    SemaContext &Ctx = contexts_.back();

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
    /* TODO Check if unary expression is compatible with sub-expression type. */
    unreachable("Not implemented.");
}

void Sema::operator()(Const<BinaryExpr> &e)
{
    /* Analyze sub-expressions. */
    (*this)(*e.lhs);
    (*this)(*e.rhs);
    /* TODO Check if both binary operator is compatible with sub-expression types. */
    unreachable("Not implemented.");
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
    SemaContext &Ctx = (contexts_.emplace_back(), contexts_.back());

    if (not C.has_database_in_use()) {
        diag.err() << "No database selected.\n";
        return;
    }
    const auto &DB = C.get_database_in_use();

    /* Check whether the source tables in the FROM clause exist in the database. */
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

    /* Analyze WHERE predicate. */
    if (s.where) (*this)(*s.where);

    /* Pop context from stack. */
    contexts_.pop_back();
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
