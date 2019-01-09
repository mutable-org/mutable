#include "parse/Sema.hpp"

#include "catalog/Schema.hpp"


using namespace db;


/*===== Expressions ==================================================================================================*/
void Sema::operator()(Const<ErrorExpr>&)
{
    /* nothing to be done */
}

void Sema::operator()(Const<Designator> &e)
{
    /* TODO A designator has the type of the referred attribute. */
}

void Sema::operator()(Const<Constant> &e)
{
    /* TODO Detect the type of the constant. */
}

void Sema::operator()(Const<FnApplicationExpr> &e)
{
}

void Sema::operator()(Const<UnaryExpr> &e)
{
    /* Analyze sub-expression. */
    (*this)(*e.expr);
    /* TODO Check if unary expression is compatible with sub-expression type. */
}

void Sema::operator()(Const<BinaryExpr> &e)
{
    /* Analyze sub-expressions. */
    (*this)(*e.lhs);
    (*this)(*e.rhs);
    /* TODO Check if both binary operator is compatible with sub-expression types. */
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

    if (not C.has_database_in_use()) {
        diag.err() << "No database selected.\n";
        return;
    }
    const auto &DB = C.get_database_in_use();

    /* Check whether the source tables in the FROM clause exist in the database. */
    for (auto &table: s.from) {
        try {
            DB[table.first.text];
        } catch (std::out_of_range) {
            diag.e(table.first.pos) << "No table " << table.first.text << " in database " << DB.name << ".\n";
            return;
        }
    }

    /* TODO Compute the set of tables that we access. */
}

void Sema::operator()(Const<InsertStmt> &s)
{
}

void Sema::operator()(Const<UpdateStmt> &s)
{
}

void Sema::operator()(Const<DeleteStmt> &s)
{
}
