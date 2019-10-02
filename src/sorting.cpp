#include "IR/CNF.hpp"
#include "IR/Interpreter.hpp"
#include "IR/Operator.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "storage/RowStore.hpp"
#include "storage/Store.hpp"
#include "util/ArgParser.hpp"
#include "util/Diagnostic.hpp"
#include "util/fn.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>

#include <variant>


using namespace db;


void usage(std::ostream &out, const char *name)
{
    out << "Creates or loads a table.\n"
        << "USAGE:\n\t" << name << " [<FILE>]"
        << "\n\t" << name << " -"
        << std::endl;
}

int main(int argc, const char **argv)
{
    Catalog &C = Catalog::Get();
    auto &db = C.add_database("mydb");
    C.set_database_in_use(db);

    /* Create pooled strings. */
    const char *a = C.pool("a");
    const char *b = C.pool("b");
    const char *c = C.pool("c");
    const char *d = C.pool("d");
    const char *e = C.pool("e");
    const char *f = C.pool("f");
    const char *g = C.pool("g");
    const char *h = C.pool("h");

    const char *mytable = C.pool("mytable");
    const char *T = C.pool("T");

    /* Create a table. */
    Table &tbl = db.add_table(mytable);
    tbl.push_back(Type::Get_Integer(Type::TY_Vector, 4), a);
    tbl.push_back(Type::Get_Boolean(Type::TY_Vector), b);
    tbl.push_back(Type::Get_Integer(Type::TY_Vector, 8), c);
    tbl.push_back(Type::Get_Decimal(Type::TY_Vector, 8, 2), d);
    tbl.push_back(Type::Get_Integer(Type::TY_Vector, 1), e);
    tbl.push_back(Type::Get_Boolean(Type::TY_Vector), f);
    tbl.push_back(Type::Get_Char(Type::TY_Vector, 3), g);
    tbl.push_back(Type::Get_Double(Type::TY_Vector), h);

    Store *store = new RowStore(tbl);

    store->dump();

    for (std::size_t i = 0; i != 9; ++i) {
        auto r = store->append();
        const auto v = int32_t(i);
        r->set(tbl[a], v);
        r->set(tbl[b], v % 2 == 1);
        r->set(tbl[c], v - (v % 2 == 0 ? 0 : 1));
        r->set(tbl[d], 1337 + v);
        r->set(tbl[e], 127);
        r->set(tbl[f], v % 3 == 0);
        r->set(tbl[g], std::string(v % 2 ? "YES" : "NO"));
        r->set(tbl[h], 1337. / (i+1));
    }

    store->for_each([](auto &row) { std::cout << row << '\n'; });
    std::cerr << '\n';

    const char *sql = "\
SELECT c, d, h, a \n\
FROM mytable AS T \n\
ORDER BY c ASC, d + h DESC \n\
;";
    Diagnostic diag(false, std::cout, std::cerr);
    std::istringstream in(sql);
    Lexer lexer(diag, C.get_pool(), "-", in);
    Parser parser(lexer);
    Sema sema(diag);

    auto stmt = as<SelectStmt>(parser.parse());
    sema(*stmt);
    std::cout << *stmt << "\n\n";
    if (diag.num_errors()) {
        std::cerr << "Error occured.\n";
        std::cerr.flush();
        std::exit(EXIT_FAILURE);
    }

    auto &select = as<const SelectClause>(stmt->select)->select;
    auto &order_by = as<const OrderByClause>(stmt->order_by)->order_by;

    /* Scan table "mytable" as "T". */
    auto scan = new ScanOperator(*store, T);

    /* Construct a projection operator. */
    std::vector<ProjectionOperator::projection_type> projections;
    for (auto &S : select) {
        projections.emplace_back(S.first, S.second.text);
    }
    auto proj = new ProjectionOperator(projections);
    proj->add_child(scan);

    /* Order by */
    std::vector<std::pair<const Expr*, bool>> order;
    for (auto &o : order_by)
        order.emplace_back(o.first, o.second);
    auto sort = new SortingOperator(order);
    sort->add_child(proj);

    /* Print tuples. */
    auto print = [](const OperatorSchema &schema, const tuple_type &t) {
        db::print(std::cout, schema, t);
        std::cout << '\n';
    };
    auto callback = new CallbackOperator(print);
    callback->add_child(sort);

    callback->dump();
    std::cout << '\n';

    Interpreter I;
    I(*callback);

    delete callback;
    delete stmt;
    delete store;
}
