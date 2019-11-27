#include "backend/Backend.hpp"
#include "IR/CNF.hpp"
#include "IR/JoinGraph.hpp"
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
    ArgParser AP;
#define ADD(TYPE, VAR, INIT, SHORT, LONG, DESCR, CALLBACK)\
    TYPE VAR = INIT;\
    {\
        std::function<void(TYPE)> callback = CALLBACK;\
        AP.add(SHORT, LONG, VAR, DESCR, callback);\
    }
    ADD(bool, show_help, false,             /* Type, Var, Init  */
        "-h", "--help",                     /* Short, Long      */
        "prints this help message",         /* Description      */
        [&](bool) { show_help = true; });   /* Callback         */
    ADD(bool, color, false,                 /* Type, Var, Init  */
        nullptr, "--color",                 /* Short, Long      */
        "use colors",                       /* Description      */
        [&](bool) { color = true; });       /* Callback         */
#undef ADD
    AP.parse_args(argc, argv);

    if (show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    if (AP.args().size() > 1) {
        usage(std::cerr, argv[0]);
        std::cerr << "WHERE\n";
        AP.print_args(stderr);
        std::exit(EXIT_FAILURE);
    }

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
    const char *S = C.pool("S");

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
        r->set(tbl[b], v % 3 == 1);
        r->set(tbl[c], 42 + v);
        r->set(tbl[d], 1337 + v);
        r->set(tbl[e], 127);
        r->set(tbl[f], v % 3 == 0);
        r->set(tbl[g], std::string(v % 2 ? "YES" : "NO"));
        r->set(tbl[h], 1337. / (i+1));
    }

    {
        auto row = store->append();
        row->set(tbl[a], 99);
        row->set(tbl[c], 50);
        row->set(tbl[g], std::string("Bot"));
    }

    store->for_each([](auto &row) { std::cout << row << '\n'; });
    std::cerr << '\n';

    const char *sql = "\
SELECT T.f AS new_f, T.g, COUNT() + 42 AS num \n\
FROM mytable AS T, mytable AS S \n\
WHERE T.c >= 44 AND T.a = S.a \n\
GROUP BY T.f, T.g \n\
HAVING COUNT() > 1 \n\
ORDER BY new_f ASC, 2 * num DESC;\n\"";
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

    auto JG = JoinGraph::Build(*stmt);

    auto where = as<WhereClause>(stmt->where)->where;
    auto &select = as<const SelectClause>(stmt->select)->select;
    auto having = as<const HavingClause>(stmt->having)->having;
    auto &order_by = as<const OrderByClause>(stmt->order_by)->order_by;

    cnf::CNFGenerator cnfGen;
    cnfGen(*where);
    auto where_cnf = cnfGen.get();
    cnfGen(*having);
    auto having_cnf = cnfGen.get();

    /* Scan table "mytable" as "T". */
    auto scan = new ScanOperator(*store, T);

    /* Filter tuples. */
    auto filter = new FilterOperator(cnf::CNF{where_cnf[0]});
    filter->add_child(scan);

    /* Scan table "mytable" as "S". */
    auto scan2 = new ScanOperator(*store, S);

    /* Join the filtered table "S" with "T". */
    auto join = new JoinOperator(cnf::CNF{where_cnf[1]}, JoinOperator::J_Undefined);
    join->add_child(filter);
    join->add_child(scan2);

    /* Group by */
    auto grouping = new GroupingOperator(JG->group_by(), JG->aggregates(), GroupingOperator::G_Hashing);
    grouping->add_child(join);

    /* Having */
    auto having_filter = new FilterOperator(having_cnf);
    having_filter->add_child(grouping);

    /* Order by */
    std::vector<std::pair<const Expr*, bool>> order;
    for (auto &o : order_by)
        order.emplace_back(o.first, o.second);
    auto sorting = new SortingOperator(order);
    sorting->add_child(having_filter);

    /* Construct a projection operator. */
    std::vector<ProjectionOperator::projection_type> projections;
    for (auto &S : select) {
        projections.emplace_back(S.first, S.second.text);
    }
    auto proj = new ProjectionOperator(projections);
    proj->add_child(sorting);

    /* Print tuples. */
    auto print = [](const OperatorSchema &schema, const tuple_type &t) {
        db::print(std::cout, schema, t);
        std::cout << '\n';
    };
    auto callback = new CallbackOperator(print);
    callback->add_child(proj);

    callback->dump();
    std::cout << '\n';

    auto I = Backend::CreateInterpreter();
    I->execute(*callback);

    delete callback;
    delete stmt;
    delete store;
}
