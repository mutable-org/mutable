#include "IR/JoinGraph.hpp"
#include "IR/JoinOrderer.hpp"
#include "IR/Optimizer.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
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
    out << "Computes a join order.\n"
        << "USAGE:\n\t" << name
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

    const char *sql = "\
SELECT * \n\
FROM mytable AS R, mytable AS S, mytable AS T \n\
WHERE R.a = S.a AND R.c = T.c AND S.g = T.g \n\
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

    auto G = JoinGraph::Build(stmt);
    DummyJoinOrderer orderer;
    DummyCostModel cm;
    Optimizer O(orderer, cm);
    auto order = O(*G.get());

    for (auto g : order)
        std::cout << g.second << '\n';

    delete stmt;
}
