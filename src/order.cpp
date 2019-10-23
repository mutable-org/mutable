#include "IR/JoinGraph.hpp"
#include "IR/JoinOrderer.hpp"
#include "IR/Operator.hpp"
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

    /* Create a table. */
    Table &tbl = db.add_table(C.pool("tbl"));
    tbl.push_back(Type::Get_Integer(Type::TY_Vector, 4), a);

    const char *sql = "\
SELECT * \n\
FROM tbl AS R, tbl AS S, tbl AS T, tbl AS U, tbl AS V \n\
WHERE R.a = V.a AND \n\
      R.a = S.a AND \n\
      S.a = V.a AND \n\
      V.a = T.a AND \n\
      V.a = U.a AND \n\
      T.a = U.a AND \n\
      S.a + T.a >= 2 * V.a \n\
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

    auto G = JoinGraph::Build(*stmt);
    DummyJoinOrderer orderer;
    DummyCostModel cm;
    Optimizer O(orderer, cm);
    auto optree = O(*G.get());
    optree->dump(std::cout);

    delete stmt;
}
