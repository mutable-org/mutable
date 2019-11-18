#include "backend/Interpreter.hpp"
#include "IR/CNF.hpp"
#include "IR/CostModel.hpp"
#include "IR/JoinOrderer.hpp"
#include "IR/Operator.hpp"
#include "IR/Optimizer.hpp"
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
    out << "Initializes the database using a setup SQL file and then accepts queries.\n"
        << "USAGE:\n\t" << name << " <SETUP.SQL>"
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
    ADD(bool, opdot, false,                 /* Type, Var, Init  */
        nullptr, "--opdot",                 /* Short, Long      */
        "dot the operator tree",            /* Description      */
        [&](bool) { opdot = true; });       /* Callback         */
#undef ADD
    AP.parse_args(argc, argv);

    if (show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    if (AP.args().size() != 1) {
        usage(std::cerr, argv[0]);
        std::cerr << "WHERE\n";
        AP.print_args(stderr);
        std::exit(EXIT_FAILURE);
    }

    Catalog &C = Catalog::Get();
    Diagnostic diag(false, std::cout, std::cerr);
    Sema sema(diag);

    /* Read the SETUP.SQL file. */
    {
        auto setup_filename = AP.args()[0];
        std::ifstream file(setup_filename);
        Lexer lexer(diag, C.get_pool(), "-", file);
        Parser parser(lexer);

        while (parser.token()) {
            auto stmt = parser.parse();
            if (diag.num_errors())
                std::exit(EXIT_FAILURE);
            sema(*stmt);
            if (diag.num_errors())
                std::exit(EXIT_FAILURE);
            delete stmt;
        }
    }

    /* Back all tables with row stores. */
    auto &DB = C.get_database_in_use();
    for (auto it = DB.begin_tables(); it != DB.end_tables(); ++it) {
        auto store = new RowStore(*it->second);
        it->second->store(store);
    }

    std::cout << "Setup complete.\n" << std::endl;

    /* Accept queries from stdin. */
    {
        Lexer lexer(diag, C.get_pool(), "-", std::cin);
        Parser parser(lexer);

        DummyJoinOrderer the_orderer;
        DummyCostModel the_costmodel;
        Optimizer O(the_orderer, the_costmodel);

        while (parser.token()) {
            auto stmt = parser.parse();
            if (diag.num_errors())
                std::exit(EXIT_FAILURE);
            sema(*stmt);
            if (diag.num_errors())
                std::exit(EXIT_FAILURE);

            if (auto select = cast<SelectStmt>(stmt)) {
                auto JG = JoinGraph::Build(*select);
                auto plan = O(*JG.get());

                if (opdot)
                    plan->dot(std::cout);
            }

            delete stmt;
        }
    }
}
