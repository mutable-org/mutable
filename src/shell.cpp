#include "catalog/Schema.hpp"
#include "io/Reader.hpp"
#include "IR/Interpreter.hpp"
#include "IR/Optimizer.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "storage/RowStore.hpp"
#include "util/ArgParser.hpp"
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>


using namespace db;


void usage(std::ostream &out, const char *name)
{
    out << "An interactive shell to communicate with the database system.\n"
        << "USAGE:\n\t" << name << " [<FILE>...]"
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
    ADD(bool, ast, false,                   /* Type, Var, Init  */
        nullptr, "--ast",                   /* Short, Long      */
        "dot the AST of statements",        /* Description      */
        [&](bool) { ast = true; });         /* Callback         */
    ADD(bool, astdot, false,                /* Type, Var, Init  */
        nullptr, "--astdot",                /* Short, Long      */
        "dot the AST of statements",        /* Description      */
        [&](bool) { astdot = true; });      /* Callback         */
    ADD(bool, graphdot, false,              /* Type, Var, Init  */
        nullptr, "--graphdot",              /* Short, Long      */
        "dot the computed join graph",      /* Description      */
        [&](bool) { graphdot = true; });    /* Callback         */
    ADD(bool, plan, false,                  /* Type, Var, Init  */
        nullptr, "--plan",                  /* Short, Long      */
        "emit the chosen execution plan",   /* Description      */
        [&](bool) { plan = true; });        /* Callback         */
    ADD(bool, plandot, false,                 /* Type, Var, Init  */
        nullptr, "--plandot",                 /* Short, Long      */
        "dot the chosen operator tree",     /* Description      */
        [&](bool) { plandot = true; });       /* Callback         */
    ADD(bool, dryrun, false,                /* Type, Var, Init  */
        nullptr, "--dryrun",                /* Short, Long      */
        "don't actually execute the query", /* Description      */
        [&](bool) { dryrun = true; });      /* Callback         */
#undef ADD
    AP.parse_args(argc, argv);

    if (show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    Catalog &C = Catalog::Get();
    Diagnostic diag(color, std::cout, std::cerr);
    Sema sema(diag);
    DummyJoinOrderer orderer;
    DummyCostModel costmodel;
    Optimizer Opt(orderer, costmodel);
    Interpreter I;
    std::size_t num_errors = 0;

    auto print = [](const OperatorSchema &schema, const tuple_type &t) {
        db::print(std::cout, schema, t);
        std::cout << '\n';
    };

    auto args = AP.args();
    if (args.empty())
        args.push_back("-"); // start in interactive mode

    /* Process all the inputs. */
    for (auto filename : args) {
        /*----- Open the input stream. -------------------------------------------------------------------------------*/
        std::istream *in;
        if (streq("-", filename)) {
            in = &std::cin;
        } else {
            in = new std::ifstream(filename);
            if (not *in) {
                diag.err() << "Could not open file '" << filename << '\'';
                if (errno)
                    diag.err() << ": " << strerror(errno);
                diag.err() << ".  Aborting." << std::endl;
                break;
            }
        }

        /*----- Process the input stream. ----------------------------------------------------------------------------*/
        Lexer lexer(diag, C.get_pool(), filename, *in);
        Parser parser(lexer);

        if (in == &std::cin) {
            std::cout << u8"db \uf6b7> ";
            std::cout.flush();
        }
        while (parser.token()) {
            auto stmt = parser.parse();
            if (diag.num_errors()) goto next;
            sema(*stmt);
            if (ast) stmt->dump(std::cout);
            if (astdot) stmt->dot(std::cout);
            if (diag.num_errors()) goto next;

            if (is<SelectStmt>(stmt)) {
                auto joingraph = JoinGraph::Build(*stmt);
                if (graphdot) joingraph->dot(std::cout);

                auto optree = Opt(*joingraph.get()).release();
                if (plan) optree->dump(std::cout);
                if (plandot) optree->dot(std::cout);

                if (not dryrun) {
                    auto callback = new CallbackOperator(print);
                    callback->add_child(optree);
                    I(*callback);
                    delete callback;
                }
            } else if (auto S = cast<CreateTableStmt>(stmt)) {
                auto &DB = C.get_database_in_use();
                auto &T = DB.get_table(S->table_name.text);
                T.store(new RowStore(T));
            } else if (auto S = cast<DSVImportStmt>(stmt)) {
                auto &DB = C.get_database_in_use();
                auto &T = DB.get_table(S->table_name.text);

                DSVReader R(T, diag);
                if (S->delimiter) R.delimiter = unescape(S->delimiter.text)[1];
                if (S->escape) R.escape = unescape(S->escape.text)[1];
                if (S->quote) R.quote = unescape(S->quote.text)[1];
                R.has_header = S->has_header;
                R.skip_header = S->skip_header;

                errno = 0;
                std::string filename(S->path.text, 1, strlen(S->path.text) - 2);
                std::ifstream file(filename);
                if (not file) {
                    diag.e(S->path.pos) << "Could not open file '" << S->path.text << '\'';
                    if (errno)
                        diag.err() << ": " << strerror(errno);
                    diag.err() << std::endl;
                } else {
                    R(file, S->path.text);
                }
            }
next:
            num_errors += diag.num_errors();
            diag.clear();
            delete stmt;

            if (in == &std::cin) {
                std::cout << u8"db \uf6b7> ";
                std::cout.flush();
            }
        }

        /*----- Clean up the input stream. ---------------------------------------------------------------------------*/
        if (in != &std::cin)
            delete in;
    }

    std::exit(num_errors ? EXIT_FAILURE : EXIT_SUCCESS);
}
