#include "backend/Interpreter.hpp"
#include "catalog/Schema.hpp"
#include "io/Reader.hpp"
#include "IR/Optimizer.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/RowStore.hpp"
#include "util/ArgParser.hpp"
#include "util/glyphs.hpp"
#include "util/terminal.hpp"
#include "util/Timer.hpp"
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <ios>
#include <iostream>
#include <vector>


using namespace db;


void usage(std::ostream &out, const char *name)
{
    out << "An interactive shell to communicate with the database system.\n"
        << "USAGE:\n\t" << name << " [<FILE>...]"
        << std::endl;
}

void prompt(std::ostream &out, Timer::duration dur = Timer::duration()) {
    unsigned bg;

    if (dur != Timer::duration()) {
        const auto ms = std::chrono::duration_cast<std::chrono::microseconds>(dur).count() / 1e3;
        bg = 242;
        out << term::fg(220) << term::bg(bg) << ' ' << term::BOLD << glyphs::CLOCK_FAST << term::RESET
            << term::fg(220) << term::bg(bg) << ' '
            << std::fixed << std::setprecision(2) <<  ms << " ms ";
        bg = 238;
        out << term::bg(bg) << term::fg(242) << glyphs::RIGHT << ' ';
    }

    bg = 238;
    static auto &C = Catalog::Get();
    out << term::bg(bg) << term::FG_WHITE << " TheDB ";
    if (C.has_database_in_use()) {
        auto &DB = C.get_database_in_use();
        out << term::fg(bg);
        bg = 242;
        out << term::bg(bg) << glyphs::RIGHT << term::FG_WHITE
            << ' ' << glyphs::DATABASE << ' ' << DB.name << ' ';
    }
    out << term::BG_DEFAULT << term::fg(bg) << glyphs::RIGHT << term::RESET << ' ';
}

struct options_t
{
    bool show_help;
    bool color;
    bool show_prompt;
    bool times;
    bool echo;
    bool ast;
    bool astdot;
    bool graphdot;
    bool plan;
    bool plandot;
    bool dryrun;
};

void process_stream(std::istream *in, const char *filename, options_t options, Diagnostic diag)
{
    // TODO: only create needed instnaces
    Catalog &C = Catalog::Get();
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

    /*----- Process the input stream. ----------------------------------------------------------------------------*/
    Lexer lexer(diag, C.get_pool(), filename, *in);
    Parser parser(lexer);

    if (options.show_prompt and in == &std::cin)
        prompt(std::cout);
    while (parser.token()) {
        Timer timer;
        auto stmt = parser.parse();
        if (options.echo)
            std::cout << *stmt << std::endl;
        if (diag.num_errors()) goto next;
        timer.start("Semantic Analysis");
        sema(*stmt);
        timer.stop();
        if (options.ast) stmt->dump(std::cout);
        if (options.astdot) stmt->dot(std::cout);
        if (diag.num_errors()) goto next;

        if (is<SelectStmt>(stmt)) {
            timer.start("Construct the Join Graph");
            auto joingraph = JoinGraph::Build(*stmt);
            timer.stop();
            if (options.graphdot) joingraph->dot(std::cout);

            timer.start("Compute an optimized Query Plan");
            auto optree = Opt(*joingraph.get()).release();
            timer.stop();
            if (options.plan) optree->dump(std::cout);
            if (options.plandot) optree->dot(std::cout);

            if (not options.dryrun) {
                auto callback = new CallbackOperator(print);
                callback->add_child(optree);
                timer.start("Interpret the Query Plan");
                I(*callback);
                timer.stop();
                delete callback;
            }
        } else if (auto I = cast<InsertStmt>(stmt)) {
            auto &DB = C.get_database_in_use();
            auto &T = DB.get_table(I->table_name.text);
            auto &store = T.store();
            std::vector<const Attribute*> attrs;
            for (auto &attr : T) attrs.push_back(&attr);
            auto W = store.writer(attrs);
            for (auto &t : I->tuples) {
                StackMachine S;
                for (auto &v : t) {
                    switch (v.first) {
                        case InsertStmt::I_Null:
                            S.add_and_emit_load(null_type());
                            break;

                        case InsertStmt::I_Default:
                            unreachable("not implemented");

                        case InsertStmt::I_Expr:
                            S.emit(*v.second);
                            break;
                    }
                }
                auto values = S();
                W(values);
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
                timer.start("Read DSV file");
                R(file, S->path.text);
                timer.stop();
            }
        }
next:
        num_errors += diag.num_errors();
        diag.clear();
        delete stmt;

        if (options.times)
            std::cout << timer;

        if (options.show_prompt and in == &std::cin)
            prompt(std::cout, timer.total());
    }
}


int main(int argc, const char **argv)
{

    /* Identify whether the terminal supports colors. */
    const bool term_has_color = term::has_color();
    /* TODO Identify whether the terminal uses a unicode character encoding. */
    (void) term_has_color;

    /*----- Parse command line arguments. ----------------------------------------------------------------------------*/
    options_t options;
    ArgParser AP;
#define ADD(TYPE, VAR, INIT, SHORT, LONG, DESCR, CALLBACK)\
    VAR = INIT;\
    {\
        std::function<void(TYPE)> callback = CALLBACK;\
        AP.add(SHORT, LONG, VAR, DESCR, callback);\
    }
    ADD(bool, options.show_help, false,                     /* Type, Var, Init  */
        "-h", "--help",                             /* Short, Long      */
        "prints this help message",                 /* Description      */
        [&](bool) { options.show_help = true; });      /* Callback         */
    ADD(bool, options.color, false,                         /* Type, Var, Init  */
        nullptr, "--color",                         /* Short, Long      */
        "use colors",                               /* Description      */
        [&](bool) { options.color = true; });          /* Callback         */
    ADD(bool, options.show_prompt, true,                    /* Type, Var, Init  */
        nullptr, "--noprompt",                      /* Short, Long      */
        "disable prompt",                           /* Description      */
        [&](bool) { options.show_prompt = false; });   /* Callback         */
    ADD(bool, options.times, false,                         /* Type, Var, Init  */
        "-t", "--times",                            /* Short, Long      */
        "report exact timings",                     /* Description      */
        [&](bool) { options.times = true; });          /* Callback         */
    ADD(bool, options.echo, false,                          /* Type, Var, Init  */
        nullptr, "--echo",                          /* Short, Long      */
        "echo statements",                          /* Description      */
        [&](bool) { options.echo = true; });           /* Callback         */
    ADD(bool, options.ast, false,                           /* Type, Var, Init  */
        nullptr, "--ast",                           /* Short, Long      */
        "dot the AST of statements",                /* Description      */
        [&](bool) { options.ast = true; });            /* Callback         */
    ADD(bool, options.astdot, false,                        /* Type, Var, Init  */
        nullptr, "--astdot",                        /* Short, Long      */
        "dot the AST of statements",                /* Description      */
        [&](bool) { options.astdot = true; });         /* Callback         */
    ADD(bool, options.graphdot, false,                      /* Type, Var, Init  */
        nullptr, "--graphdot",                      /* Short, Long      */
        "dot the computed join graph",              /* Description      */
        [&](bool) { options.graphdot = true; });       /* Callback         */
    ADD(bool, options.plan, false,                          /* Type, Var, Init  */
        nullptr, "--plan",                          /* Short, Long      */
        "emit the chosen execution plan",           /* Description      */
        [&](bool) { options.plan = true; });           /* Callback         */
    ADD(bool, options.plandot, false,                       /* Type, Var, Init  */
        nullptr, "--plandot",                       /* Short, Long      */
        "dot the chosen operator tree",             /* Description      */
        [&](bool) { options.plandot = true; });        /* Callback         */
    ADD(bool, options.dryrun, false,                        /* Type, Var, Init  */
        nullptr, "--dryrun",                        /* Short, Long      */
        "don't actually execute the query",         /* Description      */
        [&](bool) { options.dryrun = true; });         /* Callback         */
#undef ADD
    AP.parse_args(argc, argv);

    if (options.show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    /* Disable synchronisation between C and C++ I/O (e.g. stdin vs std::cin). */
    std::ios_base::sync_with_stdio(false);

    Diagnostic diag(options.color, std::cout, std::cerr);

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
        process_stream(in, filename, options, diag);
        /*----- Clean up the input stream. ---------------------------------------------------------------------------*/
        if (in != &std::cin)
            delete in;
    }

    std::exit(diag.num_errors() ? EXIT_FAILURE : EXIT_SUCCESS);
}
