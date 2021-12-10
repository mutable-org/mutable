#include "backend/Interpreter.hpp"
#include "backend/StackMachine.hpp"
#include "backend/WebAssembly.hpp"
#include "catalog/Schema.hpp"
#include "globals.hpp"
#include "io/Reader.hpp"
#include "IR/PDDL.hpp"
#include <mutable/catalog/CostModel.hpp>
#include <mutable/mutable.hpp>
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "util/ArgParser.hpp"
#include "util/glyphs.hpp"
#include "util/terminal.hpp"
#include <cerrno>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iomanip>
#include <ios>
#include <iostream>
#include <regex>
#include <replxx.hxx>
#include <vector>

#if __linux
#include <unistd.h>

#elif __APPLE__
#include <unistd.h>
#endif


/* forward declarations */
namespace m {

struct PlanTable;

}


using namespace m;
using Replxx = replxx::Replxx;


void usage(std::ostream &out, const char *name)
{
    out << "An interactive shell to communicate with the database system.\n"
        << "USAGE:\n\t" << name << " [<FILE>...]"
        << std::endl;
}

std::string prompt(bool is_editing, Timer::duration dur = Timer::duration())
{
    unsigned bg = 242;
    std::ostringstream prompt;

    if (not is_editing) {
        if (dur != Timer::duration()) {
            const auto ms = std::chrono::duration_cast<std::chrono::microseconds>(dur).count() / 1e3;
            bg = 242;
            prompt << term::fg(220) << term::bg(bg) << ' ' << term::BOLD << glyphs::CLOCK_FAST << term::RESET
                << term::fg(220) << term::bg(bg) << ' '
                << std::fixed << std::setprecision(2) <<  ms << " ms ";
            bg = 238;
            prompt << term::bg(bg) << term::fg(242) << glyphs::RIGHT << ' ';
        }

        bg = 238;
        static auto &C = Catalog::Get();
        prompt << term::bg(bg) << term::FG_WHITE << " mu"
            << term::ITALIC << term::fg(30) << 't' << term::RESET
            << term::bg(bg) << term::FG_WHITE << "able ";
        if (C.has_database_in_use()) {
            auto &DB = C.get_database_in_use();
            prompt << term::fg(bg);
            bg = 242;
            prompt << term::bg(bg) << glyphs::RIGHT << term::FG_WHITE
                << ' ' << glyphs::DATABASE << ' ' << DB.name << ' ';
        }
    }
    prompt << term::BG_DEFAULT << term::fg(bg) << glyphs::RIGHT << term::RESET << ' ';
    return prompt.str();
}


void process_stream(std::istream &in, const char *filename, Diagnostic diag)
{
    Catalog &C = Catalog::Get();
    Sema sema(diag);
    std::size_t num_errors = 0;
    const bool is_stdin = streq(filename, "-");

    /*----- Process the input stream. --------------------------------------------------------------------------------*/
    Lexer lexer(diag, C.get_pool(), filename, in);
    Parser parser(lexer);

    while (parser.token()) {
        Timer &timer = C.timer();
        auto stmt = parser.parse();
        if (Options::Get().echo)
            std::cout << *stmt << std::endl;
        if (diag.num_errors() != num_errors) goto next;
        M_TIME_EXPR(sema(*stmt), "Semantic Analysis", timer);
        if (Options::Get().ast) stmt->dump(std::cout);
        if (Options::Get().astdot) {
            DotTool dot(diag);
            stmt->dot(dot.stream());
            dot.show("ast", is_stdin);
        }
        if (diag.num_errors() != num_errors) goto next;

        if (is<SelectStmt>(stmt)) {
            auto query_graph = M_TIME_EXPR(QueryGraph::Build(*stmt), "Construct the query graph", timer);
            if (Options::Get().graph) query_graph->dump(std::cout);
            if (Options::Get().graphdot) {
                DotTool dot(diag);
                query_graph->dot(dot.stream());
                dot.show("graph", is_stdin, "fdp");
            }
            if (Options::Get().graph2sql) {
                query_graph->sql(std::cout);
                std::cout.flush();
            }
            if (Options::Get().pddl) {
                std::filesystem::path domain_path = Options::Get().pddl;
                domain_path.append("domain/");

                std::filesystem::path problem_path = Options::Get().pddl;
                problem_path.append("problem/");

                PDDLGenerator PDDL(Catalog::Get().get_database_in_use().cardinality_estimator(), diag);
                PDDL.generate_files(*query_graph.get(), Options::Get().pddl_actions, domain_path, problem_path);
            }

            std::unique_ptr<PlanEnumerator> pe = PlanEnumerator::Create(Options::Get().plan_enumerator);
            Optimizer Opt(*pe.get(), C.cost_function());
            auto optree = M_TIME_EXPR(Opt(*query_graph.get()), "Compute the query plan", timer);
            if (Options::Get().plan) optree->dump(std::cout);
            if (Options::Get().plandot) {
                DotTool dot(diag);
                optree->dot(dot.stream());
                dot.show("plan", is_stdin);
            }

            std::unique_ptr<Consumer> plan;
            if (Options::Get().benchmark) {
                plan = std::make_unique<NoOpOperator>(std::cout);
            } else {
#if 0
                auto print = [&](const Schema &S, const Tuple &t) { t.print(std::cout, S); std::cout << '\n'; };
                plan = std::make_unique<CallbackOperator>(print);
#else
                plan = std::make_unique<PrintOperator>(std::cout);
#endif
            }
            plan->add_child(optree.release());

#if M_WITH_V8
            if (Options::Get().dryrun and streq("WasmV8", Options::Get().backend)) {
                auto backend = Backend::Create(Options::Get().backend);
                auto &platform = as<WasmBackend>(*backend).platform();
                WasmModule wasm = M_TIME_EXPR(platform.compile(*plan), "Compile to WebAssembly", timer);
                wasm.dump(std::cout);
            }
#endif

            if (not Options::Get().dryrun) {
                auto backend = Backend::Create(Options::Get().backend);
                M_TIME_THIS("Execute query", timer);
                backend->execute(*plan);
            }
        } else if (auto I = cast<InsertStmt>(stmt)) {
            auto &DB = C.get_database_in_use();
            auto &T = DB.get_table(I->table_name.text);
            auto &store = T.store();
            StoreWriter W(store);
            auto &S = W.schema();
            Tuple tup(S);

            /* Write all tuples to the store. */
            for (auto &t : I->tuples) {
                StackMachine get_tuple(Schema{});
                for (std::size_t i = 0; i != t.size(); ++i) {
                    auto &v = t[i];
                    switch (v.first) {
                        case InsertStmt::I_Null:
                            get_tuple.emit_St_Tup_Null(0, i);
                            break;

                        case InsertStmt::I_Default:
                            /* nothing to be done, Tuples are initialized to default values */
                            break;

                        case InsertStmt::I_Expr:
                            get_tuple.emit(*v.second);
                            get_tuple.emit_Cast(S[i].type, v.second->type());
                            get_tuple.emit_St_Tup(0, i, S[i].type);
                            break;
                    }
                }
                Tuple *args[] = { &tup };
                get_tuple(args);
                W.append(tup);
            }
        } else if (auto S = cast<CreateTableStmt>(stmt)) {
            auto &DB = C.get_database_in_use();
            auto &T = DB.get_table(S->table_name.text);
            T.store(Store::Create(Options::Get().store, T));
        } else if (auto S = cast<DSVImportStmt>(stmt)) {
            auto &DB = C.get_database_in_use();
            auto &T = DB.get_table(S->table_name.text);

            DSVReader R(T, diag);
            if (S->rows) R.num_rows = strtol(S->rows.text, nullptr, 10);
            if (S->delimiter) R.delimiter = unescape(S->delimiter.text)[1];
            if (S->escape) R.escape = unescape(S->escape.text)[1];
            if (S->quote) R.quote = unescape(S->quote.text)[1];
            R.has_header = S->has_header;
            R.skip_header = S->skip_header;

            std::string filename(S->path.text, 1, strlen(S->path.text) - 2);
            errno = 0;
            std::ifstream file(filename);
            if (not file) {
                const auto errsv = errno;
                diag.e(S->path.pos) << "Could not open file '" << S->path.text << '\'';
                if (errsv)
                    diag.err() << ": " << strerror(errsv);
                diag.err() << std::endl;
            } else {
                M_TIME_EXPR(R(file, S->path.text), "Read DSV file", timer);
            }
        }
next:
        num_errors = diag.num_errors();
        delete stmt;

        if (Options::Get().times) {
            using namespace std::chrono;
            for (const auto &M : timer) {
                if (M.is_finished())
                    std::cout << M.name << ": " << duration_cast<microseconds>(M.duration()).count() / 1e3 << '\n';
            }
            std::cout.flush();
            timer.clear();
        }
    }

    std::cout.flush();
    std::cerr.flush();
}

/** Determine codepoint length of utf-8 string. */
int utf8str_codepoint_len(const char *s, int utf8_len) {
    int codepoint_len = 0;
    unsigned char m4 = 128 + 64 + 32 + 16;
    unsigned char m3 = 128 + 64 + 32;
    unsigned char m2 = 128 + 64;
    for (int i = 0; i < utf8_len; ++i, ++codepoint_len ) {
        char c = s[i];
        if ((c & m4) == m4)
            i += 3;
        else if (( c & m3 ) == m3)
            i += 2;
        else if (( c & m2 ) == m2)
            i += 1;
    }
    return codepoint_len;
}

/** Determines the amount of chars after a word breaker (i.e. a non-alphanumeric character). */
int context_len(const std::string &prefix)
{
    auto it = prefix.rbegin();
    for (; it != prefix.rend(); ++it) {
        if (not is_alnum(*it))
            break;
    }

    return it - prefix.rbegin();
}

/* Completion */
Replxx::completions_t hook_completion(const std::string &prefix, int &context_len)
{
    static constexpr const char *KW[] = {
#define M_KEYWORD(tt, name) #name,
#include <mutable/tables/Keywords.tbl>
#undef M_KEYWORD
    };

    Replxx::completions_t completions;
    Replxx::Color color = Replxx::Color::DEFAULT;
    context_len = ::context_len(prefix);
    std::string context = prefix.substr(prefix.size() - context_len);
    for (auto const &kw : KW) {
        if (strneq(kw, context.c_str(), context_len))
            completions.emplace_back(kw, color);
    }
    return completions;
}

/* Highlighter */
void hook_highlighter(const std::string &context, Replxx::colors_t &colors)
{
    std::vector<std::pair<std::string, Replxx::Color>> regex_color = {
        /* Keywords */
#define M_KEYWORD(tt, name)\
        { #name, Replxx::Color::BROWN },
#include <mutable/tables/Keywords.tbl>
#undef M_KEYWORD
        /* Operators */
        { "\\(",  Replxx::Color::NORMAL},
        { "\\)",  Replxx::Color::NORMAL},
        { "\\~",  Replxx::Color::NORMAL},
        { "\\+",  Replxx::Color::NORMAL},
        { "\\-",  Replxx::Color::NORMAL},
        { "\\*",  Replxx::Color::NORMAL},
        { "\\/",  Replxx::Color::NORMAL},
        { "\\%",  Replxx::Color::NORMAL},
        { "\\.",  Replxx::Color::NORMAL},
        { "\\=",  Replxx::Color::NORMAL},
        { "\\!=", Replxx::Color::NORMAL},
        { "\\<",  Replxx::Color::NORMAL},
        { "\\>",  Replxx::Color::NORMAL},
        /* Constants */
        {"[\\-|+]{0,1}[0-9]+",          Replxx::Color::BLUE}, // integral numbers
        {"[\\-|+]{0,1}[0-9]*\\.[0-9]+", Replxx::Color::BLUE}, // fixed-point and floating-point numbers
        {"\"([^\\\\\"]|\\\\\")*\"",     Replxx::Color::BRIGHTMAGENTA}, // double quoted strings
    };
    for (const auto &e : regex_color) {
        std::size_t pos = 0;
        std::string str = context; // string that is yet to be searched
        std::smatch match;

        while (std::regex_search(str, match, std::regex(e.first))) {
            std::string c = match[0]; // first match for regex
            std::string prefix = match.prefix().str(); // substring until first match
            pos += utf8str_codepoint_len(prefix.c_str(), prefix.size());
            int len = utf8str_codepoint_len(c.c_str(), c.size());

            for (int i = 0; i < len; ++i)
                colors.at(pos + i) = e.second; // set colors according to match

            pos += len; // search for regex from pos onward
            str = match.suffix();
        }
    }
}

/* Hints */
Replxx::hints_t hook_hint(const std::string &prefix, int &context_len, Replxx::Color &color)
{
    static constexpr const char *KW[] = {
#define M_KEYWORD(tt, name) #name,
#include <mutable/tables/Keywords.tbl>
#undef M_KEYWORD
    };

    Replxx::hints_t hints;
    context_len = ::context_len(prefix);
    std::string context = prefix.substr(prefix.size() - context_len);
    if (context.size() >= 2) {
        for (auto const &kw : KW) {
            if (strneq(kw, context.c_str(), context_len))
                hints.emplace_back(kw);
        }
    }
    if (hints.size() == 1)
        color = Replxx::Color::GREEN;
    return hints;
}

int main(int argc, const char **argv)
{
    /* Identify whether the terminal supports colors. */
    const bool term_has_color = term::has_color();
    /* TODO Identify whether the terminal uses a unicode character encoding. */
    (void) term_has_color;

    /*----- Parse command line arguments. ----------------------------------------------------------------------------*/
    ArgParser AP;
#define ADD(TYPE, VAR, INIT, SHORT, LONG, DESCR, CALLBACK)\
    VAR = INIT;\
    {\
        std::function<void(TYPE)> callback = CALLBACK;\
        AP.add(SHORT, LONG, DESCR, callback);\
    }
    /*----- Help message ---------------------------------------------------------------------------------------------*/
    ADD(bool, Options::Get().show_help, false,              /* Type, Var, Init  */
        "-h", "--help",                                     /* Short, Long      */
        "prints this help message",                         /* Description      */
        [&](bool) { Options::Get().show_help = true; });    /* Callback         */
    ADD(bool, Options::Get().show_version, false,           /* Type, Var, Init  */
        nullptr, "--version",                               /* Short, Long      */
        "shows version information",                        /* Description      */
        [&](bool) { Options::Get().show_version = true; }); /* Callback         */
    /* Shell configuration */
    /*----- Shell configuration --------------------------------------------------------------------------------------*/
    ADD(bool, Options::Get().has_color, false,              /* Type, Var, Init  */
        nullptr, "--color",                                 /* Short, Long      */
        "use colors",                                       /* Description      */
        [&](bool) { Options::Get().has_color = true; });    /* Callback         */
    ADD(bool, Options::Get().show_prompt, true,             /* Type, Var, Init  */
        nullptr, "--noprompt",                              /* Short, Long      */
        "disable prompt",                                   /* Description      */
        [&](bool) { Options::Get().show_prompt = false; }); /* Callback         */
    ADD(bool, Options::Get().quiet, false,                  /* Type, Var, Init  */
        "-q", "--quiet",                                    /* Short, Long      */
        "work in quiet mode",                               /* Description      */
        [&](bool) { Options::Get().quiet = true; });        /* Callback         */
    /* Additional output */
    ADD(bool, Options::Get().times, false,                  /* Type, Var, Init  */
        "-t", "--times",                                    /* Short, Long      */
        "report exact timings",                             /* Description      */
        [&](bool) { Options::Get().times = true; });        /* Callback         */
    ADD(bool, Options::Get().echo, false,                   /* Type, Var, Init  */
        nullptr, "--echo",                                  /* Short, Long      */
        "echo statements",                                  /* Description      */
        [&](bool) { Options::Get().echo = true; });         /* Callback         */
    ADD(bool, Options::Get().ast, false,                    /* Type, Var, Init  */
        nullptr, "--ast",                                   /* Short, Long      */
        "print the AST of statements",                      /* Description      */
        [&](bool) { Options::Get().ast = true; });          /* Callback         */
    ADD(bool, Options::Get().astdot, false,                 /* Type, Var, Init  */
        nullptr, "--astdot",                                /* Short, Long      */
        "dot the AST of statements",                        /* Description      */
        [&](bool) { Options::Get().astdot = true; });       /* Callback         */
    ADD(bool, Options::Get().graph, false,                  /* Type, Var, Init  */
        nullptr, "--graph",                                 /* Short, Long      */
        "print the computed query graph",                   /* Description      */
        [&](bool) { Options::Get().graph = true; });        /* Callback         */
    ADD(bool, Options::Get().graphdot, false,               /* Type, Var, Init  */
        nullptr, "--graphdot",                              /* Short, Long      */
        "dot the computed query graph",                     /* Description      */
        [&](bool) { Options::Get().graphdot = true; });     /* Callback         */
    ADD(bool, Options::Get().graph2sql, false,               /* Type, Var, Init  */
        nullptr, "--graph2sql",                             /* Short, Long      */
        "translate the computed query graph into SQL",      /* Description      */
        [&](bool) { Options::Get().graph2sql = true; });    /* Callback         */
    ADD(bool, Options::Get().plan, false,                   /* Type, Var, Init  */
        nullptr, "--plan",                                  /* Short, Long      */
        "emit the chosen execution plan",                   /* Description      */
        [&](bool) { Options::Get().plan = true; });         /* Callback         */
    ADD(bool, Options::Get().plandot, false,                /* Type, Var, Init  */
        nullptr, "--plandot",                               /* Short, Long      */
        "dot the chosen operator tree",                     /* Description      */
        [&](bool) { Options::Get().plandot = true; });      /* Callback         */
    ADD(bool, Options::Get().dryrun, false,                 /* Type, Var, Init  */
        nullptr, "--dryrun",                                /* Short, Long      */
        "don't actually execute the query",                 /* Description      */
        [&](bool) { Options::Get().dryrun = true; });       /* Callback         */
    ADD(bool, Options::Get().benchmark, false,              /* Type, Var, Init  */
        nullptr, "--benchmark",                             /* Short, Long      */
        "run queries in benchmark mode",                    /* Description      */
        [&](bool) { Options::Get().benchmark = true; });    /* Callback         */
#if M_WITH_V8
    /*----- Enable Chrome DevTools debugging via web socket ----------------------------------------------------------*/
    ADD(int, Options::Get().cdt_port, 0,                  /* Type, Var, Init  */
        nullptr, "--CDT",                                   /* Short, Long      */
        "specify the port for debugging via ChomeDevTools", /* Description      */
        [&](int port) { Options::Get().cdt_port = port; }); /* Callback         */
    /*----- Add optimization flag ------------------------------------------------------------------------------------*/
    ADD(int, Options::Get().wasm_optimization_level, 0,                         /* Type, Var, Init  */
        "-O", "--wasm-opt",                                                     /* Short, Long      */
        "set the optimization level for Wasm modules (0, 1, or 2)",             /* Description      */
        [&](int olevel) { Options::Get().wasm_optimization_level = olevel; });  /* Callback         */
    /*----- Add flag to enable adaptive mode (Liftoff + dynamic tier-up) ---------------------------------------------*/
    ADD(bool, Options::Get().wasm_adaptive, false,                              /* Type, Var, Init  */
        nullptr, "--wasm-adaptive",                                             /* Short, Long      */
        "enable adaptive execution of Wasm with Liftoff and dynamic tier-up",   /* Description      */
        [&](bool) { Options::Get().wasm_adaptive = true; });                    /* Callback         */
#endif

    /*----- Select store implementation ------------------------------------------------------------------------------*/
    ADD(const char *, Options::Get().store,                 /* Type, Var        */
        "RowStore",                                         /* Init             */
        nullptr, "--store",                                 /* Short, Long      */
        "specify the store",                                /* Description      */
        /* Callback         */
        [&](const char *str) {
            if (Store::STR_TO_KIND.find(str) == Store::STR_TO_KIND.end()) {
                std::cerr << "There is no store with the name \"" << str << "\"." << std::endl;
                AP.print_args(stderr);
                std::exit(EXIT_FAILURE);
            }
            Options::Get().store = str;
        }
       );
    ADD(bool, Options::Get().list_stores, false,            /* Type, Var, Init  */
        nullptr, "--list-stores",                           /* Short, Long      */
        "list all available stores",                        /* Description      */
        /* Callback */
        [&](bool) { Options::Get().list_stores = true; }
    );

    /*----- Select plan enumerator implementation --------------------------------------------------------------------*/
    ADD(const char *, Options::Get().plan_enumerator,       /* Type, Var        */
        "DPccp",                                            /* Init             */
        nullptr, "--plan-enumerator",                       /* Short, Long      */
        "specify the plan enumerator",                      /* Description      */
        /* Callback         */
        [&](const char *str) {
            if (PlanEnumerator::STR_TO_KIND.find(str) == PlanEnumerator::STR_TO_KIND.end()) {
                std::cerr << "There is no plan enumerator with the name \"" << str << "\"." << std::endl;
                AP.print_args(stderr);
                std::exit(EXIT_FAILURE);
            }
            Options::Get().plan_enumerator = str;
        }
       );
    ADD(bool, Options::Get().list_plan_enumerators, false,  /* Type, Var, Init  */
        nullptr, "--list-plan-enumerators",                 /* Short, Long      */
        "list all available plan enumerators",              /* Description      */
        /* Callback */
        [&](bool) { Options::Get().list_plan_enumerators = true; }
    );

    /*----- Select backend implementation ----------------------------------------------------------------------------*/
    ADD(const char *, Options::Get().backend,               /* Type, Var        */
        "Interpreter",                                      /* Init             */
        nullptr, "--backend",                               /* Short, Long      */
        "specify the execution backend",                    /* Description      */
        /* Callback         */
        [&](const char *str) {
            if (Backend::STR_TO_KIND.find(str) == Backend::STR_TO_KIND.end()) {
                std::cerr << "There is no execution backend with the name \"" << str << "\"." << std::endl;
                AP.print_args(stderr);
                std::exit(EXIT_FAILURE);
            }
            Options::Get().backend = str;
        }
       );
    ADD(bool, Options::Get().list_backends, false,          /* Type, Var, Init  */
        nullptr, "--list-backends",                         /* Short, Long      */
        "list all available backends",                      /* Description      */
        /* Callback */
        [&](bool) { Options::Get().list_backends = true; }
    );
    /*----- Select cardinality estimator -----------------------------------------------------------------------------*/
    ADD(const char *, Options::Get().cardinality_estimator,       /* Type, Var        */
        "CartesianProductEstimator",                              /* Init             */
        nullptr, "--cardinality-estimator",                       /* Short, Long      */
        "specify the cardinality estimator",                      /* Description      */
        /* Callback         */
        [&](const char *str) {
            if (CardinalityEstimator::STR_TO_KIND.find(str) == CardinalityEstimator::STR_TO_KIND.end()) {
                std::cerr << "There is no cardinality estimator with the name \"" << str << "\"." << std::endl;
                AP.print_args(stderr);
                std::exit(EXIT_FAILURE);
            }
            Options::Get().cardinality_estimator = str;
        }
    );

    ADD(bool, Options::Get().list_cardinality_estimators, false,    /* Type, Var, Init  */
        nullptr, "--list-cardinality-estimators",                   /* Short, Long      */
        "list all available cardinality estimators",                /* Description      */
        /* Callback */
        [&](bool) { Options::Get().list_cardinality_estimators = true; }
    );
    ADD(bool, Options::Get().show_injected_cardinalities_example, false,            /* Type, Var, Init  */
        nullptr, "--show-cardinality-example",                                      /* Short, Long      */
        "show example input for injected cardinalities",                            /* Description      */
        [&](bool) { Options::Get().show_injected_cardinalities_example = true; }    /* Callback         */
    );
    ADD(const char *, Options::Get().injected_cardinalities_file, nullptr,          /* Type, Var, Init  */
        nullptr, "--use-cardinality-file",                                          /* Short, Long      */
        "specify file with cardinalities to inject",                                /* Description      */
        [&](const char *str) { Options::Get().injected_cardinalities_file = str; }  /* Callback         */
    );
    /*----- PDDL Generation ------------------------------------------------------------------------------------------*/
    ADD(const char *, Options::Get().pddl, nullptr,                     /* Type, Var, Init  */
        nullptr, "--pddl",                                              /* Short, Long      */
        /* Description      */
        "generate PDDL files for the query, as a parameter specify where to save the PDDL files",
        [&](const char *str) { Options::Get().pddl = str; }             /* Callback         */
    );
    ADD(int, Options::Get().pddl_actions, 4,                                /* Type, Var, Init  */
        nullptr, "--pddl-actions",                                          /* Short, Long      */
        /* Description      */
        "specify the number of actions used for the PDDL files (2,3 or 4), 0 will create all 3 models",
        [&](int number) { Options::Get().pddl_actions = number; }           /* Callback         */
    );
    /*------ Cost Model Generation -----------------------------------------------------------------------------------*/
    ADD(bool, Options::Get().train_cost_models, false,                  /* Type, Var, Init  */
        nullptr, "--train-cost-models",                                 /* Short, Long      */
        "train cost models (may take a couple of minutes)",             /* Description      */
        [&](bool) { Options::Get().train_cost_models = true; }          /* Callback         */
    );
    /*----- AIPlanning Config ----------------------------------------------------------------------------------------*/
    ADD(const char *, Options::Get().ai_state, "BottomUp",                              /* Type, Var, Init  */
        nullptr, "--ai-state",                                                          /* Short, Long      */
        "specify which state definition to use for plan enumeration via AIPlanning",    /* Description      */
        [&](const char *str) { Options::Get().ai_state = str; }                         /* Callback         */
    );
    ADD(const char *, Options::Get().ai_heuristic, "hsum",                              /* Type, Var, Init  */
        nullptr, "--ai-heuristic",                                                      /* Short, Long      */
        "specify which heuristic to use for plan enumeration via AIPlanning",           /* Description      */
        [&](const char *str) { Options::Get().ai_heuristic = str; }                     /* Callback         */
    );
    ADD(const char *, Options::Get().ai_search, "AStar",                                /* Type, Var, Init  */
        nullptr, "--ai-search",                                                         /* Short, Long      */
        "specify which search algorithm to use for plan enumeration via AIPlanning",    /* Description      */
        [&](const char *str) { Options::Get().ai_search = str; }                        /* Callback         */
    );
#undef ADD
    AP.parse_args(argc, argv);

    if (Options::Get().show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    if (Options::Get().show_version) {
        if (term_has_color)
            std::cout << "mu" << Diagnostic::ITALIC << 't' << Diagnostic::RESET << "able";
        else
            std::cout << "mutable";
        std::cout << "\n© 2021, Big Data Analytics Group";
        auto &v = version::get();
        std::cout << "\nversion " << v.GIT_REV;
        if (not streq(v.GIT_BRANCH, ""))
            std::cout << " (" << v.GIT_BRANCH << ')';
        std::cout << "\nAuthors: \
Immanuel Haffner\
, Joris Nix\
, Marcel Maltry\
, Luca Gretscher\
, Tobias Kopp\
, Jonas Lauermann\
, Felix Brinkmann\
";
        std::cout << std::endl;
        std::exit(EXIT_SUCCESS);
    }

    if (Options::Get().list_stores) {
        std::cout << "List of available stores:";
        constexpr std::pair<const char*, const char*> stores[] = {
#define M_STORE(NAME, DESCR) { #NAME, DESCR },
#include <mutable/tables/Store.tbl>
#undef M_STORE
        };
        std::size_t max_len = 0;
        for (auto store : stores) max_len = std::max(max_len, strlen(store.first));
        for (auto store : stores)
            std::cout << "\n    " << std::setw(max_len) << std::left << store.first << "    -    " << store.second;
        std::cout << std::endl;
        std::exit(EXIT_SUCCESS);
    }

    if (Options::Get().list_backends) {
        std::cout << "List of available backends:";
        constexpr std::pair<const char*, const char*> backends[] = {
#define M_BACKEND(NAME, DESCR) { #NAME, DESCR },
#include <mutable/tables/Backend.tbl>
#undef M_BACKEND
        };
        std::size_t max_len = 0;
        for (auto backend : backends) max_len = std::max(max_len, strlen(backend.first));
        for (auto backend : backends)
            std::cout << "\n    " << std::setw(max_len) << std::left << backend.first << "    -    " << backend.second;
        std::cout << std::endl;
        std::exit(EXIT_SUCCESS);
    }

    if (Options::Get().list_plan_enumerators) {
        std::cout << "List of available plan enumerators:";
        constexpr std::pair<const char*, const char*> PE[] = {
#define M_PLAN_ENUMERATOR(NAME, DESCR) { #NAME, DESCR },
#include <mutable/tables/PlanEnumerator.tbl>
#undef M_PLAN_ENUMERATOR
        };
        std::size_t max_len = 0;
        for (auto pe : PE) max_len = std::max(max_len, strlen(pe.first));
        for (auto pe : PE)
            std::cout << "\n    " << std::setw(max_len) << std::left << pe.first << "    -    " << pe.second;
        std::cout << std::endl;
        std::exit(EXIT_SUCCESS);
    }

    if (Options::Get().list_cardinality_estimators) {
        std::cout << "List of available cardinality estimators:";
        constexpr std::pair<const char*, const char*> CE[] = {
#define M_CARDINALITY_ESTIMATOR(NAME, DESCR) { #NAME, DESCR },
#include <mutable/tables/CardinalityEstimator.tbl>
#undef M_CARDINALITY_ESTIMATOR
        };
        std::size_t max_len = 0;
        for (auto ce : CE) max_len = std::max(max_len, strlen(ce.first));
        for (auto ce : CE)
            std::cout << "\n    " << std::setw(max_len) << std::left << ce.first << "    -    " << ce.second;
        std::cout << std::endl;
        std::exit(EXIT_SUCCESS);
    }

    if (Options::Get().show_injected_cardinalities_example) {
        std::cout << "\
Example for injected cardinalities file:\n\
{\n\
    database1: [\n\
            {\n\
                \"relations\": [\"A\", \"B\", ...],\n\
                \"size\": 150\n\
            },\n\
            {\n\
                \"relations\": [\"C\", \"A\", ...],\n\
                \"size\": 100\n\
            },\n\
    },\n\
    database2: [\n\
            {\n\
                \"relations\": [\"customers\"],\n\
                \"size\": 1000\n\
            },\n\
            {\n\
                \"relations\": [\"customers\", \"orders\", ...],\n\
                \"size\": 50\n\
            },\n\
    },\n\
}\n";
        std::exit(EXIT_SUCCESS);
    }

    if (not Options::Get().quiet) {
        std::cout << "PID";
#if __linux || __APPLE__
        std::cout << ' ' << getpid();
#endif
        std::cout << std::endl;
    }

#if M_WITH_V8
    if (not (Options::Get().wasm_optimization_level >= 0 and Options::Get().wasm_optimization_level <= 2)) {
        std::cerr << "level " << Options::Get().wasm_optimization_level << " is not a valid Wasm optimization level"
                  << std::endl;
        std::exit(EXIT_FAILURE);
    }
#endif

    /* Disable synchronisation between C and C++ I/O (e.g. stdin vs std::cin). */
    std::ios_base::sync_with_stdio(false);

    /* Create the diagnostics object. */
    Diagnostic diag(Options::Get().has_color, std::cout, std::cerr);

    /* ----- Cost model training -------------------------------------------------------------------------------------*/
    if (Options::Get().train_cost_models) {
        auto CF = CostModelFactory::get_cost_function();
        Catalog::Get().cost_function(std::move(CF));
    }

    /* ----- Replxx configuration ------------------------------------------------------------------------------------*/
    Replxx rx;
    rx.install_window_change_handler();

    /* History setup */
    auto history_file = get_home_path() / ".mutable_history";
    rx.history_load(history_file);
    rx.set_max_history_size(1024);
    rx.set_unique_history(false);

    /* Key bindings */
#define KEY_BIND(BUTTON, RESULT)\
    rx.bind_key(Replxx::KEY::BUTTON, std::bind(&Replxx::invoke, &rx, Replxx::ACTION::RESULT, std::placeholders::_1));
    KEY_BIND(BACKSPACE,                   DELETE_CHARACTER_LEFT_OF_CURSOR);
    KEY_BIND(DELETE,                      DELETE_CHARACTER_UNDER_CURSOR);
    KEY_BIND(LEFT,                        MOVE_CURSOR_LEFT);
    KEY_BIND(RIGHT,                       MOVE_CURSOR_RIGHT);
    KEY_BIND(UP,                          HISTORY_PREVIOUS);
    KEY_BIND(DOWN,                        HISTORY_NEXT);
    KEY_BIND(PAGE_UP,                     HISTORY_FIRST);
    KEY_BIND(PAGE_DOWN,                   HISTORY_LAST);
    KEY_BIND(HOME,                        MOVE_CURSOR_TO_BEGINING_OF_LINE);
    KEY_BIND(END,                         MOVE_CURSOR_TO_END_OF_LINE);
    KEY_BIND(TAB,                         COMPLETE_LINE);
    KEY_BIND(control('R'),                HISTORY_INCREMENTAL_SEARCH);
    KEY_BIND(control('W'),                KILL_TO_BEGINING_OF_WORD);
    KEY_BIND(control('U'),                KILL_TO_BEGINING_OF_LINE);
    KEY_BIND(control('K'),                KILL_TO_END_OF_LINE);
    KEY_BIND(control('Y'),                YANK);
    KEY_BIND(control('L'),                CLEAR_SCREEN);
    KEY_BIND(control('D'),                SEND_EOF);
    KEY_BIND(control('Z'),                SUSPEND);
    KEY_BIND(control(Replxx::KEY::ENTER), COMMIT_LINE);
    KEY_BIND(control(Replxx::KEY::LEFT),  MOVE_CURSOR_ONE_WORD_LEFT);
    KEY_BIND(control(Replxx::KEY::RIGHT), MOVE_CURSOR_ONE_WORD_RIGHT);
    KEY_BIND(control(Replxx::KEY::UP),    HINT_PREVIOUS);
    KEY_BIND(control(Replxx::KEY::DOWN),  HINT_NEXT);
    KEY_BIND(meta('p'),                   HISTORY_COMMON_PREFIX_SEARCH);
    KEY_BIND(meta(Replxx::KEY::BACKSPACE),KILL_TO_BEGINING_OF_WORD);
#undef KEY_BIND

    if (Options::Get().show_prompt) {
        /* Completion */
        rx.set_completion_callback(std::bind(&hook_completion, std::placeholders::_1, std::placeholders::_2));
        rx.set_completion_count_cutoff(128);
        rx.set_double_tab_completion(false);
        rx.set_complete_on_empty(false);
        rx.set_beep_on_ambiguous_completion(false);

        /* Hints */
        rx.set_hint_callback(std::bind(&hook_hint, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
        rx.set_max_hint_rows(3);
        rx.set_hint_delay(500);

        /* Highlighter */
        rx.set_highlighter_callback(std::bind(&hook_highlighter, std::placeholders::_1, std::placeholders::_2));
    }

    /* Other options */
    rx.set_word_break_characters(" \t.,-%!;:=*~^'\"/?<>|[](){}");
    rx.set_no_color(not Options::Get().show_prompt);

    auto args = AP.args();
    if (args.empty())
        args.push_back("-"); // start in interactive mode

    /* Process all the inputs. */
    for (auto filename : args) {
        /*----- Open the input stream. -------------------------------------------------------------------------------*/
        if (streq("-", filename)) {
            const char *cinput = nullptr;
            std::stringstream ss;
            for (;;) {
                do
                    cinput = rx.input(Options::Get().show_prompt ? prompt(ss.str().size() != 0) : ""); // Read one line of input
                while ((cinput == nullptr) and (errno == EAGAIN));
                M_insist(errno != EAGAIN);

                /* User sent EOF */
                if (cinput == nullptr) {
                    if (Options::Get().show_prompt)
                        std::cout << std::endl;
                    break;
                }
                M_insist(cinput);

                /* User sent input */
                auto len = strlen(cinput);
                if (not isspace(cinput, len)) {
                    ss.write(cinput, len); // append replxx line to stream
                    rx.history_add(cinput);
                    if (cinput[len - 1] == ';') {
                        process_stream(ss, filename, diag);
                        ss.str(""); // empty the stream
                        ss.clear(); // and clear EOF bit
                    } else
                        ss.put('\n');
                }
            }
            rx.history_save(history_file);
        } else {
            std::ifstream in(filename);
            if (not in) {
                diag.err() << "Could not open file '" << filename << '\'';
                if (errno)
                    diag.err() << ": " << strerror(errno);
                diag.err() << ".  Aborting." << std::endl;
                break;
            }
            process_stream(in, filename, diag);
        }
    }

    std::exit(diag.num_errors() ? EXIT_FAILURE : EXIT_SUCCESS);
}
