#include "backend/Backend.hpp"
#include "backend/StackMachine.hpp"
#include "backend/WebAssembly.hpp"
#include "catalog/CostFunction.hpp"
#include "catalog/Schema.hpp"
#include "globals.hpp"
#include "io/Reader.hpp"
#include "IR/Optimizer.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "storage/Store.hpp"
#include "util/ArgParser.hpp"
#include "util/DotTool.hpp"
#include "util/fn.hpp"
#include "util/glyphs.hpp"
#include "util/terminal.hpp"
#include "util/Timer.hpp"
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


using namespace db;
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
        Timer timer;
        auto stmt = parser.parse();
        if (Options::Get().echo)
            std::cout << *stmt << std::endl;
        if (diag.num_errors() != num_errors) goto next;
        TIME_EXPR(sema(*stmt), "Semantic Analysis", timer);
        if (Options::Get().ast) stmt->dump(std::cout);
        if (Options::Get().astdot) {
            DotTool dot(diag);
            stmt->dot(dot.stream());
            dot.show("ast", is_stdin);
        }
        if (diag.num_errors() != num_errors) goto next;

        if (is<SelectStmt>(stmt)) {
            auto query_graph = TIME_EXPR(QueryGraph::Build(*stmt), "Construct the query graph", timer);
            if (Options::Get().graph) query_graph->dump(std::cout);
            if (Options::Get().graphdot) {
                DotTool dot(diag);
                query_graph->dot(dot.stream());
                dot.show("graph", is_stdin, "fdp");
            }

            std::unique_ptr<PlanEnumerator> pe = PlanEnumerator::Create(Options::Get().plan_enumerator);
            CostFunction cf([](CostFunction::Subproblem left, CostFunction::Subproblem right, int, const PlanTable &T) {
                return sum_wo_overflow(T[left].cost, T[right].cost, T[left].size, T[right].size);
            });
            Optimizer Opt(*pe.get(), cf);
            auto I = Backend::CreateInterpreter();
            auto optree = TIME_EXPR(Opt(*query_graph.get()), "Compute the query plan", timer);
            if (Options::Get().plan) optree->dump(std::cout);
            if (Options::Get().plandot) {
                DotTool dot(diag);
                optree->dot(dot.stream());
                dot.show("plan", is_stdin);
            }

            std::unique_ptr<Consumer> plan;
            auto print = [&](const Schema &S, const Tuple &t) { t.print(std::cout, S); std::cout << '\n'; };
            if (Options::Get().benchmark) {
                plan = std::make_unique<NoOpOperator>(std::cout);
            } else {
#if 0
                plan = std::make_unique<CallbackOperator>(print);
#else
                plan = std::make_unique<PrintOperator>(std::cout);
#endif
            }

            plan->add_child(optree.release());

            if (Options::Get().dryrun and Options::Get().wasm) {
                WasmModule wasm = TIME_EXPR(WasmPlatform::compile(*plan), "Compile to WebAssembly", timer);
                wasm.dump(std::cout);
            }

            if (not Options::Get().dryrun) {
                TIME_THIS("Execute query", timer);
                if (Options::Get().wasm) {
#if WITH_V8
                    auto V8 = Backend::CreateWasmV8();
                    V8->execute(*plan);
#elif WITH_SPIDERMONKEY
                    auto SM = Backend::CreateWasmSpiderMonkey();
                    SM->execute(*plan);
#else
                    std::cerr << "No WASM backend available.\n";
#endif
                } else {
                    I->execute(*plan);
                }
            }
        } else if (auto I = cast<InsertStmt>(stmt)) {
            auto &DB = C.get_database_in_use();
            auto &T = DB.get_table(I->table_name.text);
            auto &store = T.store();
            std::vector<const Attribute*> attrs;
            for (auto &attr : T) attrs.push_back(&attr);
            auto W = store.writer(attrs); // append values
            std::vector<const Type*> tuple_schema;
            for (auto attr : attrs) tuple_schema.push_back(attr->type);
            Tuple tup(tuple_schema);
            Tuple none;
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
                            get_tuple.emit_Cast(tuple_schema[i], v.second->type());
                            get_tuple.emit_St_Tup(0, i, tuple_schema[i]);
                            break;
                    }
                }
                Tuple *args[] = { &tup };
                get_tuple(args);
                W.set(0, store.num_rows());
                store.append();
                W(args);
            }
        } else if (auto S = cast<CreateTableStmt>(stmt)) {
            auto &DB = C.get_database_in_use();
            auto &T = DB.get_table(S->table_name.text);
            T.store(Store::Create(Options::Get().store, T));
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
                TIME_EXPR(R(file, S->path.text), "Read DSV file", timer);
            }
        }
next:
        num_errors = diag.num_errors();
        delete stmt;

        if (Options::Get().times) {
            std::cout << timer;
            std::cout.flush();
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
#define DB_KEYWORD(tt, name) #name,
#include "tables/Keywords.tbl"
#undef DB_KEYWORD
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
#define DB_KEYWORD(tt, name)\
        { #name, Replxx::Color::BROWN },
#include "tables/Keywords.tbl"
#undef DB_KEYWORD
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
#define DB_KEYWORD(tt, name) #name,
#include "tables/Keywords.tbl"
#undef DB_KEYWORD
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
    ADD(bool, Options::Get().show_help, false,              /* Type, Var, Init  */
        "-h", "--help",                                     /* Short, Long      */
        "prints this help message",                         /* Description      */
        [&](bool) { Options::Get().show_help = true; });    /* Callback         */
    /* Shell configuration */
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
    ADD(bool, Options::Get().wasm, false,                   /* Type, Var, Init  */
        nullptr, "--wasm",                                  /* Short, Long      */
        "show compiled WebAssembly",                        /* Description      */
        [&](bool) { Options::Get().wasm = true; });         /* Callback         */
    ADD(bool, Options::Get().benchmark, false,              /* Type, Var, Init  */
        nullptr, "--benchmark",                             /* Short, Long      */
        "run queries in benchmark mode",                    /* Description      */
        [&](bool) { Options::Get().benchmark = true; });    /* Callback         */

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
#undef ADD
    AP.parse_args(argc, argv);

    if (Options::Get().show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    if (Options::Get().list_stores) {
        std::cout << "List of available stores:";
        constexpr std::pair<const char*, const char*> stores[] = {
#define DB_STORE(NAME, DESCR) { #NAME, DESCR },
#include "tables/Store.tbl"
#undef DB_STORE
        };
        std::size_t max_len = 0;
        for (auto store : stores) max_len = std::max(max_len, strlen(store.first));
        for (auto store : stores)
            std::cout << "\n    " << std::setw(max_len) << std::left << store.first << "    -    " << store.second;
        std::cout << std::endl;
        std::exit(EXIT_SUCCESS);
    }

    if (Options::Get().list_plan_enumerators) {
        std::cout << "List of available plan enumerators:";
        constexpr std::pair<const char*, const char*> PE[] = {
#define DB_PLAN_ENUMERATOR(NAME, DESCR) { #NAME, DESCR },
#include "tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR
        };
        std::size_t max_len = 0;
        for (auto pe : PE) max_len = std::max(max_len, strlen(pe.first));
        for (auto pe : PE)
            std::cout << "\n    " << std::setw(max_len) << std::left << pe.first << "    -    " << pe.second;
        std::cout << std::endl;
        std::exit(EXIT_SUCCESS);
    }

    if (not Options::Get().quiet) {
        std::cout << "PID";
#if __linux || __APPLE__
        std::cout << ' ' << getpid();
#endif
        std::cout << std::endl;
    }

    /* Disable synchronisation between C and C++ I/O (e.g. stdin vs std::cin). */
    std::ios_base::sync_with_stdio(false);

    /* Create the diagnostics object. */
    Diagnostic diag(Options::Get().has_color, std::cout, std::cerr);

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
                insist(errno != EAGAIN);

                /* User sent EOF */
                if (cinput == nullptr) {
                    if (Options::Get().show_prompt)
                        std::cout << std::endl;
                    break;
                }
                insist(cinput);

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
