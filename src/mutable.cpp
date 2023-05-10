#include <mutable/mutable.hpp>

#include "backend/Interpreter.hpp"
#include "backend/StackMachine.hpp"
#include "backend/WebAssembly.hpp"
#include "IR/PartialPlanGenerator.hpp"
#include "lex/Lexer.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "parse/Sema.hpp"
#include <cerrno>
#include <fstream>
#include <mutable/catalog/DatabaseCommand.hpp>
#include <mutable/io/Reader.hpp>
#include <mutable/IR/Tuple.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/Diagnostic.hpp>


using namespace m;
using namespace m::ast;


bool m::init() { return streq(m::version::GIT_REV, m::version::get().GIT_REV); }

std::unique_ptr<Stmt> m::statement_from_string(Diagnostic &diag, const std::string &str)
{
    Catalog &C = Catalog::Get();

    std::istringstream in(str);
    Lexer lexer(diag, C.get_pool(), "-", in);
    Parser parser(lexer);
    auto stmt = M_TIME_EXPR(std::unique_ptr<Stmt>(parser.parse_Stmt()), "Parse the statement", C.timer());
    if (diag.num_errors() != 0)
        throw frontend_exception("syntactic error in statement");
    M_insist(diag.num_errors() == 0);

    Sema sema(diag);
    M_TIME_EXPR(sema(*stmt), "Semantic analysis", C.timer());
    if (diag.num_errors() != 0)
        throw frontend_exception("semantic error in statement");
    M_insist(diag.num_errors() == 0);

    return stmt;
}

std::unique_ptr<DatabaseCommand> m::command_from_string(Diagnostic &diag, const std::string &str)
{
    Catalog &C = Catalog::Get();

    std::istringstream in(str);
    Lexer lexer(diag, C.get_pool(), "-", in);
    Parser parser(lexer);
    auto stmt = M_TIME_EXPR(std::unique_ptr<Stmt>(parser.parse_Stmt()), "Parse the statement", C.timer());
    if (diag.num_errors() != 0)
        throw frontend_exception("syntactic error in statement");
    M_insist(diag.num_errors() == 0);

    Sema sema(diag);
    auto cmd = M_TIME_EXPR(sema.analyze(std::move(stmt)), "Semantic analysis", C.timer());
    if (diag.num_errors() != 0)
        throw frontend_exception("semantic error in statement");
    M_insist(diag.num_errors() == 0);

    return cmd;
}

void m::process_stream(std::istream &in, const char *filename, Diagnostic diag)
{
    Catalog &C = Catalog::Get();

    /*----- Process the input stream. --------------------------------------------------------------------------------*/
    ast::Lexer lexer(diag, C.get_pool(), filename, in);
    ast::Parser parser(lexer);
    ast::Sema sema(diag);

    while (parser.token()) {
        bool err = false;
        diag.clear();
        Timer &timer = C.timer();
        auto ast = parser.parse();
        err |= diag.num_errors() > 0;

        diag.clear();
        auto cmd = sema.analyze(std::move(ast));
        err |= diag.num_errors() > 0;

        M_insist(not err == bool(cmd), "when there are no errors, Sema must have returned a command");
        if (not err and cmd)
            cmd->execute(diag);

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

std::unique_ptr<Instruction> m::instruction_from_string(Diagnostic &diag, const std::string &str)
{
    Catalog &C = Catalog::Get();

    std::istringstream in(str);
    Lexer lexer(diag, C.get_pool(), "-", in);
    Parser parser(lexer);
    auto instruction = M_TIME_EXPR(parser.parse_Instruction(), "Parse the instruction", C.timer());
    if (diag.num_errors() != 0)
        throw frontend_exception("syntactic error in instruction");
    M_insist(diag.num_errors() == 0);

    return instruction;
}

void m::execute_statement(Diagnostic &diag, const ast::Stmt &stmt, const bool is_stdin)
{
    diag.clear();
    Catalog &C = Catalog::Get();
    auto timer = C.timer();

    if (is<const ast::SelectStmt>(stmt)) {
        auto query_graph = M_TIME_EXPR(QueryGraph::Build(stmt), "Construct the query graph", timer);
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
        Optimizer Opt(C.plan_enumerator(), C.cost_function());
        std::unique_ptr<Producer> optree;
        if (Options::Get().output_partial_plans_file) {
            auto res = M_TIME_EXPR(
                Opt.optimize_with_plantable<PlanTableLargeAndSparse>(*query_graph),
                "Compute the query plan",
                timer
            );
            optree = std::move(res.first);

            std::filesystem::path JSON_path(Options::Get().output_partial_plans_file);
            errno = 0;
            std::ofstream JSON_file(JSON_path);
            if (not JSON_file or errno) {
                const auto errsv = errno;
                if (errsv) {
                    diag.err() << "Failed to open output file for partial plans " << JSON_path << ": "
                               << strerror(errsv) << std::endl;
                } else {
                    diag.err() << "Failed to open output file for partial plans " << JSON_path << std::endl;
                }
            } else {
                auto for_each = [&res](PartialPlanGenerator::callback_type callback) {
                    PartialPlanGenerator{}.for_each_complete_partial_plan(res.second, callback);
                };
                PartialPlanGenerator{}.write_partial_plans_JSON(JSON_file, *query_graph, res.second, for_each);
            }
        } else {
            optree = M_TIME_EXPR(Opt(*query_graph), "Compute the query plan", timer);
        }
        M_insist(bool(optree), "optree must have been computed");
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

/* TODO implement as command line argument of plugin
    if (Options::Get().dryrun and streq("WasmV8", C.default_backend_name())) {
        Backend &backend = C.backend();
        auto &engine = as<WasmBackend>(backend).engine();
        Module::Init(); // fresh module
        M_TIME_EXPR(engine.compile(*plan), "Compile to WebAssembly", timer);
        Module::Get().dump(std::cout);
        Module::Dispose();
    }
*/

        if (not Options::Get().dryrun) {
            static thread_local std::unique_ptr<Backend> backend;
            if (not backend)
                backend = M_TIME_EXPR(C.create_backend(), "Create backend", timer);
            M_TIME_EXPR(backend->execute(*plan), "Execute query", timer);
        }
    } else if (auto I = cast<const ast::InsertStmt>(&stmt)) {
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
                    case ast::InsertStmt::I_Null:
                        get_tuple.emit_St_Tup_Null(0, i);
                        break;

                    case ast::InsertStmt::I_Default:
                        /* nothing to be done, Tuples are initialized to default values */
                        break;

                    case ast::InsertStmt::I_Expr:
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
    } else if (auto S = cast<const ast::CreateDatabaseStmt>(&stmt)) {
        C.add_database(S->database_name.text);
    } else if (auto S = cast<const ast::UseDatabaseStmt>(&stmt)) {
        auto &DB = C.get_database(S->database_name.text);
        C.set_database_in_use(DB);
    } else if (auto S = cast<const ast::CreateTableStmt>(&stmt)) {
        auto &DB = C.get_database_in_use();
        auto &T = DB.add_table(S->table_name.text);

        for (auto &attr : S->attributes) {
            const PrimitiveType *ty = cast<const PrimitiveType>(attr->type);

            T.push_back(attr->name.text, ty->as_vectorial());
            for (auto &c : attr->constraints) {
                visit(overloaded {
                    [&](const PrimaryKeyConstraint&) {
                        T.add_primary_key(attr->name.text);
                    },
                    [&](const UniqueConstraint&) {
                        T.at(attr->name.text).unique = true;
                    },
                    [&](const NotNullConstraint&) {
                        T.at(attr->name.text).not_nullable = true;
                    },
                    [&](const ReferenceConstraint &ref) {
                        auto &ref_table = DB.get_table(ref.table_name.text);
                        auto &ref_attr = ref_table.at(ref.attr_name.text);
                        T.at(attr->name.text).reference = &ref_attr;
                    },
                    [](auto&&) { M_unreachable("constraint not implemented"); },
                }, *c, tag<ConstASTConstraintVisitor>{});
            }
        }

        T.layout(C.data_layout());
        T.store(C.create_store(T));
    } else if (auto S = cast<const ast::DSVImportStmt>(&stmt)) {
        auto &DB = C.get_database_in_use();
        auto &T = DB.get_table(S->table_name.text);

        DSVReader::Config cfg;
        if (S->rows) cfg.num_rows = strtol(S->rows.text, nullptr, 10);
        if (S->delimiter) cfg.delimiter = unescape(S->delimiter.text)[1];
        if (S->escape) cfg.escape = unescape(S->escape.text)[1];
        if (S->quote) cfg.quote = unescape(S->quote.text)[1];
        cfg.has_header = S->has_header;
        cfg.skip_header = S->skip_header;

        try {
            DSVReader R(T, std::move(cfg), diag);

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
        } catch (m::invalid_argument e) {
            diag.err() << "Error reading DSV file: " << e.what() << "\n";
        }
    }

    if (Options::Get().times) {
        using namespace std::chrono;
        for (const auto &M : timer) {
            if (M.is_finished())
                std::cout << M.name << ": " << duration_cast<microseconds>(M.duration()).count() / 1e3 << '\n';
        }
        std::cout.flush();
        timer.clear();
    }

    std::cout.flush();
    std::cerr.flush();
}

void m::execute_instruction(Diagnostic &diag, const Instruction &instruction)
{
    diag.clear();
    Catalog &C = Catalog::Get();

    try {
        auto I = C.create_instruction(instruction.name, instruction.args);
        I->execute(diag);
    } catch (const std::exception &e) {
        diag.e(instruction.tok.pos) << "Instruction " << instruction.name << " does not exist.\n";
    }
}

void m::execute_query(Diagnostic&, const SelectStmt &stmt, std::unique_ptr<Consumer> consumer)
{
    Catalog &C = Catalog::Get();
    auto query_graph = M_TIME_EXPR(QueryGraph::Build(stmt), "Construct the query graph", C.timer());

    Optimizer Opt(C.plan_enumerator(), C.cost_function());
    auto optree = M_TIME_EXPR(Opt(*query_graph), "Compute the query plan", C.timer());

    consumer->add_child(optree.release());

    static thread_local std::unique_ptr<Backend> backend;
    if (not backend)
        backend = M_TIME_EXPR(C.create_backend(), "Create backend", C.timer());
    M_TIME_EXPR(backend->execute(*consumer), "Execute the query", C.timer());
}

void m::load_from_CSV(Diagnostic &diag, Table &table, const std::filesystem::path &path, std::size_t num_rows,
                      bool has_header, bool skip_header)
{
    diag.clear();
    auto cfg = DSVReader::Config::CSV();
    cfg.num_rows = num_rows;
    cfg.has_header = has_header;
    cfg.skip_header = skip_header;
    DSVReader R(table, std::move(cfg), diag);

    errno = 0;
    std::ifstream file(path);
    if (not file) {
        diag.e(Position(path.c_str())) << "Could not open file '" << path << '\'';
        if (errno)
            diag.err() << ": " << strerror(errno);
        diag.err() << std::endl;
    } else {
        R(file, path.c_str()); // read the file
    }

    if (diag.num_errors() != 0)
        throw runtime_error("error while reading CSV file");
}

void m::execute_file(Diagnostic &diag, const std::filesystem::path &path)
{
    diag.clear();
    auto &C = Catalog::Get();

    errno = 0;
    std::ifstream in(path);
    if (not in) {
        auto errsv = errno;
        std::cerr << "Could not open '" << path << "'";
        if (errno)
            std::cerr << ": " << std::strerror(errsv);
        std::cerr << std::endl;
        exit(EXIT_FAILURE);
    }

    Lexer lexer(diag, C.get_pool(), path.c_str(), in);
    Parser parser(lexer);
    Sema sema(diag);

    while (parser.token()) {
        std::unique_ptr<Command> command(parser.parse());
        if (diag.num_errors()) return;
        if (auto inst = cast<Instruction>(command)) {
            execute_instruction(diag, *inst);
        } else {
            auto stmt = as<Stmt>(std::move(command));
            sema(*stmt);
            if (diag.num_errors()) return;
            execute_statement(diag, *stmt);
        }
    }
}

m::StoreWriter::StoreWriter(Store &store)
    : store_(store)
{
    for (auto &attr : store.table())
        S.add({attr.table.name, attr.name}, attr.type);
}

m::StoreWriter::~StoreWriter() { }

void m::StoreWriter::append(const Tuple &tup) const
{
    store_.append();
    if (layout_ != &store_.table().layout()) {
        layout_ = &store_.table().layout();
        writer_ = std::make_unique<m::StackMachine>(m::Interpreter::compile_store(S, store_.memory().addr(), *layout_,
                                                                                  S, store_.num_rows() - 1));
    }

    Tuple *args[] = { const_cast<Tuple*>(&tup) };
    (*writer_)(args);
}
