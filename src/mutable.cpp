#include "mutable/mutable.hpp"

#include "globals.hpp"
#include "util/Diagnostic.hpp"
#include "lex/Lexer.hpp"
#include "parse/Parser.hpp"
#include "parse/Sema.hpp"
#include "IR/Optimizer.hpp"
#include "IR/Tuple.hpp"
#include "backend/WebAssembly.hpp"
#include "backend/StackMachine.hpp"
#include "backend/Interpreter.hpp"
#include "io/Reader.hpp"
#include <fstream>


using namespace m;


std::unique_ptr<Stmt> m::query_from_string(const std::string &str)
{
    Catalog &C = Catalog::Get();
    std::ostringstream out, err;
    Diagnostic diag(false, out, err);
    insist(diag.num_errors() == 0);
    insist(err.str().empty());

    std::istringstream in(str);
    Lexer lexer(diag, C.get_pool(), "API", in);
    Parser parser(lexer);
    auto stmt = parser.parse();
    if (diag.num_errors() != 0) {
        delete stmt;
        throw frontend_exception(err.str());
    }
    insist(diag.num_errors() == 0);
    insist(err.str().empty());

    Sema sema(diag);
    sema(*stmt);
    if (diag.num_errors() != 0) {
        delete stmt;
        throw frontend_exception(err.str());
    }
    insist(diag.num_errors() == 0);
    insist(err.str().empty());

    return std::unique_ptr<Stmt>(stmt);
}

void m::execute_query(const Stmt &stmt)
{
    Catalog &C = Catalog::Get();
    std::ostringstream out, err;
    Diagnostic diag(false, out, err);
    insist(diag.num_errors() == 0);
    insist(err.str().empty());

    if (is<const SelectStmt>(stmt)) {
        auto query_graph = QueryGraph::Build(stmt);

        std::unique_ptr<PlanEnumerator> pe = PlanEnumerator::Create("DPccp");
        CostFunction cf([](CostFunction::Subproblem left, CostFunction::Subproblem right, int, const PlanTable &T) {
            return sum_wo_overflow(T[left].cost, T[right].cost, T[left].size, T[right].size);
        });
        Optimizer Opt(*pe, cf);
        auto optree = Opt(*query_graph);

        std::unique_ptr<Consumer> plan;
        auto print = [&](const Schema &S, const Tuple &t) { t.print(std::cout, S); std::cout << '\n'; };
#if 0
        plan = std::make_unique<CallbackOperator>(print);
#else
        plan = std::make_unique<PrintOperator>(std::cout);
#endif
        plan->add_child(optree.release());

        auto backend = Backend::Create("Interpreter");
        backend->execute(*plan);
    } else if (auto I = cast<const InsertStmt>(&stmt)) {
        auto &DB = C.get_database_in_use();
        auto &T = DB.get_table(I->table_name.text);
        auto &store = T.store();

        /* Compute table schema. */
        Schema S;
        for (auto &attr : T) S.add({T.name, attr.name}, attr.type);

        /* Compile stack machine. */
        auto W = Interpreter::compile_store(S, store.linearization());
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
            store.append();
            W(args);
        }
    } else if (auto S = cast<const CreateTableStmt>(&stmt)) {
        auto &DB = C.get_database_in_use();
        auto &T = DB.get_table(S->table_name.text);
        T.store(C.create_store(T));
    } else if (auto S = cast<const DSVImportStmt>(&stmt)) {
        auto &DB = C.get_database_in_use();
        auto &T = DB.get_table(S->table_name.text);

        DSVReader R(T, diag);
        if (S->rows) R.num_rows = strtol(S->rows.text, nullptr, 10);
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

        if (diag.num_errors() != 0)
            throw backend_exception(err.str());
    }

    std::cout.flush();
    std::cerr.flush();
}
