#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/mutable.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/ArgParser.hpp>
#include <random>
#include <unordered_map>
#include <unordered_set>
#ifdef __BMI2__
#include <x86intrin.h>
#endif


using Subproblem = m::SmallBitset;

struct args_t
{
    ///> whether to show a help message
    bool show_help;
};

void emit_CSG_queries(std::ostream &out, const m::QueryGraph &G, const m::AdjacencyMatrix &M);
void emit_query_slice(std::ostream &out, const m::QueryGraph &G, m::Subproblem slice);

void usage(std::ostream &out, const char *name)
{
    out << "A tool to generate queries for all connected subgraphs of a query's query graph.\n"
        << "USAGE:\n\t" << name << " <SCHEMA.sql> [<QUERY.sql>]"
        << std::endl;
}

int main(int argc, const char **argv)
{
    /*----- Parse command line arguments. ----------------------------------------------------------------------------*/
    m::ArgParser AP;
    args_t args;
#define ADD(TYPE, VAR, INIT, SHORT, LONG, DESCR, CALLBACK)\
    VAR = INIT;\
    {\
        AP.add<TYPE>(SHORT, LONG, DESCR, CALLBACK);\
    }
    /*----- Help message ---------------------------------------------------------------------------------------------*/
    ADD(bool, args.show_help, false,                                        /* Type, Var, Init  */
        "-h", "--help",                                                     /* Short, Long      */
        "prints this help message",                                         /* Description      */
        [&](bool) { args.show_help = true; });                              /* Callback         */
    /*----- Parse command line arguments. ----------------------------------------------------------------------------*/
    AP.parse_args(argc, argv);

    /*----- Help message. -----*/
    if (args.show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n" << AP;
        std::exit(EXIT_SUCCESS);
    }

    /*----- Validate command line arguments. -------------------------------------------------------------------------*/
    if (AP.args().size() == 0 or AP.args().size() > 2) {
        usage(std::cout, argv[0]);
        std::exit(EXIT_FAILURE);
    }

    /*----- Configure mutable. ---------------------------------------------------------------------------------------*/
    m::Options::Get().quiet = true;

    /*----- Load schema. ---------------------------------------------------------------------------------------------*/
    m::Diagnostic diag(false, std::cout, std::cerr);
    std::filesystem::path path_to_schema(AP.args()[0]);
    m::execute_file(diag, path_to_schema);
    m::Catalog &C = m::Catalog::Get();
    if (not C.has_database_in_use()) {
        std::cerr << "No database selected.\n";
        std::exit(EXIT_FAILURE);
    }

    /*----- Read input from stdin or file. ---------------------------------------------------------------------------*/
    const std::string input = [&AP]() -> std::string {
        if (AP.args().size() == 1) {
            return std::string(std::istreambuf_iterator<char>(std::cin), {});
        } else {
            std::filesystem::path path(AP.args()[1]);
            errno = 0;
            std::ifstream in(path);
            if (not in) {
                std::cerr << "Could not open file '" << path << '\'';
                const auto errsv = errno;
                if (errsv)
                    std::cerr << ": " << strerror(errsv);
                std::cerr << std::endl;
                std::exit(EXIT_FAILURE);
            }
            return std::string(std::istreambuf_iterator<char>(in), {});
        }
    }();

    /*----- Parse input. ---------------------------------------------------------------------------------------------*/
    const std::unique_ptr<m::ast::SelectStmt> select = [&diag, &input]() -> std::unique_ptr<m::ast::SelectStmt> {
        auto stmt = m::statement_from_string(diag, input);
        if (not m::is<m::ast::SelectStmt>(stmt.get())) {
            std::cerr << "Expected a SELECT statement.\n";
            std::exit(EXIT_FAILURE);
        }
        return std::unique_ptr<m::ast::SelectStmt>(m::as<m::ast::SelectStmt>(stmt.release()));
    }();

    auto G = m::QueryGraph::Build(*select);
    m::AdjacencyMatrix &M = G->adjacency_matrix();

    /*----- Emit the queries. ----------------------------------------------------------------------------------------*/
    const Subproblem All((1UL << G->num_sources()) - 1UL);
    auto emit = [&G](Subproblem S) { emit_query_slice(std::cout, *G, S); };
    M.for_each_CSG_undirected(All, emit);
}

void emit_query_slice(std::ostream &out, const m::QueryGraph &G, m::Subproblem slice)
{
    /*----- SELECT clause -----*/
    out << "SELECT COUNT(*)\n";

    /*----- FROM clause -----*/
    out << "FROM ";
    for (auto start = slice.begin(), it = start; it != slice.end(); ++it) {
        if (it != start) out << ", ";
        const auto source = G.sources()[*it].get();
        if (const m::BaseTable *T = cast<const m::BaseTable>(source)) {
            out << T->table().name;
            if (T->alias())
                out << " AS " << T->alias();
        } else {
            std::cerr << "ERROR: Nested queries are not supported." << std::endl;
            std::exit(EXIT_FAILURE);
        }
    }

    /*----- WHERE clause -----*/
    bool is_first_in_where = true;

    /* Joins */
    for (auto &J : G.joins()) {
        auto &sources = J->sources();
        for (auto source : sources) {
            if (not slice[source.get().id()]) // slice does not contain this source of the join
                goto skip_join;
        }
        if (is_first_in_where) {
            out << "\nWHERE ";
            is_first_in_where = false;
        } else {
            out << " AND ";
        }
        J->condition().to_sql(out);
skip_join:;
    }

    /* Conditions */
    {
        for (auto idx : slice) {
            const auto &source = *G.sources()[idx];
            const auto &selection = source.filter();
            if (not selection.empty()) {
                if (is_first_in_where) {
                    out << "\nWHERE ";
                    is_first_in_where = false;
                } else {
                    out << " AND ";
                }
                selection.to_sql(out);
            }
        }
    }

    out << ";\n\n";
}
