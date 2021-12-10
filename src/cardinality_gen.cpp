#include "globals.hpp"
#include "util/ArgParser.hpp"
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutable/mutable.hpp>
#include <random>
#include <unordered_set>
#include <x86intrin.h>


using Subproblem = m::SmallBitset;

struct args_t
{
    ///> whether to show a help message
    bool show_help;
    ///> the seed for the PRNG
    unsigned seed;
    ///> minimum cardinality of relations and intermediate results
    std::size_t min_cardinality;
    ///> maximum cardinality of relations and intermediate results
    std::size_t max_cardinality;
};

struct entry_t
{
    std::size_t max_cardinality = -1UL;
    entry_t() { }
};

std::unique_ptr<entry_t[]>
generate_cardinalities_for_query(const m::QueryGraph &G, const m::AdjacencyMatrix &M, const args_t &args);

void emit_cardinalities(std::ostream &out, const m::QueryGraph &G, const entry_t *table);

void usage(std::ostream &out, const char *name)
{
    out << "A tool to generate fake cardinalities for queries.\n"
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
        std::function<void(TYPE)> callback = CALLBACK;\
        AP.add(SHORT, LONG, DESCR, callback);\
    }
    /*----- Help message ---------------------------------------------------------------------------------------------*/
    ADD(bool, args.show_help, false,                                        /* Type, Var, Init  */
        "-h", "--help",                                                     /* Short, Long      */
        "prints this help message",                                         /* Description      */
        [&](bool) { args.show_help = true; });                              /* Callback         */
    /*----- Seed -----------------------------------------------------------------------------------------------------*/
    ADD(unsigned, args.seed, 42,                                            /* Type, Var, Init  */
        nullptr, "--seed",                                                  /* Short, Long      */
        "the seed for the PRNG",                                            /* Description      */
        [&](unsigned s) { args.seed = s; });                                /* Callback         */
    /*----- Cardinalities --------------------------------------------------------------------------------------------*/
    ADD(std::size_t, args.min_cardinality, 1,                               /* Type, Var, Init  */
        nullptr, "--min",                                                   /* Short, Long      */
        "the minimum cardinality of base tables",                           /* Description      */
        [&](std::size_t card) { args.min_cardinality = card; });            /* Callback         */
    ADD(std::size_t, args.max_cardinality, 1e6,                             /* Type, Var, Init  */
        nullptr, "--max",                                                   /* Short, Long      */
        "the maximum cardinality of base tables",                           /* Description      */
        [&](std::size_t card) { args.max_cardinality = card; });            /* Callback         */
    /*----- Parse command line arguments. ----------------------------------------------------------------------------*/
    AP.parse_args(argc, argv);

    /*----- Help message. -----*/
    if (args.show_help) {
        usage(std::cout, argv[0]);
        std::cout << "WHERE\n";
        AP.print_args(stdout);
        std::exit(EXIT_SUCCESS);
    }

    /*----- Validate command line arguments. -------------------------------------------------------------------------*/
    if (AP.args().size() == 0 or AP.args().size() > 2) {
        usage(std::cout, argv[0]);
        std::exit(EXIT_FAILURE);
    }

    /*----- Configure mutable. ---------------------------------------------------------------------------------------*/
    Options::Get().quiet = true;

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
    const std::unique_ptr<m::SelectStmt> select = [&diag, &input]() -> std::unique_ptr<m::SelectStmt> {
        auto stmt = m::statement_from_string(diag, input);
        if (not is<m::SelectStmt>(stmt.get())) {
            std::cerr << "Expected a SELECT statement.\n";
            std::exit(EXIT_FAILURE);
        }
        return std::unique_ptr<m::SelectStmt>(as<m::SelectStmt>(stmt.release()));
    }();

    auto G = m::QueryGraph::Build(*select);
    m::AdjacencyMatrix M(*G);

    /*----- Generate cardinalities. ----------------------------------------------------------------------------------*/
    std::unique_ptr<entry_t[]> table = generate_cardinalities_for_query(*G, M, args);

    /*----- Emit the table. ------------------------------------------------------------------------------------------*/
    emit_cardinalities(std::cout, *G, table.get());
}

std::unique_ptr<entry_t[]>
generate_cardinalities_for_query(const m::QueryGraph &G, const m::AdjacencyMatrix &M, const args_t &args)
{
    const std::size_t num_relations = G.sources().size();

    std::mt19937_64 g(args.seed);
    std::gamma_distribution<double> cardinality_dist(.5, 1.);

    auto table = std::make_unique<entry_t[]>(1UL << num_relations);

    /*----- Fill table with cardinalities for base relations. --------------------------------------------------------*/
    {
        for (unsigned i = 0; i != num_relations; ++i) {
            auto &e = table[1UL << i];

            e.max_cardinality =
                args.max_cardinality - (args.max_cardinality - args.min_cardinality) / (1. + cardinality_dist(g));
        }
    }

    auto update = [&table, &g](const Subproblem S1, const Subproblem S2) -> void {
        auto &left   = table[uint64_t(S1)];
        auto &right  = table[uint64_t(S2)];
        auto &joined = table[uint64_t(S1 | S2)];

        std::gamma_distribution<double> selectivity_dist(.15, 1.);

        /*----- Compute selectivity ranges. -----*/
        constexpr double MAX_SELECTIVITY = .8;
        constexpr std::size_t MAX_GROWTH_FACTOR = 100;
        const double max_selectivity = std::min<double>(
            MAX_SELECTIVITY,
            MAX_GROWTH_FACTOR * double(std::max(left.max_cardinality, right.max_cardinality)) /
                (left.max_cardinality * right.max_cardinality)
        );
        const double selectivity_factor = 1. - 1. / (1. + selectivity_dist(g));
        const double selectivity = max_selectivity * selectivity_factor;

        /* Make sure relations are never empty. */
        joined.max_cardinality = std::max<std::size_t>(1UL, selectivity * left.max_cardinality * right.max_cardinality);
    };

    /*----- Run DPccp algorithm to enumerate all connected complement pairs in ascending order. ----------------------*/
#ifndef NDEBUG
    using duplicate_t = std::pair<uint64_t, uint64_t>;
    struct duplicate_hash
    {
        uint64_t operator()(const duplicate_t &d) const {
            return std::hash<uint64_t>{}(d.first) ^ std::hash<uint64_t>{}(d.second);
        }
    };
    std::unordered_set<duplicate_t, duplicate_hash> duplicate_check;
#endif

    std::deque<std::pair<Subproblem, Subproblem>> cmp_Q;
    auto enumerate_complements = [&](const Subproblem S1) -> void {
        const Subproblem X = S1 | Subproblem(uint64_t(S1) - 1UL); // mask all nodes in S1 and smaller than its smallest
        const Subproblem N = M.neighbors(S1) - X;
        if (N.empty()) return;

        cmp_Q.clear();
        for (auto it = N.begin(); it != N.end(); ++it) {
            const Subproblem vi = it.as_set();
            cmp_Q.emplace_back(vi, X | (vi.singleton_to_lo_mask() & N));

            while (not cmp_Q.empty()) {
                auto [S2, X2] = cmp_Q.front();
                cmp_Q.pop_front();

                M_insist((S1 & S2).empty());
                M_insist(M.is_connected(S1, S2));
#ifndef NDEBUG
                M_insist(duplicate_check.emplace(S1, S2).second, "duplicate");
#endif

                update(S1, S2);

                const Subproblem N2 = M.neighbors(S2) - X2;
                for (Subproblem n = m::least_subset(N2); bool(n); n = m::next_subset(n, N2))
                    cmp_Q.emplace_back(S2 | n, X2 | N);
            }
        }
    };

    std::deque<std::pair<Subproblem, Subproblem>> Q;
    for (unsigned i = num_relations; i --> 0;) {
        Subproblem I(1UL << i);
        Q.emplace_back(I, I.singleton_to_lo_mask());

        while (not Q.empty()) {
            auto [S, X] = Q.front();
            Q.pop_front();

            enumerate_complements(S);

            const Subproblem N = M.neighbors(S) - X;
            for (Subproblem n = m::least_subset(N); bool(n); n = m::next_subset(n, N))
                Q.emplace_back(S | n, X | N);
        }
    }

    return table;
}

void emit_cardinalities(std::ostream &out, const m::QueryGraph &G, const entry_t *table)
{
    m::Catalog &C = m::Catalog::Get();
    m::Database &DB = C.get_database_in_use();
    const std::size_t num_relations = G.sources().size();
    const Subproblem All((1UL << num_relations) - 1UL);

    out << "{\n    \"" << DB.name << "\": [\n";
    bool first = true;
    for (auto I = m::least_subset(All), S = I; bool(S); S = m::next_subset(S, All)) {
        /*----- Get the size and check whether subproblem is feasible. -----*/
        const std::size_t size = table[uint64_t(S)].max_cardinality;
        if (size == -1UL)
            continue; // not a feasible subproblem

        /*----- Emit relations. -----*/
        if (first) first = false;
        else       out << ",\n";
        out << "        { \"relations\": [";
        for (auto it = S.begin(); it != S.end(); ++it) {
            if (it != S.begin()) out << ", ";
            auto DS = G.sources()[*it];
            const char *relation_name = DS->alias() ? : as<m::BaseTable>(DS)->table().name;
            out << '"' << relation_name << '"';
        }
        /*----- Emit size. -----*/
        out << "], \"size\": " << size << "}";
    }
    out << "\n    ]\n}\n";
}
