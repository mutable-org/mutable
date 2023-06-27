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
#include <mutable/util/fn.hpp>
#include <random>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#ifdef __BMI2__
#include <x86intrin.h>
#endif


using Subproblem = m::SmallBitset;


/** A distribution of values in range *[0; 1]* that are skewed towards 0.  The `alpha` parameter affects the factor of
 * skewed-ness, with `alpha` = 1 causing a uniform distribution, `alpha` < 1 causing a skew towards 1, and `alpha` > 1
 * causing a skew towards 0. */
template<typename RealType>
struct skewed_distribution
{
    using real_type = RealType;
    static_assert(std::is_floating_point_v<real_type>, "RealType must be a floating-point type");

    private:
    real_type alpha_;
    std::uniform_real_distribution<real_type> d_;

    public:
    skewed_distribution(real_type alpha)
        : alpha_(alpha), d_(0, 1)
    {
        if (alpha_ <= 0)
            throw std::invalid_argument("alpha must be positive");
    }

    real_type alpha() const { return alpha_; }

    template<typename Generator>
    real_type operator()(Generator &&g) { return std::pow(d_(std::forward<Generator>(g)), alpha_); }
};

struct
{
    ///> whether to show a help message
    bool show_help;
    ///> the seed for the PRNG
    unsigned seed;
    ///> whether selectivities are correlated
    bool correlated_selectivities;
    ///> minimum cardinality of relations and intermediate results
    std::size_t min_cardinality;
    ///> maximum cardinality of relations and intermediate results
    std::size_t max_cardinality;
    ///> alpha for skewed distribution
    double alpha;
} args;

using table_type = std::unordered_map<Subproblem, double, m::SubproblemHash>;

template<typename Generator>
void generate_correlated_cardinalities(table_type &table, const m::QueryGraph &G, Generator &&g);

template<typename Generator>
void generate_uncorrelated_cardinalities(table_type &table, const m::QueryGraph &G, Generator &&g);

void emit_cardinalities(std::ostream &out, const m::QueryGraph &G, const table_type &table);

void usage(std::ostream &out, const char *name)
{
    out << "A tool to generate fake cardinalities for queries.\n"
        << "USAGE:\n\t" << name << " <SCHEMA.sql> [<QUERY.sql>]"
        << std::endl;
}

int main(int argc, const char **argv)
{
    m::Catalog &C = m::Catalog::Get();

    /*----- Parse command line arguments. ----------------------------------------------------------------------------*/
    m::ArgParser &AP = C.arg_parser();
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
    /*----- Seed -----------------------------------------------------------------------------------------------------*/
    ADD(unsigned, args.seed, 42,                                            /* Type, Var, Init  */
        nullptr, "--seed",                                                  /* Short, Long      */
        "the seed for the PRNG",                                            /* Description      */
        [&](unsigned s) { args.seed = s; });                                /* Callback         */
    /*----- Correlated selectivity -----------------------------------------------------------------------------------*/
    ADD(bool, args.correlated_selectivities, true,                          /* Type, Var, Init  */
        nullptr, "--uncorrelated",                                          /* Short, Long      */
        "make join selectivities uncorrelated",                             /* Description      */
        [&](bool) { args.correlated_selectivities = false; });              /* Callback         */
    /*----- Cardinalities --------------------------------------------------------------------------------------------*/
    ADD(std::size_t, args.min_cardinality, 10,                              /* Type, Var, Init  */
        nullptr, "--min",                                                   /* Short, Long      */
        "the minimum cardinality of base tables",                           /* Description      */
        [&](std::size_t card) { args.min_cardinality = card; });            /* Callback         */
    ADD(std::size_t, args.max_cardinality, 1e4,                             /* Type, Var, Init  */
        nullptr, "--max",                                                   /* Short, Long      */
        "the maximum cardinality of base tables",                           /* Description      */
        [&](std::size_t card) { args.max_cardinality = card; });            /* Callback         */
    ADD(int, args.alpha, 3,                                                 /* Type, Var, Init  */
        nullptr, "--alpha",                                                 /* Short, Long      */
        "skewedness of cardinalities and selectivities, from ℤ",            /* Description      */
        [&](int alpha) {                                                    /* Callback         */
            if (alpha == 0)
                args.alpha = 1;
            else if (alpha > 0)
                args.alpha = alpha;
            else
                args.alpha = 1./-alpha;
        });
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
        if (not m::is<m::ast::SelectStmt>(stmt)) {
            std::cerr << "Expected a SELECT statement.\n";
            std::exit(EXIT_FAILURE);
        }
        return m::as<m::ast::SelectStmt>(std::move(stmt));
    }();

    /*----- Prepare graph, table, and generator. ---------------------------------------------------------------------*/
    auto G = m::QueryGraph::Build(*select);
    table_type table(G->num_sources() * G->num_sources());
    std::mt19937_64 g(args.seed ^ 0x1d9a07cfbc6e4464UL);

    /*----- Fill table with cardinalities for base relations. --------------------------------------------------------*/
    const double delta = args.max_cardinality - args.min_cardinality;
    skewed_distribution<double> dist(args.alpha);
    for (unsigned i = 0; i != G->num_sources(); ++i) {
        const std::size_t cardinality = args.min_cardinality + delta * dist(g);
        M_insist(args.min_cardinality <= cardinality);
        M_insist(cardinality <= args.max_cardinality);
        table[Subproblem(1UL << i)] = cardinality;
    }

    /*----- Generate cardinalities. ----------------------------------------------------------------------------------*/
    if (args.correlated_selectivities)
        generate_correlated_cardinalities(table, *G, g);
    else
        generate_uncorrelated_cardinalities(table, *G, g);

    /*----- Emit the table. ------------------------------------------------------------------------------------------*/
    emit_cardinalities(std::cout, *G, table);

    m::Catalog::Destroy();
}

template<typename Generator>
void generate_correlated_cardinalities(table_type &table, const m::QueryGraph &G, Generator &&g)
{
    const Subproblem All((1UL << G.num_sources()) - 1UL);
    const m::AdjacencyMatrix &M = G.adjacency_matrix();
    std::unordered_map<Subproblem, double, m::SubproblemHash> max_cardinalities;

    auto cardinality = [&](const Subproblem S) -> double {
        auto table_it = table.find(S);
        if (table_it != table.end()) [[likely]] { // we already know the cardinality of S
            M_insist(table_it->second <= args.max_cardinality * args.max_cardinality);
            return table_it->second;
        } else {
            const auto it = max_cardinalities.find(S);
            M_insist(it != max_cardinalities.end());
            M_insist(it->second > args.min_cardinality);
            skewed_distribution<double> selectivity_dist(args.alpha);
            const double max_cardinality = std::min<double>(it->second, args.max_cardinality * args.max_cardinality);
            M_insist(max_cardinality >= args.min_cardinality);
            const double c = args.min_cardinality + (max_cardinality - args.min_cardinality) * selectivity_dist(g);
            M_insist(c != std::numeric_limits<double>::infinity());
            M_insist(c <= double(args.max_cardinality) * args.max_cardinality);
            max_cardinalities.erase(it);
            table_it = table.emplace_hint(table_it, S, c);
            M_insist(table_it->second == c);
            return c;
        }
    };

    auto update = [&](const Subproblem S1, const Subproblem S2) -> void {
        const double cardinality_S1 = cardinality(S1);
        const double cardinality_S2 = cardinality(S2);

        if (auto it = max_cardinalities.find(S1|S2); it == max_cardinalities.end())
            max_cardinalities.emplace_hint(it, S1|S2, cardinality_S1 * cardinality_S2);
        else
            it->second = std::min(it->second, cardinality_S1 * cardinality_S2);
    };

    M.for_each_CSG_pair_undirected(All, update);
    M_insist(max_cardinalities.size() == 1);
    table[All] = cardinality(All);
}

template<typename Generator>
void generate_uncorrelated_cardinalities(table_type &table, const m::QueryGraph &G, Generator &&g)
{
    const Subproblem All((1UL << G.num_sources()) - 1UL);
    const m::AdjacencyMatrix &M = G.adjacency_matrix();
    skewed_distribution<double> selectivity_dist(args.alpha);
    // std::uniform_real_distribution<double> selectivity_dist(0, 1);
    const std::size_t delta = args.max_cardinality - args.min_cardinality;

    /* Roll result cardinality. */
    const std::size_t cardinality_of_result = args.min_cardinality + delta * selectivity_dist(g);

    /*----- Compute combined selectivity. -----*/
    const double combined_selectivity = [&]() -> double {
        double cardinality_of_Cartesian_product = 1;
        for (auto it = All.begin(); it != All.end(); ++it)
            cardinality_of_Cartesian_product *= table[it.as_set()];
        const double x = cardinality_of_result / cardinality_of_Cartesian_product;
        return x;
    }();

    const double avg_selectivity = std::pow(combined_selectivity, 1. / G.num_joins());

    /*----- Generate selectivities for joins. -----*/
    std::vector<double> selectivities(G.num_joins());
    double remaining_selectivity = combined_selectivity;
    for (std::size_t j = 1; j != G.num_joins(); ++j) {
        auto &J = G.joins()[j]->sources();

        /*----- Create a fresh PRNG from the sources that are being joined. -----*/
        std::size_t seed = 0;
        for (auto s : J)
            seed = (seed * 526122883134911UL) ^ m::StrHash{}(s.get().name());
        std::mt19937_64 local_g(seed);

        /*----- Compute the selectivity of this join. -----*/
        double cartesian = 1;
        for (auto s : J)
            cartesian *= table[Subproblem(1UL << s.get().id())];
        const double min_selectivity = std::max(args.min_cardinality / cartesian, remaining_selectivity);
        if (min_selectivity < avg_selectivity) {
            /*----- Draw selectivity between min and average. -----*/
            M_insist(min_selectivity <= avg_selectivity);
            selectivities[j] = avg_selectivity - (avg_selectivity - min_selectivity) * selectivity_dist(local_g);
        } else {
            /*----- Draw selectivity between average and 1. -----*/
            selectivities[j] = avg_selectivity + (1. - avg_selectivity) * selectivity_dist(local_g);
        }
        remaining_selectivity /= selectivities[j];
    }
    selectivities[0] = remaining_selectivity;


    auto update = [&G, &M, &selectivities, &table](const Subproblem left, const Subproblem right) {
        M_insist(M.is_connected(left, right));

        /* Compute total selectivity as product of selectivities of all edges between `left` and `right`. */
        double total_selectivity = 1.;
        for (std::size_t j = 0; j != G.num_joins(); ++j) {
            auto &sources = G.joins()[j]->sources();
            if (sources.size() != 2)
                throw std::invalid_argument("unsupported join");
            /* Check whether this join connects `left` and `right`. */
            if ((left[sources[0].get().id()] and right[sources[1].get().id()]) or
                (left[sources[1].get().id()] and right[sources[0].get().id()]))
            {
                /* This join connects `left` and `right`. */
                total_selectivity *= selectivities[j];
            }
        }

        /* Compute cardinality of CSG. */
        const double real_cardinality = table[left] * table[right] * total_selectivity;
        std::size_t clamped_cardinality;
        if (real_cardinality > double(std::numeric_limits<std::size_t>::max()))
            clamped_cardinality = std::numeric_limits<std::size_t>::max();
        else
            clamped_cardinality = real_cardinality;
        table[left | right] = std::max<std::size_t>(1UL, clamped_cardinality);
    };

    M.for_each_CSG_pair_undirected(All, update);
}

void emit_cardinalities(std::ostream &out, const m::QueryGraph &G, const table_type &table)
{
    m::Catalog &C = m::Catalog::Get();
    m::Database &DB = C.get_database_in_use();

    out << "{\n    \"" << DB.name << "\": [\n";
    bool first = true;
    const Subproblem All((1UL << G.num_sources()) - 1UL);

    /*----- Print singletons aka base relations. -----*/
    for (auto it = All.begin(); it != All.end(); ++it) {
        if (first) first = false;
        else       out << ",\n";
        out << "        { \"relations\": [\"" << G.sources()[*it]->name() << "\"], "
            << "\"size\": " << table.at(it.as_set()) << "}";
    }

    /*----- Print non-singletons. -----*/
    for (auto entry : table) {
        const Subproblem S = entry.first;
        if (S.singleton()) continue; // skip singleton
        const std::size_t size = entry.second;

        /*----- Emit relations. -----*/
        if (first) first = false;
        else       out << ",\n";
        out << "        { \"relations\": [";
        for (auto it = S.begin(); it != S.end(); ++it) {
            if (it != S.begin()) out << ", ";
            auto &DS = G.sources()[*it];
            out << '"' << DS->name() << '"';
        }
        /*----- Emit size. -----*/
        out << "], \"size\": " << size << "}";
    }
    out << "\n    ]\n}\n";
}
