#include "mutable/IR/PlanEnumerator.hpp"

#include "util/ADT.hpp"
#include <algorithm>
#include <globals.hpp>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/AIPlanner.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/list_allocator.hpp>
#include <mutable/util/malloc_allocator.hpp>
#include <mutable/util/Timer.hpp>
#include <queue>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <x86intrin.h>


using namespace m;


const std::unordered_map<std::string, PlanEnumerator::kind_t> PlanEnumerator::STR_TO_KIND = {
#define DB_PLAN_ENUMERATOR(NAME, _) { #NAME,  PlanEnumerator::PE_ ## NAME },
#include "mutable/tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR
};

std::unique_ptr<PlanEnumerator> PlanEnumerator::Create(PlanEnumerator::kind_t kind) {
    switch(kind) {
#define DB_PLAN_ENUMERATOR(NAME, _) case PE_ ## NAME: return Create ## NAME();
#include "mutable/tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR
    }
}

/*======================================================================================================================
 * DPsize
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming. */
struct DPsize final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const override;
};

void DPsize::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    /* Process all subplans of size greater than one. */
    for (std::size_t s = 2; s <= n; ++s) {
        for (std::size_t s1 = 1; s1 < s; ++s1) {
            std::size_t s2 = s - s1;
            /* Check for all combinations of subsets if they are valid joins and if so, forward the combination to the
             * plan table. */
            for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                if (not PT.has_plan(*S1)) continue; // subproblem S1 not connected -> skip
                for (auto S2 = GospersHack::enumerate_all(s2, n); S2; ++S2) { // enumerate all subsets of size `s - s1`
                    if (not PT.has_plan(*S2)) continue; // subproblem S2 not connected -> skip
                    if (*S1 & *S2) continue; // subproblems not disjoint -> skip
                    if (not M.is_connected(*S1, *S2)) continue; // subproblems not connected -> skip
                    auto cost = CF.calculate_join_cost(PT, CE, *S1, *S2, cnf::CNF{}); // TODO use join condition
                    PT.update(*S1, *S2, cost);
                }
            }
        }
    }
}

/*======================================================================================================================
 * DPsizeOpt
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming.  In addition to `DPsize`, applies the following
 * optimizations.  First, do not enumerate symmetric subproblems.  Second, in case both subproblems are of equal size,
 * consider only subproblems succeeding the first subproblem. */
struct DPsizeOpt final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const override;
};

void DPsizeOpt::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    /* Process all subplans of size greater than one. */
    for (std::size_t s = 2; s <= n; ++s) {
        std::size_t m = s / 2; // division with rounding down
        for (std::size_t s1 = 1; s1 <= m; ++s1) {
            std::size_t s2 = s - s1;
            /* Check for all combinations of subsets if they are valid joins and if so, forward the combination to the
             * plan table. */
            if (s1 == s2) { // exploit commutativity of join: if A⋈ B is possible, so is B⋈ A
                for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                    if (not PT.has_plan(*S1)) continue; // subproblem not connected -> skip
                    GospersHack S2 = GospersHack::enumerate_from(*S1, n);
                    for (++S2; S2; ++S2) { // enumerate only the subsets following S1
                        if (not PT.has_plan(*S2)) continue; // subproblem not connected -> skip
                        if (*S1 & *S2) continue; // subproblems not disjoint -> skip
                        if (not M.is_connected(*S1, *S2)) continue; // subproblems not connected -> skip
                        /* Exploit commutativity of join. */
                        auto cost = CF.calculate_join_cost(PT, CE, *S1, *S2, cnf::CNF{}); // TODO use join condition
                        PT.update(*S1, *S2, cost);
                        cost = CF.calculate_join_cost(PT, CE, *S2, *S1, cnf::CNF{}); // TODO use join condition
                        PT.update(*S2, *S1, cost);
                    }
                }
            } else {
                for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                    if (not PT.has_plan(*S1)) continue; // subproblem not connected -> skip
                    for (auto S2 = GospersHack::enumerate_all(s2, n); S2; ++S2) { // enumerate all subsets of size `s2`
                        if (not PT.has_plan(*S2)) continue; // subproblem not connected -> skip
                        if (*S1 & *S2) continue; // subproblems not disjoint -> skip
                        if (not M.is_connected(*S1, *S2)) continue; // subproblems not connected -> skip
                        /* Exploit commutativity of join. */
                        auto cost = CF.calculate_join_cost(PT, CE, *S1, *S2, cnf::CNF{}); // TODO use join condition
                        PT.update(*S1, *S2, cost);
                        cost = CF.calculate_join_cost(PT, CE, *S1, *S2, cnf::CNF{}); // TODO use join condition
                        PT.update(*S2, *S1, cost);
                    }
                }
            }
        }
    }
}

/*======================================================================================================================
 * DPsizeSub
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming. */
struct DPsizeSub final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const override;
};

void DPsizeSub::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    /* Process all subplans of size greater than one. */
    for (std::size_t s = 2; s <= n; ++s) {
        for (auto S = GospersHack::enumerate_all(s, n); S; ++S) { // enumerate all subsets of size `s`
            if (not M.is_connected(*S)) continue; // not connected -> skip
            for (Subproblem O(least_subset(*S)); O != *S; O = Subproblem(next_subset(O, *S))) {
                Subproblem Comp = *S - O;
                insist(M.is_connected(O, Comp), "implied by S inducing a connected subgraph");
                if (not PT.has_plan(O)) continue; // not connected -> skip
                if (not PT.has_plan(Comp)) continue; // not connected -> skip
                auto cost = CF.calculate_join_cost(PT, CE, O, Comp, cnf::CNF{}); // TODO use join condition
                PT.update(O, Comp, cost);
            }
        }
    }
}

/*======================================================================================================================
 * DPsub
 *====================================================================================================================*/

/** Computes the join order using subset-based dynamic programming. */
struct DPsub final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const override;
};

void DPsub::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    auto &sources = G.sources();
    const std::size_t n = sources.size();
    const AdjacencyMatrix M(G);
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
        Subproblem S(i);
        if (S.size() == 1) continue; // no non-empty and strict subset of S -> skip
        if (not M.is_connected(S)) continue; // not connected -> skip
        for (Subproblem S1(least_subset(S)); S1 != S; S1 = Subproblem(next_subset(S1, S))) {
            Subproblem S2 = S - S1;
            insist(M.is_connected(S1, S2), "implied by S inducing a connected subgraph");
            if (not PT.has_plan(S1)) continue; // not connected -> skip
            if (not PT.has_plan(S2)) continue; // not connected -> skip
            auto cost = CF.calculate_join_cost(PT, CE, S1, S2, cnf::CNF{}); // TODO use join condition
            PT.update(S1, S2, cost);
        }
    }
}

/*======================================================================================================================
 * DPsubOpt
 *====================================================================================================================*/

/** Computes the join order using subset-based dynamic programming.  In comparison to `DPsub`, do not enumerate
 * symmetric subproblems. */
struct DPsubOpt final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const override;
};

void DPsubOpt::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    auto &sources = G.sources();
    const std::size_t n = sources.size();
    const AdjacencyMatrix M(G);
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
        Subproblem S(i);
        if (S.size() == 1) continue; // no non-empty and strict subset of S -> skip
        if (not M.is_connected(S)) continue;
        /* Compute break condition to avoid enumerating symmetric subproblems. */
        uint64_t offset = S.capacity() - __builtin_clzl(uint64_t(S));
        insist(offset != 0, "invalid subproblem offset");
        Subproblem limit(1UL << (offset - 1));
        for (Subproblem S1(least_subset(S)); S1 != limit; S1 = Subproblem(next_subset(S1, S))) {
            Subproblem S2 = S - S1; // = S \ S1;
            insist(M.is_connected(S1, S2), "implied by S inducing a connected subgraph");
            if (not PT.has_plan(S1)) continue; // not connected -> skip
            if (not PT.has_plan(S2)) continue; // not connected -> skip
            /* Exploit commutativity of join. */
            auto cost = CF.calculate_join_cost(PT, CE, S1, S2, cnf::CNF{}); // TODO use join condition
            PT.update(S1, S2, cost);
            cost = CF.calculate_join_cost(PT, CE, S1, S2, cnf::CNF{}); // TODO use join condition
            PT.update(S2, S1, cost);
        }
    }
}

/*======================================================================================================================
 * DPccp
 *====================================================================================================================*/

/** Computes the join order using connected subgraph complement pairs (CCP). */
struct DPccp final : PlanEnumerator
{
    /** For each connected subgraph (csg) `S1` of `G`, enumerate all complement connected subgraphs,
     * i.e.\ all csgs of `G - S1` that are connected to `S1`. */
    void enumerate_cmp(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF, PlanTable &PT,
                       const CardinalityEstimator &CE, Subproblem S1) const;
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const override;
};

void DPccp::enumerate_cmp(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF, PlanTable &PT,
                          const CardinalityEstimator &CE, Subproblem S1) const
{
    SmallBitset min = least_subset(S1); // node in `S1` with the lowest ID
    Subproblem Bmin((uint64_t(min) << 1UL) - 1UL); // all nodes 'smaller' than `min`
    Subproblem X(Bmin | S1); // exclude `S1` and all nodes with a lower ID than the smallest node in `S1`

    Subproblem N = M.neighbors(S1) - X;
    if (not N) return; // empty neighborhood

    /* Process subgraphs in breadth-first order.  The queue contains pairs of connected subgraphs and the corresponding
     * set of nodes to exclude. */
    std::queue<std::pair<Subproblem, Subproblem>> Q;

    for (std::size_t i = G.sources().size(); i != 0; --i) {
        Subproblem vi(1UL << (i - 1));
        if (not (vi & N)) continue; // `vi` is in neighborhood of `S1`
        /* Compute exclude set. */
        Subproblem excluded((1UL << i) - 1); // all nodes 'smaller' than the current node `vi`
        Subproblem excluded_N = excluded & N; // only exclude nodes in the neighborhood of `S1`
        Q.emplace(std::make_pair(vi, X | excluded_N));
        while (not Q.empty()) {
            auto [S, X] = Q.front();
            Q.pop();
            /* Update `PlanTable` with connected subgraph complement pair (S1, S). */
            auto cost = CF.calculate_join_cost(PT, CE, S1, S, cnf::CNF{}); // TODO use join condition
            PT.update(S1, S, cost);
            cost = CF.calculate_join_cost(PT, CE, S, S1, cnf::CNF{}); // TODO use join condition
            PT.update(S, S1, cost);

            Subproblem N = M.neighbors(S) - X;
            /* Iterate over all subsets `sub` in `N` */
            for (Subproblem sub(least_subset(N)); bool(sub); sub = Subproblem(next_subset(sub, N))) {
                /* Connected subgraph `S` expanded by `sub` constitutes a new connected subgraph.  For each of those
                 * connected subgraphs, do not consider the neighborhood `N` in addition to the existing set of excluded
                 * nodes. */
                Q.emplace(std::make_pair(S | sub, X | N));
            }
        }
    }
}

void DPccp::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    auto &sources = G.sources();
    const std::size_t n = sources.size();
    const AdjacencyMatrix M(G);
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    /* Process subgraphs in breadth-first order.  The queue contains pairs of connected subgraphs and the corresponding
     * set of nodes to exclude. */
    std::queue<std::pair<Subproblem, Subproblem>> Q;

    for (std::size_t i = n; i != 0; --i) {
        /* For a given single node subgraph, the node itself and all nodes with a lower ID are excluded. */
        Q.emplace(std::make_pair(Subproblem(1UL << (i - 1)), Subproblem ((1UL << i) - 1)));
        while (not Q.empty()) {
            auto [S, X] = Q.front();
            Q.pop();
            enumerate_cmp(G, M, CF, PT, CE, S);

            Subproblem N = M.neighbors(S) - X;
            /* Iterate over all subsets `sub` in `N` */
            for (Subproblem sub(least_subset(N)); bool(sub); sub = Subproblem(next_subset(sub, N)))
                /* Connected subgraph `S` expanded by `sub` constitutes a new connected subgraph.  For each of those
                 * connected subgraphs, do not consider the neighborhood `N` in addition to the existing set of excluded
                 * nodes. */
                Q.emplace(std::make_pair(S | sub, X | N));
        }
    }
}

/*======================================================================================================================
 * TDbasic
 *====================================================================================================================*/

struct TDbasic final : PlanEnumerator
{
    void PlanGen(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF, const CardinalityEstimator &CE,
                 PlanTable &PT, Subproblem S)
        const;
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &Pt) const override;
};

void TDbasic::PlanGen(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                      const CardinalityEstimator &CE, PlanTable &PT, Subproblem S) const
{
    if (not PT.has_plan(S)) {
        /* Naive Partitioning */
        /* Iterate over all non-empty and strict subsets in `S`. */
        for (Subproblem sub(least_subset(S)); sub != S; sub = Subproblem(next_subset(sub, S))) {
            Subproblem complement = S - sub;
            /* Check for valid connected subgraph complement pair. */
            if (uint64_t(least_subset(sub)) < uint64_t(least_subset(complement)) and
                M.is_connected(sub) and M.is_connected(complement))
            {
                /* Process `sub` and `complement` recursively. */
                PlanGen(G, M, CF, CE, PT, sub);
                PlanGen(G, M, CF, CE, PT, complement);

                /* Update `PlanTable`. */
                auto cost = CF.calculate_join_cost(PT, CE, sub, complement, cnf::CNF{}); // TODO use join condition
                PT.update(sub, complement, cost);
                cost = CF.calculate_join_cost(PT, CE, complement, sub, cnf::CNF{}); // TODO use join condition
                PT.update(complement, sub, cost);
            }
        }
    }
}

void TDbasic::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    PlanGen(G, M, CF, CE, PT, Subproblem((1UL << n) - 1));
}

/*======================================================================================================================
 * TDMinCutAGaT
 *====================================================================================================================*/

struct TDMinCutAGaT final : PlanEnumerator
{
    struct queue_entry
    {
        Subproblem C;
        Subproblem X;
        Subproblem T;

        queue_entry(Subproblem C, Subproblem X, Subproblem T) : C(C), X(X), T(T) { }
    };
    void MinCutAGaT(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                    const CardinalityEstimator &CE, PlanTable &PT,
                    Subproblem S, Subproblem C, Subproblem X, Subproblem T) const;
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const override;
};

void TDMinCutAGaT::MinCutAGaT(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                              const CardinalityEstimator &CE, PlanTable &PT,
                              Subproblem S, Subproblem C, Subproblem X, Subproblem T) const
{
    if (PT.has_plan(S)) return;

    std::queue<queue_entry> queue;
    queue.push(queue_entry(C, X, T));

    while (not queue.empty()) {
        auto e = queue.front();
        queue.pop();

        Subproblem T_tmp;
        Subproblem X_tmp;
        Subproblem N_T = (M.neighbors(e.T) & S) - e.C; // sufficient to check if neighbors of T are connected
        if (M.is_connected(N_T)) {
            /* ccp (C, S - C) found, process `C` and `S - C` recursively. */
            Subproblem cmpl = S - e.C;
            MinCutAGaT(G, M, CF, CE, PT,
                       e.C, Subproblem(least_subset(e.C)), Subproblem(0), Subproblem(least_subset(e.C)));
            MinCutAGaT(G, M, CF, CE, PT,
                       cmpl, Subproblem(least_subset(cmpl)), Subproblem(0), Subproblem(least_subset(cmpl)));

            /* Update `PlanTable`. */
            auto cost = CF.calculate_join_cost(PT, CE, e.C, cmpl, cnf::CNF{}); // TODO use join condition
            PT.update(e.C, cmpl, cost);
            cost = CF.calculate_join_cost(PT, CE, cmpl, e.C, cnf::CNF{}); // TODO use join condition
            PT.update(cmpl, e.C, cost);

            T_tmp = Subproblem(0);
        } else T_tmp = e.C;

        if (e.C.size() + 1 >= S.size()) continue;

        X_tmp = e.X;
        Subproblem N_C = M.neighbors(e.C);

        for (auto i : (N_C - e.X)) {
            Subproblem v(1UL << i);
            queue.push(queue_entry(e.C | v, X_tmp, T_tmp | v));
            X_tmp = X_tmp | v;
        }
    }
}

void TDMinCutAGaT::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    MinCutAGaT(G, M, CF, CE, PT, Subproblem((1UL << n) - 1), Subproblem(1), Subproblem(0), Subproblem(1));
}


/*======================================================================================================================
 * AI Planning
 *====================================================================================================================*/

namespace {

/** A "less than" comparator for `Subproblem`s. */
bool subproblem_lt(Subproblem left, Subproblem right) { return uint64_t(left) < uint64_t(right); };

/** Decomposes `Subproblem` `S` into two smaller, non-empty `Subproblem`s, that are connected w.r.t. `M`. */
std::pair<Subproblem, Subproblem> decompose(const Subproblem S, const AdjacencyMatrix &M)
{
    insist(S.size() >= 2);
    Subproblem left = least_subset(S);
    Subproblem right = S - left;
    while ((not M.is_connected(left)) || (not M.is_connected(right))) {
        left = next_subset(left, S);
        right = S - left;
    }
    insist((left | right) == S, "incorrect set decomposition");
    return {left, right};
}

/** Computes the `DataModel` of `Subproblem` `S` by recursive decomposition.  Requires that `S` is *connected* in `G`.
 */
void compute_data_model_recursive(const Subproblem S, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                                  const CardinalityEstimator &CE)
{
    insist(M.is_connected(S), "S must be a connected subproblem");

    if (PT[S].model) return; // we already have a data model
    insist(S.size() > 1, "data sources must have a model by construction of the plan table");

    auto [left, right] = decompose(S, M);
    compute_data_model_recursive(left,  PT, G, M, CE);
    compute_data_model_recursive(right, PT, G, M, CE);
    PT[S].model = CE.estimate_join(*PT[left].model, *PT[right].model, cnf::CNF());
}

/*----------------------------------------------------------------------------------------------------------------------
 * States
 *--------------------------------------------------------------------------------------------------------------------*/

// #define WITH_STATE_COUNTERS
#if !defined(NDEBUG) && !defined(WITH_STATE_COUNTERS)
#define WITH_STATE_COUNTERS
#endif

/** A state in the AI planning search space.
 *
 * A state consists of the accumulated costs of actions to reach this state from the from initial state and a desciption
 * of the actual problem within the state.  The problem is described as the sorted list of `Subproblem`s yet to be
 * joined.
 */
template<typename Actual, typename Allocator>
struct AIPlanningStateBase
{
    using actual_type = Actual;
    using size_type = std::size_t;
    using Subproblem = PlanTable::Subproblem;

    using iterator = Subproblem*;
    using const_iterator = const Subproblem*;
    using allocator_type = Allocator;

#ifdef WITH_STATE_COUNTERS
    struct state_counters_t
    {
        unsigned num_states_generated;
        unsigned num_states_expanded;
        unsigned num_states_constructed;
        unsigned num_states_disposed;

        state_counters_t()
            : num_states_generated(0)
            , num_states_expanded(0)
            , num_states_constructed(0)
            , num_states_disposed(0)
        { }
    };

    private:
    static state_counters_t state_counters_;

    public:
    static void RESET_STATE_COUNTERS() { state_counters_ = state_counters_t(); }
    static unsigned NUM_STATES_GENERATED() { return state_counters_.num_states_generated; }
    static unsigned NUM_STATES_EXPANDED() { return state_counters_.num_states_expanded; }
    static unsigned NUM_STATES_CONSTRUCTED() { return state_counters_.num_states_constructed; }
    static unsigned NUM_STATES_DISPOSED() { return state_counters_.num_states_disposed; }

    static void INCREMENT_NUM_STATES_GENERATED() { state_counters_.num_states_generated += 1; }
    static void INCREMENT_NUM_STATES_EXPANDED() { state_counters_.num_states_expanded += 1; }
    static void INCREMENT_NUM_STATES_CONSTRUCTED() { state_counters_.num_states_constructed += 1; }
    static void INCREMENT_NUM_STATES_DISPOSED() { state_counters_.num_states_disposed += 1; }

    static state_counters_t STATE_COUNTERS() { return state_counters_; }
    static state_counters_t STATE_COUNTERS(state_counters_t new_counters) {
        state_counters_t old_counters = state_counters_;
        state_counters_ = new_counters;
        return old_counters;
    }
#else
    public:
    static void RESET_STATE_COUNTERS() { /* nothing to be done */ };
    static unsigned NUM_STATES_GENERATED() { return 0; }
    static unsigned NUM_STATES_EXPANDED() { return 0; }
    static unsigned NUM_STATES_CONSTRUCTED() { return 0; }
    static unsigned NUM_STATES_DISPOSED() { return 0; }

    static void INCREMENT_NUM_STATES_GENERATED() { /* nothing to be done */ }
    static void INCREMENT_NUM_STATES_EXPANDED() { /* nothing to be done */ }
    static void INCREMENT_NUM_STATES_CONSTRUCTED() { /* nothing to be done */ }
    static void INCREMENT_NUM_STATES_DISPOSED() { /* nothing to be done */ }
#endif

    private:
    static allocator_type allocator_;

    public:
    static allocator_type & ALLOCATOR() { return allocator_; }
    static allocator_type ALLOCATOR(allocator_type &&A) {
        allocator_type tmp = std::move(allocator_);
        allocator_ = std::move(A);
        return tmp;
    }

    private:
    ///> the cost to reach this state from the initial state
    double g_;
    ///> number of subproblems in this state
    size_type size_ = 0;
    ///> array of subproblems
    std::unique_ptr<Subproblem[]> subproblems_;

    public:
    friend void swap(AIPlanningStateBase &first, AIPlanningStateBase &second) {
        using std::swap;
        swap(first.g_,           second.g_);
        swap(first.size_,        second.size_);
        swap(first.subproblems_, second.subproblems_);
    }

    AIPlanningStateBase() { insist(not subproblems_); }

    /** Creates a state with actual costs `g` and given `subproblems`. */
    AIPlanningStateBase(double g, size_type size, std::unique_ptr<Subproblem[]> subproblems)
        : g_(g)
        , size_(size)
        , subproblems_(std::move(subproblems))
    {
        insist(size == 0 or bool(subproblems_));
        insist(std::is_sorted(begin(), end(), subproblem_lt));
        INCREMENT_NUM_STATES_CONSTRUCTED();
    }

    /** Creates an initial state with the given `subproblems`. */
    AIPlanningStateBase(size_type size, std::unique_ptr<Subproblem[]> subproblems)
        : AIPlanningStateBase(0, size, std::move(subproblems))
    { }

    /** Creates a state with actual costs `g` and subproblems in range `[begin; end)`. */
    template<typename It>
    AIPlanningStateBase(double g, It begin, It end)
        : g_(g)
        , size_(std::distance(begin, end))
        , subproblems_(ALLOCATOR().template make_unique<Subproblem[]>(size_))
    {
        insist(begin != end);
        std::copy(begin, end, this->begin());
        insist(std::is_sorted(this->begin(), this->end(), subproblem_lt));
        INCREMENT_NUM_STATES_CONSTRUCTED();
    }

    ~AIPlanningStateBase() {
        if (subproblems_) INCREMENT_NUM_STATES_DISPOSED();
        ALLOCATOR().dispose(std::move(subproblems_), size_);
    }

    explicit AIPlanningStateBase(const AIPlanningStateBase &other)
        : g_(other.g_)
        , size_(other.size_)
        , subproblems_(ALLOCATOR().template make_unique<Subproblem[]>(size_))
    {
        INCREMENT_NUM_STATES_CONSTRUCTED();
        insist(bool(subproblems_));
        std::copy(other.begin(), other.end(), this->begin());
    }

    AIPlanningStateBase(AIPlanningStateBase &&other) : AIPlanningStateBase() { swap(*this, other); }
    AIPlanningStateBase & operator=(AIPlanningStateBase other) { swap(*this, other); return *this; }

    /** Compares two states lexicographically. */
    bool operator<(const AIPlanningStateBase &other) const {
        return std::lexicographical_compare(cbegin(), cend(), other.cbegin(), other.cend(), subproblem_lt);
    }

    /** Returns `true` iff `this` and `other` have the exact same `Subproblem`s. */
    bool operator==(const AIPlanningStateBase &other) const {
        if (this->size() != other.size()) return false;
        insist(this->size() == other.size());
        auto this_it = this->cbegin();
        auto other_it = other.cbegin();
        for (; this_it != this->cend(); ++this_it, ++other_it) {
            if (*this_it != *other_it)
                return false;
        }
        return true;
    }

    bool operator!=(const AIPlanningStateBase &other) const { return not operator==(other); }

    /** Returns the number of `Subproblem`s in this state. */
    std::size_t size() const { return size_; }

    Subproblem operator[](std::size_t idx) const { insist(idx < size_); return subproblems_[idx]; }

    /** Returns the cost to reach this state from the initial state. */
    double g() const { return g_; }

    /** Returns `true` iff this is a goal state. */
    bool is_goal() const { return static_cast<const actual_type*>(this)->is_goal(); }

    // const std::vector<Subproblem> & subproblems() const { return subproblems_; }

    iterator begin() { return subproblems_.get(); };
    iterator end() { return begin() + size(); }
    const_iterator begin() const { return subproblems_.get(); };
    const_iterator end() const { return begin() + size(); }
    const_iterator cbegin() const { return begin(); };
    const_iterator cend() const { return end(); }

    /** Calls `callback` on every state reachable from this state by a single actions. */
    template<typename Callback>
    void for_each_successor(Callback &&callback, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                            const CostFunction &CF, const CardinalityEstimator &CE) const {
        static_cast<const actual_type*>(this)->for_each_successor(std::forward<Callback>(callback), PT, G, M, CF, CE);
    }

    /** Returns a unique identifier for this state. */
    std::string id() const {
        std::ostringstream oss;
        oss << "state";
        for (auto s : *this)
            oss << '_' << s;
        return oss.str();
    }

    friend std::ostream & operator<<(std::ostream &out, const AIPlanningStateBase &S) {
        out << "g = " << S.g() << ", [";
        for (auto it = S.cbegin(); it != S.cend(); ++it) {
            if (it != S.cbegin()) out << ", ";
            it->print_fixed_length(out, 15);
        }
        return out << ']';
    }

    void dump(std::ostream &out) const { out << (Actual&)(*this) << std::endl; }
    void dump() const { dump(std::cerr); }
};

#ifdef WITH_STATE_COUNTERS
template<typename Actual, typename Allocator>
typename AIPlanningStateBase<Actual, Allocator>::state_counters_t
AIPlanningStateBase<Actual, Allocator>::state_counters_;
#endif

template<typename Actual, typename Allocator>
typename AIPlanningStateBase<Actual, Allocator>::allocator_type
AIPlanningStateBase<Actual, Allocator>::allocator_;

#define IMPORT_BASE(CLASS) \
    using base_type = AIPlanningStateBase<CLASS<Allocator>, Allocator>; \
    using size_type = typename base_type::size_type; \
    using \
        base_type::ALLOCATOR, \
        base_type::size, \
        base_type::g, \
        base_type::begin, base_type::end, \
        base_type::cbegin, base_type::cend, \
        base_type::dump

}

namespace std {

template<typename Actual, typename Allocator>
struct hash<AIPlanningStateBase<Actual, Allocator>>
{
    uint64_t operator()(const AIPlanningStateBase<Actual, Allocator> &state) const {
        /* Rolling hash with multiplier taken from [1] where the moduli is 2^64.
         * [1] http://www.ams.org/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf */
        uint64_t hash = 0;
        for (Subproblem s : state) {
            hash = hash ^ uint64_t(s);
            hash = hash * 3935559000370003845UL + 0xcbf29ce484222325UL;
        }
        return hash;
    }
};

}

namespace {

template<typename Allocator>
struct AIPlanningStateBottomUp : AIPlanningStateBase<AIPlanningStateBottomUp<Allocator>, Allocator>
{
    IMPORT_BASE(AIPlanningStateBottomUp);

    static AIPlanningStateBottomUp CreateInitial(const QueryGraph &G, const AdjacencyMatrix&) {
        const size_type size = G.sources().size();
        auto subproblems = ALLOCATOR().template make_unique<Subproblem[]>(size);
        for (auto ds : G.sources())
            new (&subproblems[ds->id()]) Subproblem(1UL << ds->id());
        insist(std::is_sorted(subproblems.get(), subproblems.get() + size, subproblem_lt));
        return AIPlanningStateBottomUp(size, std::move(subproblems));
    }

    static AIPlanningStateBottomUp CreateGoal(double g) {
        return AIPlanningStateBottomUp(g, 0, nullptr);
    }

    template<typename It>
    static AIPlanningStateBottomUp CreateFromSubproblems(const QueryGraph&, const AdjacencyMatrix&, double g,
                                                         It begin, It end) {
        return AIPlanningStateBottomUp(g, begin, end);
    }

    AIPlanningStateBottomUp() = default;

    /** Creates a state with actual costs `g` and given `subproblems`. */
    AIPlanningStateBottomUp(double g, size_type size, std::unique_ptr<Subproblem[]> subproblems)
        : base_type(g, size, std::move(subproblems))
    { }

    /** Creates an initial state with the given `subproblems`. */
    AIPlanningStateBottomUp(size_type size, std::unique_ptr<Subproblem[]> subproblems)
        : AIPlanningStateBottomUp(0, size, std::move(subproblems))
    { }

    /** Creates a state with actual costs `g` and subproblems in range `[begin; end)`. */
    template<typename It>
    AIPlanningStateBottomUp(double g, It begin, It end)
        : base_type(g, begin, end)
    { }

    explicit AIPlanningStateBottomUp(const AIPlanningStateBottomUp&) = default;
    AIPlanningStateBottomUp(AIPlanningStateBottomUp&&) = default;
    AIPlanningStateBottomUp & operator=(AIPlanningStateBottomUp&&) = default;

    bool is_goal() const { return size() <= 1; }

    template<typename Callback>
    void for_each_successor(Callback &&callback, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                            const CostFunction &CF, const CardinalityEstimator &CE) const;
};

template<typename Allocator>
template<typename Callback>
void AIPlanningStateBottomUp<Allocator>::for_each_successor(Callback &&callback,
                                                            PlanTable &PT, const QueryGraph&, const AdjacencyMatrix &M,
                                                            const CostFunction &CF,
                                                            const CardinalityEstimator &CE) const
{
    base_type::INCREMENT_NUM_STATES_EXPANDED();

    /* Enumerate all potential join pairs and check whether they are connected. */
    for (auto outer_it = cbegin(), outer_end = std::prev(cend()); outer_it != outer_end; ++outer_it)
    {
        const auto neighbors = M.neighbors(*outer_it);
        for (auto inner_it = std::next(outer_it); inner_it != cend(); ++inner_it) {
            insist(uint64_t(*inner_it) > uint64_t(*outer_it), "subproblems must be sorted");
            insist((*outer_it & *inner_it).empty(), "subproblems must not overlap");
            if (neighbors & *inner_it) { // inner and outer are joinable.
                /* Compute joined subproblem. */
                const Subproblem joined = *outer_it | *inner_it;

                /* Compute new subproblems after join */
                auto subproblems = ALLOCATOR().template make_unique<Subproblem[]>(size() - 1);
                Subproblem *ptr = subproblems.get();
                for (auto it = cbegin(); it != cend(); ++it) {
                    if (it == outer_it) continue; // skip outer
                    else if (it == inner_it) new (ptr++) Subproblem(joined); // replace inner
                    else new (ptr++) Subproblem(*it);
                }
                insist(std::is_sorted(subproblems.get(), subproblems.get() + size() - 1, subproblem_lt));

                /* Compute total cost. */
                const double total_cost = CF.calculate_join_cost(PT, CE, *outer_it, *inner_it, cnf::CNF{});
                PT.update(*outer_it, *inner_it, total_cost);

                /* Compute action cost. */
                const double action_cost = total_cost - (PT[*outer_it].cost + PT[*inner_it].cost);

                /* Create new AIPlanningState. */
                AIPlanningStateBottomUp S(g() + action_cost, size() - 1, std::move(subproblems));
                base_type::INCREMENT_NUM_STATES_GENERATED();
                callback(std::move(S));
            }
        }
    }
}

}

namespace std {

template<typename Allocator>
struct hash<AIPlanningStateBottomUp<Allocator>>
{
    uint64_t operator()(const AIPlanningStateBottomUp<Allocator> &state) const {
        return std::hash<typename std::remove_reference_t<decltype(state)>::base_type>{}(state);
    }
};

}

namespace {

template<typename Allocator>
struct AIPlanningStateBottomUpOpt : AIPlanningStateBase<AIPlanningStateBottomUpOpt<Allocator>, Allocator>
{
    IMPORT_BASE(AIPlanningStateBottomUpOpt);

    using join_iterator = SmallBitset*;
    using const_join_iterator = const SmallBitset*;

    /** Remember for each subproblem in this state, with which other subproblems it can join.  Do this asymmetrically to
     * avoid repeating commutative joins twice.  A `SmallBitset` contains the *indices* of `Subproblems` to join with.
     * */
    private:
    std::unique_ptr<SmallBitset[]> joins_;

    public:
    static AIPlanningStateBottomUpOpt CreateInitial(const QueryGraph &G, const AdjacencyMatrix &M) {
        const std::size_t num_sources = G.sources().size();
        auto subproblems = ALLOCATOR().template make_unique<Subproblem[]>(num_sources);
        auto joins = ALLOCATOR().template make_unique<SmallBitset[]>(num_sources);

        /* Initialize subproblems. */
        for (std::size_t idx = 0; idx != num_sources; ++idx) {
            Subproblem &S = subproblems[idx];
            new (&S) Subproblem(1UL << idx);
            const SmallBitset mask_back_edges = S.singleton_to_lo_mask();
            const Subproblem neighbors = M.neighbors(S);
            const SmallBitset forward_edges = neighbors - mask_back_edges;
            insist((forward_edges & mask_back_edges).empty(), "only forward edges");
            insist(forward_edges.empty() or *forward_edges.begin() > idx, "must not have backward edges");
            new (&joins[idx]) SmallBitset(forward_edges);
        }
        insist(std::is_sorted(subproblems.get(), subproblems.get() + num_sources, subproblem_lt));
        insist(joins[num_sources - 1].empty(), "last subproblem must not have forward edges");

        return AIPlanningStateBottomUpOpt(num_sources, std::move(subproblems), std::move(joins));
    }

    static AIPlanningStateBottomUpOpt CreateGoal(double g) {
        return AIPlanningStateBottomUpOpt(g, 0, nullptr, nullptr);
    }

    template<typename It>
    static AIPlanningStateBottomUpOpt CreateFromSubproblems(const QueryGraph&, const AdjacencyMatrix &M, double g,
                                                            It begin, It end)
    {
        const size_type size = std::distance(begin, end);
        auto joins = ALLOCATOR().template make_unique<SmallBitset[]>(size);

        auto outer_it = begin;
        for (std::size_t outer_idx = 0; outer_idx != size; ++outer_idx, ++outer_it) {
            SmallBitset &J = joins[outer_idx];
            new (&J) SmallBitset();
            const auto neighbors = M.neighbors(*outer_it);
            auto inner_it = std::next(outer_it);
            for (std::size_t inner_idx = outer_idx + 1; inner_idx < size; ++inner_idx, ++inner_it)
                J[inner_idx] = not (neighbors & *inner_it).empty();
        }

        return AIPlanningStateBottomUpOpt(g, begin, end, std::move(joins));
    }

    friend void swap(AIPlanningStateBottomUpOpt &first, AIPlanningStateBottomUpOpt &second) {
        using std::swap;
        swap<base_type>(first, second);
        swap(first.joins_, second.joins_);
    }

    AIPlanningStateBottomUpOpt() = default;

    /** Creates a state with actual costs `g` and given `subproblems`. */
    AIPlanningStateBottomUpOpt(double g, size_type size, std::unique_ptr<Subproblem[]> subproblems,
                               std::unique_ptr<SmallBitset[]> joins)
        : base_type(g, size, std::move(subproblems))
        , joins_(std::move(joins))
    {
        insist(size == 0 or bool(joins_));
#ifndef NDEBUG
        insist(std::is_sorted(cbegin(), cend(), subproblem_lt));
        for (std::size_t idx = 0; idx != size; ++idx) {
            const SmallBitset joins_with = joins_[idx];
            insist(not joins_with(idx), "must not self-join");
            insist(joins_with.empty() or *joins_with.begin() > idx, "must not have backward edges");
        }
#endif
    }

    /** Creates an initial state with the given `subproblems`. */
    AIPlanningStateBottomUpOpt(size_type size, std::unique_ptr<Subproblem[]> subproblems,
                               std::unique_ptr<SmallBitset[]> joins)
        : AIPlanningStateBottomUpOpt(0, size, std::move(subproblems), std::move(joins))
    { }

    /** Creates a state with actual costs `g` and subproblems in range `[begin; end)`. */
    template<typename It>
    AIPlanningStateBottomUpOpt(double g, It begin, It end, std::unique_ptr<SmallBitset[]> joins)
        : base_type(g, begin, end)
        , joins_(std::move(joins))
    { }

    ~AIPlanningStateBottomUpOpt() {
        ALLOCATOR().dispose(std::move(joins_), size());
    }

    explicit AIPlanningStateBottomUpOpt(const AIPlanningStateBottomUpOpt &other)
        : base_type(static_cast<const base_type&>(other))
    {
        joins_ = ALLOCATOR().template make_unique<Subproblem[]>(size());
        std::copy(other.joins_begin(), other.joins_end(), this->joins_begin());
    }

    AIPlanningStateBottomUpOpt(AIPlanningStateBottomUpOpt &&other)
        : AIPlanningStateBottomUpOpt()
    {
        swap(*this, other);
    }

    AIPlanningStateBottomUpOpt & operator=(AIPlanningStateBottomUpOpt other) { swap(*this, other); return *this; }

    bool is_goal() const { return size() <= 1; }

    join_iterator joins_begin() { return joins_.get(); }
    join_iterator joins_end() { return joins_begin() + size(); }
    const_join_iterator joins_begin() const { return joins_.get(); }
    const_join_iterator joins_end() const { return joins_begin() + size(); }
    const_join_iterator joins_cbegin() const { return joins_begin(); }
    const_join_iterator joins_cend() const { return joins_end(); }

    template<typename Callback>
    void for_each_successor(Callback &&callback, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                            const CostFunction &CF, const CardinalityEstimator &CE) const;

    friend std::ostream & operator<<(std::ostream &out, const AIPlanningStateBottomUpOpt &S) {
        out << "g = " << S.g() << ", [";
        for (auto it = S.cbegin(); it != S.cend(); ++it) {
            if (it != S.cbegin()) out << ", ";
            it->print_fixed_length(out, 15);
        }
        out << "], joins [";
        for (size_type idx = 0; idx != S.size(); ++idx) {
            if (idx != 0) out << ", ";
            S.joins_[idx].print_fixed_length(out, 15);
        }
        return out << ']';
    }
};

/** Erase the bit masked by `bit_mask` from `S` by moving all bits at positions higher than the masked bit down by one
 * position.  If the masked bit was set in the original `S`, set the bit that is `delta` positions higher. */
SmallBitset update_join(const SmallBitset S, const uint64_t bit_mask, const unsigned delta)
{
    insist(delta < 64);

    const uint64_t erased_bit = uint64_t(S) & bit_mask;
    const uint64_t extracted_bits = _pext_u64(uint64_t(S), ~bit_mask); // erase masked bit by parallel bit extract
    const uint64_t replace_bit = erased_bit << delta;
    insist(bool(replace_bit) == bool(erased_bit), "the replace bit is only set if the erased bit was set");

    return SmallBitset(extracted_bits | replace_bit);
}

template<typename Allocator>
template<typename Callback>
void AIPlanningStateBottomUpOpt<Allocator>::for_each_successor(Callback &&callback,
                                                               PlanTable &PT, const QueryGraph &G,
                                                               const AdjacencyMatrix &M, const CostFunction &CF,
                                                               const CardinalityEstimator &CE) const
{
#ifndef NDEBUG
    std::size_t num_states_generated = 0;
#endif

    base_type::INCREMENT_NUM_STATES_EXPANDED();

    /* Enumerate all join pairs. */
    for (std::size_t left_idx = 0; left_idx < size() - 1; ++left_idx) {
        const Subproblem left = (*this)[left_idx];
        const SmallBitset left_joins_with = joins_[left_idx];
        insist(left_joins_with.empty() or *left_joins_with.begin() > left_idx, "must not have backward edges");

        /* Compute mask of the join bit corresponding to `left`.  Used later for updating joins. */
        const uint64_t mask_join_with_left = 1UL << left_idx;

        for (auto right_idx : left_joins_with) {
            insist(left_idx < right_idx,
                   "joins are directed and must always point towards the subproblem of higher index");
            const Subproblem right = (*this)[right_idx];
            insist((left & right).empty(), "subproblems must be disjoint");
            insist(M.is_connected(left, right), "subproblems must be joinable");
            insist(uint64_t(left) < uint64_t(right), "subproblems must be ordered");

            /* Compute joined subproblem. */
            const Subproblem joined = left | right;
            const std::size_t new_joined_index = right_idx - 1; // left is erased, right is replaced by joined

            /* Compute delta of join bit for `left` and `joined`.  Used later for updating joins. */
            const unsigned delta_join_bits = new_joined_index - left_idx;

            /* Compute subproblems of successor. */
            auto subproblems = ALLOCATOR().template make_unique<Subproblem[]>(size() - 1);
            {
                auto ptr = subproblems.get();
                for (auto s : *this) {
#if 1
                    if (s == left) [[unlikely]]
                        continue; // left is erased, so skip
                    else if (s == right) [[unlikely]]
                        new (ptr++) Subproblem(joined); // right is replaced by joined
                    else [[likely]]
                        new (ptr++) Subproblem(s); // other entries remain
#else
                    new (ptr) Subproblem(s == right ? joined : s);
                    ptr += unsigned(s != left);
#endif
                }
                insist(std::is_sorted(subproblems.get(), subproblems.get() + size() - 1, subproblem_lt));
            }

            /* Compute join partners for each subproblem in the successor. */
            auto joins = ALLOCATOR().template make_unique<SmallBitset[]>(size() - 1);
            std::size_t idx = 0;

            /* Subproblems before `left`. */
            for (; idx < left_idx; ++idx) {
                const SmallBitset j = this->joins_[idx];
                insist(j.empty() or *j.begin() > idx,
                       "joins are directed and must always point towards the subproblem of higher index");
                const bool joined_with_left = j(left_idx);
                const bool joined_with_right = j(right_idx);
                const SmallBitset j_upd = update_join(j, mask_join_with_left, delta_join_bits); // TODO precompute PEXT mask

                insist(j_upd.size() + (joined_with_left and joined_with_right) == j.size(),
                       "looses exactly one join if it joined with both left and right");
                insist(not j_upd.empty() or j.size() <= 1,
                       "updated joins can only be empty if there was at most one join before");
                insist(j_upd.empty() or *j_upd.begin() > idx,
                       "joins are directed and must always point towards the subproblem of higher index");
                new (&joins[idx]) SmallBitset(j_upd);
            }
            insist(idx == left_idx);
            /* Subproblems between `left` and `right`. */
            for (/* skip left */ ++idx; idx < right_idx; ++idx) {
                const SmallBitset j = this->joins_[idx];
                const bool joined_with_left = this->joins_[left_idx](idx);
                const bool joined_with_right = j[right_idx];
                insist(j.empty() or *j.begin() > idx,
                       "joins are directed and must always point towards the subproblem of higher index");
                SmallBitset j_upd = SmallBitset(uint64_t(j) >> 1U); // erase by right shift, all lower bits are 0
                insist(j_upd.size() == j.size(), "must not loose a join");
                /* If `left` joined with the current subproblem, this subproblem will have a new edge to `joined`.  */
                insist(j_upd[new_joined_index] == joined_with_right);
                j_upd[new_joined_index] = joined_with_right or joined_with_left; // there was an edge from `left` to this subproblem
                insist(j_upd[new_joined_index] == joined_with_left or joined_with_right);
                // insist(joined_with_right or j_upd.size() == j.size() + joined_with_left, "must not loose a join");
                // insist(not joined_with_right or j_upd.size() == j.size(), "must not loose a join");
                insist(j_upd.empty() or *j_upd.begin() > idx - 1,
                       "joins are directed and must always point towards the subproblem of higher index");
                new (&joins[idx - 1]) SmallBitset(j_upd);
            }
            insist(idx == right_idx);
            /* Joined subproblems. */
            {
                SmallBitset j = this->joins_[left_idx] | this->joins_[right_idx];
                const SmallBitset mask_back_edges((1UL << (right_idx + 1U)) - 1UL);
                j = j - mask_back_edges;
                j = SmallBitset(uint64_t(j) >> 1U); // erase by right shift, all lower bits are 0
                insist(j.empty() or *j.begin() > idx - 1,
                       "joins are directed and must always point towards the subproblem of higher index");
                new (&joins[idx - 1]) SmallBitset(j);
            }
            /* Subproblems after `right`. */
            for (++idx; idx < size(); ++idx) {
                const SmallBitset j = this->joins_[idx];
                insist(j.empty() or *j.begin() > idx,
                       "joins are directed and must always point towards the subproblem of higher index");
                const SmallBitset j_upd = SmallBitset(uint64_t(j) >> 1U); // erase by right shift, all lower bits are 0
                insist(j_upd.size() == j.size(), "must not loose a join");
                insist(j_upd.empty() or *j_upd.begin() > idx - 1);
                new (&joins[idx - 1]) SmallBitset(j_upd);
            }

            /* Compute total cost. */
            const double total_cost = CF.calculate_join_cost(PT, CE, left, right, cnf::CNF{});
            PT.update(left, right, total_cost);

            /* Compute action cost. */
            const double action_cost = total_cost - (PT[left].cost + PT[right].cost);

            /* Create new AIPlanningState. */
            AIPlanningStateBottomUpOpt S(g() + action_cost, size() - 1, std::move(subproblems),
                                         std::move(joins));
            base_type::INCREMENT_NUM_STATES_GENERATED();
#ifndef NDEBUG
            ++num_states_generated;
#endif
            callback(std::move(S));
        }
    }

#ifndef NDEBUG
    {
#ifdef WITH_STATE_COUNTERS
        auto counters = base_type::STATE_COUNTERS();
#endif
        AIPlanningStateBottomUp<Allocator> clone(g(), begin(), end());
        std::size_t num_states_generated_by_clone = 0;
        auto callback = [&num_states_generated_by_clone](const AIPlanningStateBottomUp<Allocator>&) {
            ++num_states_generated_by_clone;
        };
        clone.for_each_successor(callback, PT, G, M, CF, CE);
        if (num_states_generated != num_states_generated_by_clone) {
            std::cerr << ">>> discrepancy detected (";
            if (num_states_generated < num_states_generated_by_clone)
                std::cerr << "LESS";
            else
                std::cerr << "MORE";
            std::cerr << ")\n"
                      << "  AIPlanningStateBottomUpOpt generated " << num_states_generated << " states,\n"
                      << "  AIPlanningStateBottomUp    generated " << num_states_generated_by_clone << " states\n"
                      << "  the state is:\n";
            dump(std::cerr);
            std::cerr << "  the cloned AIPlanningStateBottomUp is:\n";
            clone.dump(std::cerr);
            abort();
        }
#ifdef WITH_STATE_COUNTERS
        base_type::STATE_COUNTERS(std::move(counters));
#endif
    }
#endif
}

}

namespace std {

template<typename Allocator>
struct hash<AIPlanningStateBottomUpOpt<Allocator>>
{
    uint64_t operator()(const AIPlanningStateBottomUpOpt<Allocator> &state) const {
        return std::hash<typename std::remove_reference_t<decltype(state)>::base_type>{}(state);
    }
};

}

namespace {

template<typename Allocator>
struct AIPlanningStateTopDown : AIPlanningStateBase<AIPlanningStateTopDown<Allocator>, Allocator>
{
    IMPORT_BASE(AIPlanningStateTopDown);

    private:
    std::size_t number_of_goal_relations_; ///< number of single relations in a state to be a goal state

    public:
    static AIPlanningStateTopDown CreateInitial(const QueryGraph &G, const AdjacencyMatrix&) {
        int num_sources = G.sources().size();
        auto subproblems = ALLOCATOR().template make_unique<Subproblem[]>(1);
        new (&subproblems[0]) Subproblem((1UL << num_sources) - 1U);
        return AIPlanningStateTopDown(num_sources, 1, std::move(subproblems));
    }

    /** Creates a state with actual costs `g` and given `subproblems`. */
    AIPlanningStateTopDown(double g, std::size_t number_of_goal_relations, size_type size,
                           std::unique_ptr<Subproblem[]> subproblems)
        : base_type(g, size, std::move(subproblems))
        , number_of_goal_relations_(number_of_goal_relations)
    { }

    AIPlanningStateTopDown() = default;

    /** Creates an initial state with the given `subproblems`. */
    AIPlanningStateTopDown(std::size_t number_of_goal_relations, size_type size,
                           std::unique_ptr<Subproblem[]> subproblems)
        : AIPlanningStateTopDown(0, number_of_goal_relations, size, std::move(subproblems))
    { }

    /** Creates a state with actual costs `g` and subproblems in range `[begin; end)`. */
    template<typename It>
    AIPlanningStateTopDown(double g, size_type number_of_goal_relations, It begin, It end)
        : base_type(g, begin, end)
        , number_of_goal_relations_(number_of_goal_relations)
    { }

    explicit AIPlanningStateTopDown(const AIPlanningStateTopDown&) = default;
    AIPlanningStateTopDown(AIPlanningStateTopDown&&) = default;
    AIPlanningStateTopDown & operator=(AIPlanningStateTopDown&&) = default;

    /** Returns `true` iff this is a goal state. */
    bool is_goal() const { return size() == number_of_goal_relations_; }

    /** Calls `callback` on every state reachable from this state by a single actions. */
    template<typename Callback>
    void for_each_successor(Callback &&callback, PlanTable &PT, const QueryGraph&, const AdjacencyMatrix &M,
                            const CostFunction &CF, const CardinalityEstimator&) const;
};

template<typename Allocator>
template<typename Callback>
void AIPlanningStateTopDown<Allocator>::for_each_successor(Callback &&callback,
                                                           PlanTable &PT, const QueryGraph&, const AdjacencyMatrix &M,
                                                           const CostFunction&, const CardinalityEstimator &CE) const
{
    /* Enumerate all potential join pairs and check whether they are connected. */
    for (auto outer_it = cbegin(); outer_it != cend(); ++outer_it) {
        std::vector<Subproblem> subproblems;
        for (Subproblem sub(least_subset(*outer_it)); sub != *outer_it; sub = next_subset(sub, *outer_it)) {
            Subproblem complement = *outer_it - sub;
            /* Check for valid connected subgraph complement pair. */
            if (uint64_t(least_subset(sub)) < uint64_t(least_subset(complement)) and
                M.is_connected(sub) and M.is_connected(complement))
            {
                for (auto it = cbegin(); it != cend(); ++it) {
                    if (it == outer_it) continue; //skip subproblem to be split
                    else subproblems.emplace_back(*it);
                }
                subproblems.emplace_back(sub);
                subproblems.emplace_back(complement);
                //TODO Find a smarter and more efficient way to order
                std::sort(subproblems.begin(), subproblems.end(), subproblem_lt);
                auto left = CE.predict_cardinality(*PT.at(sub).model);
                auto right = CE.predict_cardinality(*PT.at(complement).model);
                AIPlanningStateTopDown S(/* g=           */ g() + left + right,
                                         /* #relations = */ number_of_goal_relations_,
                                         /* subproblems= */ subproblems.begin(), subproblems.end());
                subproblems.clear();
                callback(std::move(S));
            }
        }
    }
}

}

namespace std {

template<typename Allocator>
struct hash<AIPlanningStateTopDown<Allocator>>
{
    uint64_t operator()(const AIPlanningStateTopDown<Allocator> &state) const {
        return std::hash<typename std::remove_reference_t<decltype(state)>::base_type>{}(state);
    }
};

}

namespace heuristics {

/*----------------------------------------------------------------------------------------------------------------------
 * Heuristics
 *--------------------------------------------------------------------------------------------------------------------*/

/** This heuristic implements a perfect oracle, always returning the exact distance to the nearest goal state. */
template<typename State>
struct perfect_oracle
{
    using state_type = State;

    perfect_oracle(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction &CF,
                   const CardinalityEstimator&)
    {
        DPccp{}(G, CF, PT); // pre-compute costs
    }

    double operator()(const state_type &state, const PlanTable &PT, const QueryGraph&, const AdjacencyMatrix&,
                      const CostFunction&, const CardinalityEstimator&) const
    {
        // TODO calculate distance based on pre-filled PlanTable
        (void) state;
        (void) PT;
        return 0;
    }
};

/** This heuristic estimates the distance from a state to the nearest goal state as the sum of the sizes of all
 * `Subproblem`s yet to be joined.
 * This heuristic is admissible, yet dramatically underestimates the actual distance to a goal state.
 */
template<typename State>
struct hsum
{
    using state_type = State;

    hsum(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
         const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, const PlanTable &PT, const QueryGraph&, const AdjacencyMatrix&,
                      const CostFunction&, const CardinalityEstimator &CE) const
    {
        double distance = 0;
        if (state.size() > 1) {
            for (auto s : state)
                distance += CE.predict_cardinality(*PT[s].model);
        }
        return distance;
    }
};

/** This heuristic estimates the distance from a state to the nearest goal state as the product of the sizes of all
 * `Subproblem`s yet to be joined.
 * This heuristic is not admissible and dramatically overestimates the actual distance to a goal state. Serves as a
 * starting point for development of other heuristics that try to estimate the cost by making the margin of error for
 * the overestimation smaller.
 */
template<typename State>
struct hprod
{
    using state_type = State;

    hprod(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, const PlanTable &PT, const QueryGraph&, const AdjacencyMatrix&,
                      const CostFunction&, const CardinalityEstimator &CE) const
    {
        if (state.size() > 1) {
            double distance = 1;
            for (auto s : state)
                distance *= CE.predict_cardinality(*PT[s].model);
            return distance;
        }
        return 0;
    }
};

template<typename State>
struct bottomup_lookahead_cheapest
{
    using state_type = State;

    bottomup_lookahead_cheapest(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                                const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph&, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        if (state.is_goal()) return 0;

        double distance = 0;
        for (auto s : state)
            distance += CE.predict_cardinality(*PT[s].model);

        /* If there are only 2 subproblems left, the result of joining the two is the goal; no lookahead needed. */
        if (state.size() == 2)
            return distance;

        /* Look for least additional costs. */
        double min_additional_costs = std::numeric_limits<decltype(min_additional_costs)>::infinity();
        Subproblem min_subproblem_left;
        Subproblem min_subproblem_right;
        for (auto outer_it = state.cbegin(); outer_it != state.cend(); ++outer_it) {
            const auto neighbors = M.neighbors(*outer_it);
            for (auto inner_it = std::next(outer_it); inner_it != state.cend(); ++inner_it) {
                insist(uint64_t(*inner_it) > uint64_t(*outer_it), "subproblems must be sorted");
                insist((*outer_it & *inner_it).empty(), "subproblems must not overlap");
                if (neighbors & *inner_it) { // inner and outer are joinable.
                    const Subproblem joined = *outer_it | *inner_it;
                    if (not PT[joined].model) {
                        PT[joined].model = CE.estimate_join(*PT[*outer_it].model, *PT[*inner_it].model,
                                                            /* TODO */ cnf::CNF{});
                    }
                    const double total_cost = CF.calculate_join_cost(PT, CE, *outer_it, *inner_it, cnf::CNF{});
                    const double action_cost = total_cost - (PT[*outer_it].cost + PT[*inner_it].cost);
                    ///> XXX: Sum of different units: cost and cardinality
                    const double additional_costs = action_cost + CE.predict_cardinality(*PT[joined].model);
                    if (additional_costs < min_additional_costs) {
                        min_subproblem_left = *outer_it;
                        min_subproblem_right = *inner_it;
                        min_additional_costs = additional_costs;
                    }
                }
            }
        }
        distance += min_additional_costs;
        distance -= CE.predict_cardinality(*PT[min_subproblem_left].model);
        distance -= CE.predict_cardinality(*PT[min_subproblem_right].model);
        return distance;
    }
};

/** This heuristic estimates the distance to the goal by calculating certain checkpoints on the path from the goal state
 * to the current state and searching for plans between those checkpoints.
 * Each checkpoint is a `Subproblem` containing a certain number of relations that has the minimal size for that number
 * of relations, while still being reachable from the current state and still being able to reach the next checkpoint.
 * The last checkpoint is the goal state, while the first search is from the current state to the first checkpoint.
 * The heuristic value then is the sum of the results of the executed searches.
 * This heuristic is not admissible and depends highly on the strategy for chosing checkpoints.
 */
template<typename State>
struct checkpoints
{
    using state_type = State;

    private:
    using internal_state_type = AIPlanningStateBottomUp<typename State::allocator_type>;

    ///> the distance between two checkpoints, i.e. the difference in the number of relations contained in two
    ///> consecutive checkpoints
    static constexpr unsigned CHECKPOINT_DISTANCE = 3;

    /** This heuristic estimates the distance from a state to the nearest goal state as the sum of the sizes of all
     * `Subproblem`s yet to be joined.
     * This heuristic is admissible, yet dramatically underestimates the actual distance to a goal state.  */
    using internal_hsum = hsum<internal_state_type>;

    /** Represents one specific search done in the checkpoints heurisitc with its `initial_state` and `goal`.  Used to
     * determine which searches have already been conducted and thus need not be conducted again but can be loaded from
     * cache instead.  */
    struct SearchSection
    {
        private:
        internal_state_type initial_state_;
        Subproblem goal_;

        public:
        SearchSection(internal_state_type state, Subproblem goal)
            : initial_state_(std::move(state))
            , goal_(goal)
        { }

        const internal_state_type & initial_state() const { return initial_state_; }
        Subproblem goal() const { return goal_; }

        bool operator==(const SearchSection &other) const {
            return this->goal() == other.goal() and this->initial_state() == other.initial_state();
        }
        bool operator<(const SearchSection &other) const {
            return this->goal() <  other.goal() or
                  (this->goal() == other.goal() and this->initial_state() < other.initial_state());
        }
    };

    struct SearchSectionHash
    {
        uint64_t operator()(const SearchSection &section) const {
            using std::hash;
            return hash<internal_state_type>{}(section.initial_state()) * 0x100000001b3UL ^ uint64_t(section.goal());
        }
    };

    /** Represents a `Subproblem` that could be used as a checkpoint later.
     * Also saves the cardinality of that `Subproblem` for checkpoint calculation.  */
    struct PotentialCheckpoint
    {
        Subproblem checkpoint;
        std::size_t size;
    };


    /** This map caches the estimated costs for searches already performed. */
    std::unordered_map<SearchSection, double, SearchSectionHash> cached_searches_;

    /** States generated by greedy search with lookup_cheapest heuristic.  Maps states to distance to goal. */
    std::unordered_map<state_type, double> greedy_states_;

    /** Precomputed `PotentialCheckpoints` for every checkpoint size that may occur, ordered ascendingly by cardinality.
     * The index is equal to the iteration of the checkpoint calculation, counted from the goal, e.g. with a goal of 12
     * relations and `CHECKPOINT_DISTANCE` of 5, the 1st iteration looks for checkpoints of size `12 - 5 = 7`. Those
     * will be in `checkpoint_connected_subproblems_[1]`.  The 2nd iteration looks for checkpoints of size 2, those will
     * be in `checkpoint_connected_subproblems_[2]`.  */
    std::vector<std::vector<PotentialCheckpoint>> checkpoint_connected_subproblems_;

    ai::AStar<
        internal_state_type,
        internal_hsum,
        /*----- context ----- */
        PlanTable&,
        const QueryGraph&,
        const AdjacencyMatrix&,
        const CostFunction&,
        const CardinalityEstimator&
    > S_;

    public:
    checkpoints(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction&,
                const CardinalityEstimator &CE)
    {
        /* The maximum number of checkpoints that can occur during the use of this heuristic
         * The minus 2 eliminates the calculation of checkpoints of sizes 1 and 0, e.g.
         * For a query with 10 or 11 relations and `CHECKPOINT_DISTANCE` = 5, we only want a checkpoint at sizes 5 and 6
         * respectively.
         * We do not want a second checkpoint at sizes 0 and 1, respectively.  */
        const auto num_sources = G.sources().size();
        const std::size_t num_checkpoints = num_sources <= 2 ? 0 : (num_sources - 2) / CHECKPOINT_DISTANCE;
        checkpoint_connected_subproblems_.reserve(num_checkpoints);

        for (std::size_t i = 0; i != num_checkpoints; ++i) {
            std::vector<PotentialCheckpoint> &connected_subproblems = checkpoint_connected_subproblems_.emplace_back();
            insist(i * CHECKPOINT_DISTANCE < num_sources);
            const std::size_t checkpoint_size = num_sources - (i + 1) * CHECKPOINT_DISTANCE;

            /* Reserve size for connected_subproblems. */
            const auto est_num_checkpoints = n_choose_k_approx(num_sources, checkpoint_size);
            connected_subproblems.reserve(est_num_checkpoints);

            for (auto S = GospersHack::enumerate_all(checkpoint_size, num_sources); S; ++S) {
                if (M.is_connected(*S)) {
                    compute_data_model_recursive(*S, PT, G, M, CE);
                    std::size_t size = CE.predict_cardinality(*PT[*S].model);
                    connected_subproblems.emplace_back(PotentialCheckpoint{*S, size});
                }
            }
        }

        /* Sort the potential checkpoints by estimated subproblem size in ascending order to speed up search for best
         * checkpoint.  This way, the first element in the list, that can be reached from a certain state, is the one
         * with the smallest size and thus the checkpoint.  */
        for (std::size_t i = 0; i < checkpoint_connected_subproblems_.size(); i++) {
            std::sort(checkpoint_connected_subproblems_[i].begin(), checkpoint_connected_subproblems_[i].end(),
                      [](PotentialCheckpoint left, PotentialCheckpoint right) { return left.size < right.size; });
        }
    }

    checkpoints(const checkpoints&) = delete;
    checkpoints(checkpoints&&) = default;

    checkpoints & operator=(checkpoints&&) = default;

    /** Returns `true` iff `to` is reachable from `from`.  */
    bool is_reachable(const Subproblem to, const state_type &from) const {
        for (auto subproblem : from) {
            /* If `subproblem` intersects with but is no subset of `to`, we cannot reach `to` anymore. */
            if ((subproblem & to) and not subproblem.is_subset(to))
                return false;
        }
        return true;
    }

    /** Calculates the smallest (w.r.t. estimated cardinality) checkpoint for a given `iteration` for a given `state` on
     * the way to `goal`.  The `iteration` describes whether we look for the 1st, 2nd, etc. checkpoint, starting from
     * the goal of the whole search.  For example, with a query of 15 relations and a `CHECKPOINT_DISTANCE` of 5, the
     * 1st checkpoint would contain 10 relations and the 2nd checkpoint would contain 5 relations.  A potential
     * checkpoint must be a subset of `superset`. */
    Subproblem calculate_checkpoint(std::size_t iteration, const state_type &state, const Subproblem superset) {
        insist(iteration < checkpoint_connected_subproblems_.size());
        for (PotentialCheckpoint &PC : checkpoint_connected_subproblems_[iteration]) {
            if (not PC.checkpoint.is_subset(superset)) // checkpoint is not a subset
                continue;
            if (not is_reachable(PC.checkpoint, state)) // state cannot reach this checkpoint
                continue;
            return PC.checkpoint; // found cheapest feasible checkpoint
        }

        return Subproblem(0);
    }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE)
    {
        if (state.is_goal()) return 0;
#ifndef NDEBUG
        std::cerr << "h_checkpoints(" << state << ")\n";
#endif

        const std::size_t num_checkpoints = state.size() <= 2 ? 0 : (state.size() - 2) / CHECKPOINT_DISTANCE;

        /* No more checkpoints on the remaining path to a goal state, simply compute `hsum`. */
        if (num_checkpoints == 0) {
#ifndef NDEBUG
            std::cerr << " `  no more checkpoints, hsum = "
                      << hsum<state_type>{PT, G, M, CF, CE}(state, PT, G, M, CF, CE) << '\n';
#endif
            return hsum<state_type>{PT, G, M, CF, CE}(state, PT, G, M, CF, CE);
        }

        ///> the heuristic value determined by greedy search with lookahead_cheapest heuristic
        double h_greedy_bushy = 0;
        ///> the heuristic value determined by connecting checkpoints
        double h_checkpoints = 0;

        /*----- Run greedy search with lookup_cheapest heuristic to estimate bushy plan cost. ------------------------*/
        if constexpr (true) {
#ifdef WITH_STATE_COUNTERS
            const auto old_counters = state_type::STATE_COUNTERS();
            state_type::RESET_STATE_COUNTERS();
#endif

            std::vector<state_type> path; // collect states on the path to goal
            path.reserve(state.size() - 1);
            state_type runner = state_type(state); // copy current state
            bottomup_lookahead_cheapest<state_type> h(PT, G, M, CF, CE);

            while (not runner.is_goal()) {
                if (auto it = greedy_states_.find(runner); it != greedy_states_.end()) {
                    /* We found a state for which we know the distance to goal.  Take a shortcut. */
                    runner = state_type::CreateGoal(runner.g() + it->second); // dummy goal state with correct cost
                    break;
                }

                /* Find the "best" successor or `runner` w.r.t. g+h where `h` is lookahead_cheapest heuristic. */
                state_type best_successor;
                double least_successor_cost = std::numeric_limits<decltype(least_successor_cost)>::infinity();
                runner.for_each_successor([&](state_type successor) {
                    const double h_successor = h(successor, PT, G, M, CF, CE);
                    const double successor_cost = successor.g() + h_successor;
                    if (successor_cost < least_successor_cost) {
                        least_successor_cost = successor_cost;
                        best_successor = std::move(successor);
                    }
                }, PT, G, M, CF, CE);
                path.emplace_back(std::move(runner)); // add state to path s.t. we can later save distance to goal
                runner = std::move(best_successor); // advance to best successor
            }
            insist(runner.is_goal(), "we must reach a goal state by greedy search");

            /* Remember distance to goal for all states along the path. */
            for (state_type &s : path) {
                const double distance_to_goal = runner.g() - s.g();
                greedy_states_.emplace(std::move(s), distance_to_goal);
            }

            const double distance_to_goal = runner.g() - state.g();
            h_greedy_bushy = distance_to_goal;
#ifndef NDEBUG
            std::cerr << " `  h_greedy_bushy = " << h_greedy_bushy << '\n';
#endif

#ifdef WITH_STATE_COUNTERS
            state_type::STATE_COUNTERS(old_counters);
#endif
        } else {
            h_greedy_bushy = std::numeric_limits<decltype(h_greedy_bushy)>::infinity();
        }


        /*----- Compute checkpoints for this `state`. ----------------------------------------------------------------*/
        std::vector<Subproblem> checkpoints;
        {
            checkpoints.reserve(num_checkpoints + 1);
            /* The final *checkpoint* is the goal state. */
            checkpoints.emplace_back(Subproblem((1UL << G.sources().size()) - 1UL)); // goal state
            /* Compute checkpoints on the path from `state` to goal. */
            for (std::size_t i = 0; i != num_checkpoints; ++i) {
                Subproblem checkpoint = calculate_checkpoint(i, state, checkpoints.back());

                /* If, at some point in the checkpoint chain, no valid next checkpoint can be found, skip this
                 * checkpoint. */
                if (checkpoint.empty())
                    continue; // skip checkpoint of infeasible size

                /* Add checkpoint. */
                checkpoints.emplace_back(checkpoint);
            }

#if 0
            std::cerr << "  ` The checkpoints for this state are:";
            for (auto cp : checkpoints)
                std::cerr << "\n     " << cp;
            std::cerr << '\n';
#endif
        }

        /*----- Perform searches towards checkpoints.  ---------------------------------------------------------------*/
        {
            ///> the subproblems in the current state
            std::vector<Subproblem> current_subproblems_unpruned(state.cbegin(), state.cend());
            ///> the subproblems relevant for reaching the next checkpoint
            std::vector<Subproblem> subproblems_current_checkpoint;
            subproblems_current_checkpoint.reserve(state.size());
            ///> the available subproblems for the next checkpoint (must be pruned again)
            std::vector<Subproblem> subproblems_later_checkpoints;
            subproblems_later_checkpoints.reserve(state.size());

            /* Process checkpoints in reverse order, i.e. from largest to smallest. */
            // std::cerr << " `  searching for checkpoints:\n";
            for (auto it = checkpoints.rbegin(); it != checkpoints.rend(); ++it) {
                const Subproblem checkpoint = *it;
                // std::cerr << "     `  next checkpoint is " << checkpoint << '\n';
                subproblems_later_checkpoints.clear();
                subproblems_current_checkpoint.clear();

                /* Split available subproblems in those for current checkpoint and later checkpoints. */
                for (auto S : current_subproblems_unpruned) {
                    // Only add subproblems to next initial state, if they are not needed for the checkpoint
                    if (S.is_subset(checkpoint))
                        subproblems_current_checkpoint.emplace_back(S);
                    else
                        subproblems_later_checkpoints.emplace_back(S);
                }
                insist(std::is_sorted(subproblems_current_checkpoint.begin(), subproblems_current_checkpoint.end(),
                                      subproblem_lt));
                insist(std::is_sorted(subproblems_later_checkpoints.begin(), subproblems_later_checkpoints.end(),
                                      subproblem_lt));

                /* The current checkpoints becomes a subproblem for reaching later checkpoints. */
                const auto pos = std::upper_bound(subproblems_later_checkpoints.cbegin(),
                                                  subproblems_later_checkpoints.cend(),
                                                  checkpoint,
                                                  subproblem_lt);
                subproblems_later_checkpoints.insert(pos, checkpoint);

                insist(std::is_sorted(subproblems_current_checkpoint.begin(), subproblems_current_checkpoint.end(),
                                      subproblem_lt));
                insist(std::is_sorted(subproblems_later_checkpoints.begin(), subproblems_later_checkpoints.end(),
                                      subproblem_lt));

                /* Construct initial state for local search to next checkpoint. */
                internal_state_type initial_state = internal_state_type::CreateFromSubproblems(
                    G, M, 0, subproblems_current_checkpoint.begin(), subproblems_current_checkpoint.end()
                );
                SearchSection cache_candidate(std::move(initial_state), checkpoint);

                if (auto it = cached_searches_.find(cache_candidate); it != cached_searches_.end()) {
                    h_checkpoints += it->second;
                } else {
                    internal_hsum h(PT, G, M, CF, CE);
                    const double cost = S_.search(internal_state_type(cache_candidate.initial_state()), // clone
                                                  h, PT, G, M, CF, CE);
                    S_.clear();
                    h_checkpoints += cost;
                    cached_searches_.emplace_hint(it, std::move(cache_candidate), cost);
                }

                if (h_checkpoints >= h_greedy_bushy) {
#ifndef NDEBUG
                    std::cerr << " `  aborting checkpoints heuristic: h_checkpoints = " << h_checkpoints << '\n';
#endif
                    return h_greedy_bushy;
                }

                using std::swap;
                swap(current_subproblems_unpruned, subproblems_later_checkpoints);
            }
        }

        insist(h_checkpoints < h_greedy_bushy);
#ifndef NDEBUG
        std::cerr << " `  h_checkpoints = " << h_checkpoints << '\n';
#endif
        return h_checkpoints;
    }
};

/** This heuristic estimates the distance to the goal by calculating a path of `Subproblem`s from the goal state to the
 * current state, such that the successor of the goal state is the smallest `Subproblem` containing n-1 relations.
 * Each subsequent `Subproblem` on the path is chosen in a way, that it is the smallest `Subproblem` containing
 * predecessor_num_relations - 1 relations that can still be reached from the current state and can reach the already
 * chosen predecessor.
 * The heuristic value is then the sum of the sizes of all `Subproblem`s on this path plus the sum of the sizes of all
 * `Subproblem`s in the current state.
 */
template<typename State>
struct path
{
    using state_type = State;

    /** Represents a `Subproblem` that could be used as a checkpoint later.
     * Also saves the cardinality of that `Subproblem` for checkpoint calculation.  */
    struct PotentialCheckpoint
    {
        Subproblem checkpoint;
        std::size_t size;
    };

    private:
    /**
     * Precomputed `PotentialCheckpoints` for every step of the path from goal to the initial state.
     */
    std::vector<std::vector<PotentialCheckpoint>> connected_subproblems;

    public:
    path(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction&,
         const CardinalityEstimator &CE)
    {
        // Precompute connected `Subproblem`s for every possible size
        std::size_t number_of_checkpoints = G.sources().size();

        // Set index 0 to an empty vector
        connected_subproblems.push_back(std::vector<PotentialCheckpoint>());

        for (std::size_t i = 1; i < number_of_checkpoints - 1; i++) {
            connected_subproblems.push_back(std::vector<PotentialCheckpoint>());
            std::size_t checkpoint_size = G.sources().size() - i;
            for (auto S = GospersHack::enumerate_all(checkpoint_size, G.sources().size()); S; ++S) {
                if (M.is_connected(*S)) {
                    std::size_t size = CE.predict_cardinality(*(PT[*S].model));
                    connected_subproblems[i].emplace_back(PotentialCheckpoint{.checkpoint = *S, .size = size});
                }
            }
        }

        /*
         * Sort the potential checkpoints by estimated subproblem size in ascending order to speed up search for
         * best checkpoint. This way, the first element in the list, that can be reached from a certain state is the one
         * with the smallest size and thus the checkpoint.
         */
        for (std::size_t i = 0; i < connected_subproblems.size(); i++) {
            std::sort(connected_subproblems.at(i).begin(), connected_subproblems.at(i).end(),
                      [](PotentialCheckpoint left, PotentialCheckpoint right){
                          return left.size < right.size;
                      });
        }

    }

    /** Returns whether `goal` is reachable from `current_state`  */
    bool is_reachable(Subproblem goal, const state_type &current_state) const {
        for (auto subproblem : current_state) {
            // there has to be at least one relation from goal in subproblem but no relation that is not in goal can
            // be in subproblem
            if ((subproblem & goal) and not subproblem.is_subset(goal))
                return false;
        }
        return true;
    }

    /**
     * Calculates the checkpoint for a given `iteration`, for a given `state` on the way to `goal`.
     * The `iteration` describes, whether we look for the 1st, 2nd, 3rd ... checkpoint, starting from the goal of the
     * whole search, i.e. for a query with 15 relations, and a `CHECKPOINT_DISTANCE` of 5, the 1st checkpoint would
     * would contain 10 relations and the 2nd one 5
     */
    Subproblem calculate_checkpoint(std::size_t iteration, const state_type &state, Subproblem &goal)
    {
        for (PotentialCheckpoint &PC : connected_subproblems[iteration]) {
            if (PC.checkpoint.is_subset(goal) and is_reachable(PC.checkpoint, state))
                return PC.checkpoint;
        }

        return Subproblem(0);
    }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix&,
                      const CostFunction&, const CardinalityEstimator &CE)
    {
        // goal states have a heuristic value of 0
        if (state.size() == 1)
            return 0;

        // one step before goal, the sum of the sizes of the subproblems is the perfect heuristic
        if (state.size() == 2) {
            double distance = 0;
            for (auto s : state)
                distance += CE.predict_cardinality(*PT[s].model);
            return distance;
        }

        // For rationale see comment in constructor
        std::size_t number_of_checkpoints = state.size() - 2;

        std::size_t heuristic_value = 0;

        /********** Checkpoint calculation *****************************************************/
        Subproblem S((1UL << G.sources().size()) - 1U);
        std::vector<Subproblem> checkpoints;
        for (std::size_t i = 1; i <= number_of_checkpoints; i++) {
            Subproblem checkpoint = calculate_checkpoint(i, state, S);

            // If, at some point in the checkpoint chain, no valid next checkpoint can be found, return hsum
            if (checkpoint.empty()) {
                double distance = 0;
                if (state.size() > 1) {
                    for (auto s : state)
                        distance += CE.predict_cardinality(*PT[s].model);
                }
                return distance;
            }

            checkpoints.push_back(checkpoint);
            S = checkpoint;
        }

        /********** Sum Checkpoint sizes *****************************************************/
        for (auto checkpoint : checkpoints)
            heuristic_value += CE.predict_cardinality(*PT[checkpoint].model);

        for (auto state_subproblem : state)
            heuristic_value += CE.predict_cardinality(*PT[state_subproblem].model);

        return heuristic_value;
    }

};

}

/** Computes the join order using an AI Planning approach */
struct AIPlanning final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const override;
};

template<typename State, typename Heuristic, typename... Context>
using AStar = ai::AStar<State, Heuristic, Context...>;
template<typename State, typename Heuristic, typename... Context>
using wAStar = ai::wAStar<std::ratio<2, 1>>::type<State, Heuristic, Context...>;
template<typename State, typename Heuristic, typename... Context>
using lazyAStar = ai::lazy_AStar<State, Heuristic, Context...>;
template<typename State, typename Heuristic, typename... Context>
using beam_search = ai::beam_search<2>::type<State, Heuristic, Context...>;
template<typename State, typename Heuristic, typename... Context>
using dynamic_beam_search = ai::beam_search<-1U>::type<State, Heuristic, Context...>;
template<typename State, typename Heuristic, typename... Context>
using lazy_beam_search = ai::lazy_beam_search<2>::type<State, Heuristic, Context...>;
template<typename State, typename Heuristic, typename... Context>
using lazy_dynamic_beam_search = ai::lazy_beam_search<-1U>::type<State, Heuristic, Context...>;
template<typename State, typename Heuristic, typename... Context>
using acyclic_beam_search = ai::acyclic_beam_search<2>::type<State, Heuristic, Context...>;
template<typename State, typename Heuristic, typename... Context>
using acyclic_dynamic_beam_search = ai::acyclic_beam_search<-1U>::type<State, Heuristic, Context...>;

template<
    typename State,
    typename Heuristic,
    template<typename, typename, typename...> typename Search
>
void run_planner_config(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                        const CardinalityEstimator &CE)
{
    // State::ALLOCATOR(list_allocator(get_pagesize(), AllocationStrategy::Exponential));
    State::ALLOCATOR(malloc_allocator{});
    State::RESET_STATE_COUNTERS();
    State initial_state = State::CreateInitial(G, M);
    try {
        Heuristic h = Heuristic(PT, G, M, CF, CE);
        ai::solve<
            State,
            Heuristic,
            Search,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >(std::move(initial_state), h, PT, G, M, CF, CE);
    } catch (std::logic_error err) {
        std::cerr << "search did not reach a goal state, fall back to DPccp" << std::endl;
        DPccp dpccp;
        dpccp(G, CF, PT);
    }
#ifdef WITH_STATE_COUNTERS
    std::cerr <<   "States generated: " << State::NUM_STATES_GENERATED()
              << "\nStates expanded: " << State::NUM_STATES_EXPANDED()
              << "\nStates constructed: " << State::NUM_STATES_CONSTRUCTED()
              << "\nStates disposed: " << State::NUM_STATES_DISPOSED()
              << std::endl;
#endif
    State::ALLOCATOR(malloc_allocator{}); // reset allocator
}

void AIPlanning::operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
{
    Catalog &C = Catalog::Get();
    auto &CE = C.get_database_in_use().cardinality_estimator();
    AdjacencyMatrix M(G);

#define IS_PLANNER_CONFIG(STATE, HEURISTIC, SEARCH) \
    (streq(Options::Get().ai_state, #STATE) and streq(Options::Get().ai_heuristic, #HEURISTIC) and \
     streq(Options::Get().ai_search, #SEARCH))
#define EMIT_PLANNER_CONFIG(STATE, HEURISTIC, SEARCH) \
    if (IS_PLANNER_CONFIG(STATE, HEURISTIC, SEARCH)) \
    { \
        run_planner_config<\
            AIPlanningState ## STATE<malloc_allocator>,\
            heuristics:: HEURISTIC <AIPlanningState ## STATE<malloc_allocator>>,\
            SEARCH>\
        (PT, G, M, CF, CE); \
    }

         EMIT_PLANNER_CONFIG(BottomUp,      hsum,                           AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUpOpt,   hsum,                           AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUp,      hprod,                          AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUpOpt,   hprod,                          AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUp,      bottomup_lookahead_cheapest,    AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUp,      checkpoints,                    AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUp,      checkpoints,                    lazyAStar                       )
    else EMIT_PLANNER_CONFIG(BottomUp,      checkpoints,                    beam_search                     )
    else EMIT_PLANNER_CONFIG(BottomUp,      checkpoints,                    dynamic_beam_search             )
    else EMIT_PLANNER_CONFIG(BottomUp,      checkpoints,                    lazy_beam_search                )
    else EMIT_PLANNER_CONFIG(BottomUp,      checkpoints,                    lazy_dynamic_beam_search        )
    else EMIT_PLANNER_CONFIG(BottomUp,      checkpoints,                    acyclic_beam_search             )
    else EMIT_PLANNER_CONFIG(BottomUp,      checkpoints,                    acyclic_dynamic_beam_search     )
    else EMIT_PLANNER_CONFIG(BottomUpOpt,   checkpoints,                    AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUpOpt,   checkpoints,                    beam_search                     )
    else EMIT_PLANNER_CONFIG(BottomUpOpt,   checkpoints,                    dynamic_beam_search             )
    else EMIT_PLANNER_CONFIG(BottomUpOpt,   checkpoints,                    acyclic_beam_search             )
    else EMIT_PLANNER_CONFIG(BottomUpOpt,   checkpoints,                    acyclic_dynamic_beam_search     )
    else EMIT_PLANNER_CONFIG(BottomUp,      perfect_oracle,                 AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUp,      perfect_oracle,                 beam_search                     )
    else EMIT_PLANNER_CONFIG(BottomUp,      path,                           AStar                           )
    else EMIT_PLANNER_CONFIG(BottomUp,      path,                           beam_search                     )
    else { throw std::invalid_argument("illegal planner configuration"); }
#undef EMIT_PLANNER_CONFIG

#if 0
    {
        const auto ai_cost = PT.get_final().cost;
        DPccp dpccp;
        dpccp(G, CF, PT);
        const auto dp_cost = PT.get_final().cost;

        std::cerr << "AI: " << ai_cost << ", DP: " << dp_cost << ", Δ " << double(ai_cost) / dp_cost << 'x'
                  << std::endl;
    }
#endif
}

#define DB_PLAN_ENUMERATOR(NAME, _) \
    std::unique_ptr<PlanEnumerator> PlanEnumerator::Create ## NAME() { \
        return std::make_unique<NAME>(); \
    }
#include "mutable/tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR
