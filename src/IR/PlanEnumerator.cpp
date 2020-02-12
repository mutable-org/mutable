#include "IR/PlanEnumerator.hpp"

#include "IR/Optimizer.hpp"
#include "IR/QueryGraph.hpp"
#include "util/ADT.hpp"
#include "util/fn.hpp"
#include <unordered_set>


using namespace db;


void DummyPlanEnumerator::operator()(const QueryGraph &G, const CostFunction&, PlanTable &PT) const
{
#if 0
    while (not worklist.empty()) {
        auto G = worklist.back();
        worklist.pop_back();
        auto &order = map[G];

        std::unordered_set<const DataSource*> frontier;
        if (not G->sources().empty())
            frontier.insert(*G->sources().begin());
        std::unordered_set<const Join*> joins(G->joins().begin(), G->joins().end()); ///< set of available joins
        std::unordered_set<const DataSource*> used; ///< set of already joined data sources

        while (not frontier.empty()) {
            /* Pick the next relation from the frontier. */
            auto r = *frontier.begin();
            used.insert(r);
            order.push_back(r); // emit

            /* Check if relation is subquery. */
            if (auto q = cast<const Query>(r))
                worklist.push_back(q->query_graph());

            /* Apply all possible joins. */
            std::vector<const Join*> joins_used;
            for (auto j : joins) { // TODO apply smaller joins first to get better plans
                if (subset(j->sources(), used)) {
                    joins_used.push_back(j);
                    order.push_back(j); // emit
                }
            }
            for (auto j : joins_used) joins.erase(j); // erase the used joins so every join is used exactly once

            /* Update frontier.  Explore 'r' by adding all its unexplored neighbors to the frontier. */
            frontier.erase(r);
            for (auto j : r->joins()) {
                for (auto n : j->sources()) {
                    if (used.count(n) == 0)
                        frontier.insert(n);
                }
            }
        }
    }
#endif
}

void DPsize::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t N = sources.size();
    AdjacencyMatrix M(G);

    /* Both counters serve for debugging. */
    std::size_t inner_counter = 0;
    std::size_t csg_cmp_pair_counter = 0;

    /* Process all subplans of size greater than one. */
    for (std::size_t s = 2, n = sources.size(); s <= n; ++s) {
        for (std::size_t s1 = 1; s1 < s; ++s1) {
            /* Check for all combinations of subsets if they are valid joins and if so, forward the combination to the cost model. */
            std::size_t s2 = s - s1;
            for (auto S1 = GospersHack::enumerate_all(s1, N); S1; ++S1) { // enumerate all subsets of size `s1`
                for (auto S2 = GospersHack::enumerate_all(s2, N); S2; ++S2) { // enumerate all subsets of size `s - s1`
                    ++inner_counter;
                    if (*S1 & *S2) continue; // check for disjointness
                    if (not M.is_connected(Subproblem(*S1), Subproblem(*S2))) continue; // check for connectedness
                    ++csg_cmp_pair_counter;
                    PT.update(cf, Subproblem(*S1), Subproblem(*S2), 0);
                }
            }
        }
    }
}

void DPsizeOpt::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t N = sources.size();
    AdjacencyMatrix M(G);

    /* Both counters serve for debugging. */
    std::size_t inner_counter = 0;
    std::size_t csg_cmp_pair_counter = 0;

    /* Process all subplans of size greater than one. */
    for (std::size_t s = 2, n = sources.size(); s <= n; ++s) {
        for (std::size_t s1 = 1; s1 < s; ++s1) {
            /* Check for all combinations of subsets if they are valid joins and if so, forward the combination to the cost model. */
            std::size_t s2 = s - s1;
            if (s1 == s2) { // if subproblems of equal size
                /* Use optimized version of DPsize.*/
                for (auto S1 = GospersHack::enumerate_all(s1, N); S1; ++S1) { // enumerate all subsets of size `s1`
                    GospersHack S2 = GospersHack::enumerate_from(*S1, N);
                    for (++S2; S2; ++S2) { // consider only the subsets following S1
                        ++inner_counter;
                        if (*S1 & *S2) continue; // check for disjointness
                        if (not M.is_connected(Subproblem(*S1), Subproblem(*S2))) continue; // check for connectedness
                        ++csg_cmp_pair_counter;
                        PT.update(cf, Subproblem(*S1), Subproblem(*S2), 0);
                    }
                }
            } else {
                /* Standard version. */
                for (auto S1 = GospersHack::enumerate_all(s1, N); S1; ++S1) { // enumerate all subsets of size `s1`
                    for (auto S2 = GospersHack::enumerate_all(s2, N); S2; ++S2) { // enumerate all subsets of size `s - s1`
                        ++inner_counter;
                        if (*S1 & *S2) continue; // check for disjointness
                        if (not M.is_connected(Subproblem(*S1), Subproblem(*S2))) continue; // check for connectedness
                        ++csg_cmp_pair_counter;
                        PT.update(cf, Subproblem(*S1), Subproblem(*S2), 0);
                    }
                }
            }
        }
    }
}

void DPsub::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t N = sources.size();
    AdjacencyMatrix M(G);

    /* Both counters serve for debugging. */
    std::size_t inner_counter = 0;
    std::size_t csg_cmp_pair_counter = 0;

    for (std::size_t i = 1, end = 1UL << N; i < end; ++i) {
        Subproblem S(i); // {Rj | floor(i/2^j) mod 2 = 1}
        if (not M.is_connected(S)) continue;
        for (Subproblem S1(least_subset(S)); S1 != 0; S1 = Subproblem(next_subset(S1, S))) {
            ++inner_counter;
            Subproblem S2 = S - S1; // = S \ S1;
            if (S2.empty()) continue;
            if (not M.is_connected(S1)) continue;
            if (not M.is_connected(S2)) continue;
            if (not M.is_connected(S1, S2)) continue;
            ++csg_cmp_pair_counter;
            PT.update(cf, S1, S2, 0);
        }
    }
}

void DPccp::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    // TODO implement
}
