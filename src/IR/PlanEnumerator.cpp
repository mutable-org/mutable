#include "IR/PlanEnumerator.hpp"

#include "IR/Optimizer.hpp"
#include "IR/QueryGraph.hpp"
#include "util/ADT.hpp"
#include "util/fn.hpp"
#include <unordered_set>


#ifndef PE_COUNTER
#define PE_COUNTER 0
#endif


using namespace db;


void DummyPlanEnumerator::operator()(const QueryGraph&, const CostFunction&, PlanTable&) const
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
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

#if PE_COUNTER
    std::size_t inner_counter = 0;
    std::size_t csg_cmp_pair_counter = 0;
#endif

    /* Process all subplans of size greater than one. */
    for (std::size_t s = 2; s <= n; ++s) {
        for (std::size_t s1 = 1; s1 < s; ++s1) {
            std::size_t s2 = s - s1;
            /* Check for all combinations of subsets if they are valid joins and if so, forward the combination to the
             * plan table. */
            for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                for (auto S2 = GospersHack::enumerate_all(s2, n); S2; ++S2) { // enumerate all subsets of size `s - s1`
#if PE_COUNTER
                    ++inner_counter;
#endif
                    if (*S1 & *S2) continue; // not disjoint? -> skip
                    if (not M.is_connected(*S1, *S2)) continue; // not connected? -> skip
#if PE_COUNTER
                    ++csg_cmp_pair_counter;
#endif
                    PT.update(cf, *S1, *S2, 0);
                }
            }
        }
    }
#if PE_COUNTER
    std::cout << "DPsize:\n";
    std::cout << "  inner_counter: " << inner_counter << "\n";
    std::cout << "  csg_cmp_pair_counter: " << csg_cmp_pair_counter << "\n";
    std::cout << "  OnoLohmanCounter (#cpp): " << csg_cmp_pair_counter/2 << std::endl;
#endif
}

void DPsizeOpt::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    constexpr uint64_t MAX = std::numeric_limits<uint64_t>::max();
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

#if PE_COUNTER
    std::size_t inner_counter = 0;
    std::size_t csg_cmp_pair_counter = 0;
#endif

    /* Process all subplans of size greater than one. */
    for (std::size_t s = 2; s <= n; ++s) {
        std::size_t m = s/2 + 1;
        for (std::size_t s1 = 1; s1 < /*s*/m; ++s1) {
            std::size_t s2 = s - s1;
            /* Check for all combinations of subsets if they are valid joins and if so, forward the combination to the
             * plan table. */
            if (s1 == s2) { // if subproblems of equal size
                /* Use optimized version of DPsize.*/
                for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                    if (PT[*S1].cost == MAX) continue; // subproblem not considered, i.e. no join exists -> skip
                    GospersHack S2 = GospersHack::enumerate_from(*S1, n);
                    for (++S2; S2; ++S2) { // consider only the subsets following S1
                        if (PT[*S2].cost == MAX) continue; // subproblem not considered, i.e. no join exists -> skip
#if PE_COUNTER
                        ++inner_counter;
#endif
                        if (*S1 & *S2) continue; // not disjoint? -> skip
                        if (not M.is_connected(*S1, *S2)) continue; // not connected? -> skip
#if PE_COUNTER
                        ++csg_cmp_pair_counter;
#endif
                        /* Consider symmetry of subproblems. */
                        PT.update(cf, *S1, *S2, 0);
                        PT.update(cf, *S2, *S1, 0);
                    }
                }
            } else {
                /* Standard version. */
                for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                    if (PT[*S1].cost == MAX) continue; // subproblem not considered, i.e. no join exists -> skip
                    for (auto S2 = GospersHack::enumerate_all(s2, n); S2; ++S2) { // enumerate all subsets of size `s2`
                        if (PT[*S2].cost == MAX) continue; // subproblem not considered, i.e. no join exists -> skip
#if PE_COUNTER
                        ++inner_counter;
#endif
                        if (*S1 & *S2) continue; // not disjoint? -> skip
                        if (not M.is_connected(Subproblem(*S1), Subproblem(*S2))) continue; // not connected? -> skip
#if PE_COUNTER
                        ++csg_cmp_pair_counter;
#endif
                        /* Consider symmetry of subproblems. */
                        PT.update(cf, *S1, *S2, 0);
                        PT.update(cf, *S2, *S1, 0);
                    }
                }
            }
        }
    }
#if PE_COUNTER
    std::cout << "DPsizeOpt:\n";
    std::cout << "  inner_counter: " << inner_counter << "\n";
    std::cout << "  csg_cmp_pair_counter: " << csg_cmp_pair_counter << "\n";
    /* OnoLohmanCounter corresponds to `csg_cmp_pair_counter` because symmetric subproblems are already excluded. */
    std::cout << "  OnoLohmanCounter (#cpp): " << csg_cmp_pair_counter << std::endl;
#endif
}

void DPsub::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

#if PE_COUNTER
    std::size_t inner_counter = 0;
    std::size_t csg_cmp_pair_counter = 0;
#endif

    for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
        Subproblem S(i);
        if (S.size() == 1) continue; // no non-empty and strict subset of S -> skip
        if (not M.is_connected(S)) continue; // not connected? -> skip
        for (Subproblem S1(least_subset(S)); S1 != S; S1 = Subproblem(next_subset(S1, S))) {
#if PE_COUNTER
            ++inner_counter;
#endif
            Subproblem S2 = S - S1; // = S \ S1;
            if (not M.is_connected(S1)) continue; // not connected? -> skip
            if (not M.is_connected(S2)) continue; // not connected? -> skip
            if (not M.is_connected(S1, S2)) continue; // not connected? -> skip
#if PE_COUNTER
            ++csg_cmp_pair_counter;
#endif
            PT.update(cf, S1, S2, 0);
        }
    }
#if PE_COUNTER
    std::cout << "DPsub:\n";
    std::cout << "  inner_counter: " << inner_counter << "\n";
    std::cout << "  csg_cmp_pair_counter: " << csg_cmp_pair_counter << "\n";
    std::cout << "  OnoLohmanCounter (#cpp): " << csg_cmp_pair_counter/2 << std::endl;
#endif
}

void DPsubOpt::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

#if PE_COUNTER
    std::size_t inner_counter = 0;
    std::size_t csg_cmp_pair_counter = 0;
#endif

    for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
        Subproblem S(i);
        if (S.size() == 1) continue; // no non-empty and strict subset of S -> skip
        if (not M.is_connected(S)) continue;
        /* Compute break condition to avoid enumerating symmetric subproblems. */
        uint64_t offset = S.capacity() - __builtin_clzl(S);
        insist(offset != 0, "invalid subproblem offset");
        Subproblem limit(1UL << (offset - 1));
        for (Subproblem S1(least_subset(S)); S1 != limit; S1 = Subproblem(next_subset(S1, S))) {
#if PE_COUNTER
            ++inner_counter;
#endif
            Subproblem S2 = S - S1; // = S \ S1;
            if (not M.is_connected(S1)) continue; // not connected? -> skip
            if (not M.is_connected(S2)) continue; // not connected? -> skip
            if (not M.is_connected(S1, S2)) continue; // not connected? -> skip
#if PE_COUNTER
            ++csg_cmp_pair_counter;
#endif
            /* Consider symmetry of subproblems. */
            PT.update(cf, S1, S2, 0);
            PT.update(cf, S2, S1, 0);
        }
    }
#if PE_COUNTER
    std::cout << "DPsubOpt:\n";
    std::cout << "  inner_counter: " << inner_counter << "\n";
    std::cout << "  csg_cmp_pair_counter: " << csg_cmp_pair_counter << "\n";
    std::cout << "  OnoLohmanCounter (#cpp): " << csg_cmp_pair_counter << std::endl;
#endif
}

void DPccp::operator()(const QueryGraph&, const CostFunction&, PlanTable&) const
{
    // TODO implement
}
