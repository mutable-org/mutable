#include "IR/PlanEnumerator.hpp"

#include "IR/Optimizer.hpp"
#include "IR/QueryGraph.hpp"
#include "util/ADT.hpp"
#include "util/fn.hpp"
#include <unordered_set>
#include <queue>


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

#if PE_COUNTER
std::size_t ccp = 0;
#endif

void DPccp::enumerate_cmp(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &cf, PlanTable &PT,
                          Subproblem S1) const
{
    std::size_t min = least_subset(S1); // node in `S1` with the lowest ID
    Subproblem Bmin((min << 1) - 1); // all nodes 'smaller' than `min`
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
#if PE_COUNTER
            ++ccp;
#endif
            /* Update `PlanTable` with connected subgraph complement pair (S1, S). */
            PT.update(cf, S1, S, 0);
            PT.update(cf, S, S1, 0);

            Subproblem N = M.neighbors(S) - X;
            /* Iterate over all subsets `sub` in `N` */
            for (Subproblem sub(least_subset(N)); sub != 0; sub = Subproblem(next_subset(sub, N))) {
                /* Connected subgraph `S` expanded by `sub` constitutes a new connected subgraph.  For each of those
                 * connected subgraphs, do not consider the neighborhood `N` in addition to the existing set of excluded
                 * nodes. */
                Q.emplace(std::make_pair(S | sub, X | N));
            }
        }
    }
}

void DPccp::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

    /* Process subgraphs in breadth-first order.  The queue contains pairs of connected subgraphs and the corresponding
     * set of nodes to exclude. */
    std::queue<std::pair<Subproblem, Subproblem>> Q;

    for (std::size_t i = n; i != 0; --i) {
        /* For a given single node subgraph, the node itself and all nodes with a lower ID are excluded. */
        Q.emplace(std::make_pair(Subproblem(1UL << (i - 1)), Subproblem ((1UL << i) - 1)));
        while (not Q.empty()) {
            auto [S, X] = Q.front();
            Q.pop();
            enumerate_cmp(G, M, cf, PT, S);

            Subproblem N = M.neighbors(S) - X;
            /* Iterate over all subsets `sub` in `N` */
            for (Subproblem sub(least_subset(N)); sub != 0; sub = Subproblem(next_subset(sub, N)))
                /* Connected subgraph `S` expanded by `sub` constitutes a new connected subgraph.  For each of those
                 * connected subgraphs, do not consider the neighborhood `N` in addition to the existing set of excluded
                 * nodes. */
                Q.emplace(std::make_pair(S | sub, X | N));
        }
    }

#if PE_COUNTER
    std::cout << "DPccp:\n";
    std::cout << "  #ccp: " << ccp << "\n";
#endif
}

void TDbasic::PlanGen(const QueryGraph &G, const CostFunction &cf, PlanTable &PT, const AdjacencyMatrix &M,
                      Subproblem S) const
{
    if (not PT.has_plan(S)) {
        /* Naive Partitioning */
        /* Iterate over all non-empty and strict subsets in `S`. */
        for (Subproblem sub(least_subset(S)); sub != S; sub = Subproblem(next_subset(sub, S))) {
            Subproblem complement = S - sub;
            /* Check for valid connected subgraph complement pair. */
            if (least_subset(sub) < least_subset(complement) and M.is_connected(sub) and M.is_connected(complement)){
                /* Process `sub` and `complement` recursively. */
                PlanGen(G, cf, PT, M, sub);
                PlanGen(G, cf, PT, M, complement);

                /* Update `PlanTable`. */
                PT.update(cf, sub, complement, 0);
                PT.update(cf, complement, sub, 0);
            }
        }
    }
}

void TDbasic::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

    PlanGen(G, cf, PT, M, Subproblem((1UL << n) - 1));
}
