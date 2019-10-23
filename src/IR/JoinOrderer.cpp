#include "IR/JoinOrderer.hpp"

#include "IR/JoinGraph.hpp"
#include <unordered_set>


using namespace db;


DummyJoinOrderer::mapping_type DummyJoinOrderer::operator()(const JoinGraph &G, const CostModel&) const
{
    mapping_type map;
    std::vector<const JoinGraph*> worklist;
    worklist.push_back(&G);

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

    return map;
}
