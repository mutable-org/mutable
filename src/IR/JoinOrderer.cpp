#include "IR/JoinOrderer.hpp"

#include "IR/JoinGraph.hpp"
#include <unordered_set>


using namespace db;


DummyJoinOrderer::mapping_type DummyJoinOrderer::operator()(const JoinGraph &G) const
{
    mapping_type map;
    std::vector<const JoinGraph*> worklist;
    worklist.push_back(&G);

    while (not worklist.empty()) {
        auto G = worklist.back();
        worklist.pop_back();

        auto &order = map[G];

        std::unordered_set<const DataSource*> joined; ///< set of already joined data sources

        for (auto J : G->joins()) {
            for (auto S : J->sources()) {
                if (not joined.count(S)) {
                    order.push_back(S);
                    joined.insert(S);
                }
            }
            order.push_back(J);
        }
    }

    return map;
}
