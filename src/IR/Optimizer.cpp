#include "IR/Optimizer.hpp"

#include "IR/Operator.hpp"
#include "storage/Store.hpp"


using namespace db;



std::unique_ptr<Producer> Optimizer::operator()(const JoinGraph &G) const
{
    /* Compute join order. */
    auto order = join_orderer()(G, cost_model());
#ifndef NDEBUG
    for (auto g : order)
        std::cout << g.second << '\n';
#endif

    /* Construct operator tree from join order. */
    return build_operator_tree(G, order);
}

std::unique_ptr<Producer> Optimizer::build_operator_tree(const JoinGraph &G,
                                                         const JoinOrderer::mapping_type &orders) const
{
    auto order = orders.at(&G);
    using relation_set = std::unordered_set<const DataSource*>;
    using entry_type = std::pair<relation_set, Producer*>;
    std::vector<entry_type> stack; ///< stack of (partial) results

    for (auto e : order) {
        if (e.is_join()) {
            auto J = e.as_join();

            /* Compute the arity of this join. */
            unsigned arity = 0;
            for (auto it = stack.rbegin(); it != stack.rend(); ++it) {
                auto &e = *it;
                if (intersect(J->sources(), e.first))
                    ++arity;
                else
                    break;
            }
            insist(arity >= 1, "join arity must be at least one, in which case it degenerates to a filter");

            if (arity == 1) {
                /* If the arity of the join is 1, it has degenerated to a filter condition. */
                auto filter = new FilterOperator(J->condition());
                auto e = stack.back();
                stack.pop_back();
                filter->add_child(e.second);
                stack.emplace_back(entry_type(e.first, filter));
            } else {
                /* The arity is at least 2, hence this is a "real" join. */
                auto join = new JoinOperator(J->condition(), JoinOperator::J_NestedLoops);
                relation_set S;
                while (arity--) {
                    auto e = stack.back();
                    stack.pop_back();
                    join->add_child(e.second);
                    S.insert(e.first.begin(), e.first.end());
                }
                stack.emplace_back(S, join);
            }
        } else if (auto ds = cast<const BaseTable>(e.as_datasource())) {
            /* Create a table scan. */
            auto &table = ds->table();
            Producer *scan = new ScanOperator(table.store(), ds->alias());
            if (ds->filter().size()) {
                auto filter = new FilterOperator(ds->filter());
                filter->add_child(scan);
                scan = filter;
            }
            stack.emplace_back(entry_type({ds}, scan));
        } else {
            auto query = as<const Query>(e.as_datasource());
            unreachable("subqueries not yet implemented");
        }
    }

    // TODO implement projection, group by, order by

#ifndef NDEBUG
    std::cerr << "Optimizer constructed the following operator tree(s):\n";
    for (auto it = stack.cbegin(); it != stack.cend(); ++it) {
        if (it != stack.cbegin()) std::cerr << "\n====================\n";
        std::cerr << '{';
        for (auto ds = it->first.cbegin(); ds != it->first.cend(); ++ds) {
            if (ds != it->first.cbegin()) std::cerr << ", ";
            std::cerr << (*ds)->alias();
        }
        std::cerr << "}:\n";
        it->second->dump(std::cerr);
    }
#endif

    return std::unique_ptr<Producer>(stack[0].second);
}
