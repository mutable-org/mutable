#include "IR/Optimizer.hpp"

#include "IR/Operator.hpp"
#include "storage/Store.hpp"


using namespace db;



std::unique_ptr<Producer> Optimizer::operator()(const QueryGraph &G) const
{
    /* Compute join order. */
    auto order = join_orderer()(G, cost_model());

    /* Construct operator tree from join order. */
    return build_operator_tree(G, order);
}

std::unique_ptr<Producer> Optimizer::build_operator_tree(const QueryGraph &G,
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
        } else if (auto query = cast<const Query>(e.as_datasource())) {
            /* Create sub query. */
            auto sub = build_operator_tree(*query->query_graph(), orders).release();
            if (query->filter().size()) {
                auto filter = new FilterOperator(query->filter());
                filter->add_child(sub);
                sub = filter;
            }
            stack.emplace_back(entry_type({query}, sub));
        } else
            unreachable("unsupported join order entry");
    }

    if (stack.empty())
        return std::make_unique<ProjectionOperator>(G.projections());

    insist(stack.size() == 1, "unconnected results");
    auto plan = stack.back().second;
    stack.pop_back();

    /* Perform grouping */
    if (not G.group_by().empty() or not G.aggregates().empty()) {
        // TODO pick "best" algorithm
        auto group_by = new GroupingOperator(G.group_by(), G.aggregates(), GroupingOperator::G_Hashing);
        group_by->add_child(plan);
        plan = group_by;
    }

     /* Perform ordering */
    if (not G.order_by().empty()) {
        auto order_by = new SortingOperator(G.order_by());
        order_by->add_child(plan);
        plan = order_by;
    }

    //TODO if expression requires computation, replace expression in final projection

    /* Perform projection */
    if (not G.projections().empty() or G.projection_is_anti())
    {
        auto projection = new ProjectionOperator(G.projections(), G.projection_is_anti());
        projection->add_child(plan);
        plan = projection;
    }

    if (G.limit().limit or G.limit().offset) {
        auto limit = new LimitOperator(G.limit().limit, G.limit().offset);
        limit->add_child(plan);
        plan = limit;
    }

    plan->minimize_schema();
    return std::unique_ptr<Producer>(plan);
}
