#include "IR/Optimizer.hpp"

#include "IR/Operator.hpp"
#include "storage/Store.hpp"


using namespace db;



std::unique_ptr<Producer> Optimizer::operator()(const JoinGraph &G) const
{
    /* Compute join order. */
    auto order = join_orderer()(G, cost_model());

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

    if (stack.empty())
        return std::make_unique<ProjectionOperator>(G.projections());

    auto e = stack.back();
    auto result = e.second;
    stack.pop_back();

    /* Perform grouping */
    if (not G.group_by().empty()) {
        // TODO pick "best" algorithm
        auto group_by = new GroupingOperator(G.group_by(), G.aggregates(), GroupingOperator::G_Hashing);
        group_by->add_child(result);
        result = group_by;
    }

     /* Perform ordering */
    if (not G.order_by().empty()) {
        auto order_by = new SortingOperator(G.order_by());
        order_by->add_child(result);
        result = order_by;
    }

    //TODO if expression requires computation, replace expression in final projection

    /* Perform projection */
    if (G.projections().empty()) {
        auto projection = new ProjectionOperator(ProjectionOperator::Anti());
        projection->add_child(result);
        result = projection;
    } else {
        auto projection = new ProjectionOperator(G.projections());
        projection->add_child(result);
        result = projection;
    }

    if (G.limit().limit or G.limit().offset) {
        auto limit = new LimitOperator(G.limit().limit, G.limit().offset);
        limit->add_child(result);
        result = limit;
    }


    stack.emplace_back(e.first, result);
    return std::unique_ptr<Producer>(stack[0].second);
}
