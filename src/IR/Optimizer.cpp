#include "IR/Optimizer.hpp"

#include "IR/Operator.hpp"
#include "storage/Store.hpp"


using namespace db;


void PlanTable::dump(std::ostream &out) const
{
    constexpr std::size_t width = 20;
    std::size_t n = 1UL << num_sources_;

    out << "Plan Table:\n";
    out << std::setw(Subproblem::CAPACITY) << "Subproblem" << "\t";
    out << std::setw(width) << "Size" << "\t";
    out << std::setw(width) << "Cost" << "\t";
    out << std::setw(Subproblem::CAPACITY) << "Left" << "\t";
    out << std::setw(Subproblem::CAPACITY) << "Right" << "\n";

    for (std::size_t i = 1; i < n; ++i) {
        out << /*std::setw(width) <<*/ Subproblem(i) << "\t";
        out << std::setw(width) << at(Subproblem(i)).size << "\t";
        out << std::setw(width) << at(Subproblem(i)).cost << "\t";
        out << /*std::setw(width) <<*/ at(Subproblem(i)).left << "\t";
        out << /*std::setw(width) <<*/ at(Subproblem(i)).right << "\n";
    }
}
void PlanTable::dump() const { dump(std::cerr); }

std::unique_ptr<Producer> Optimizer::operator()(const QueryGraph &G) const
{
    return std::move(optimize(G).first);
}

std::pair<std::unique_ptr<Producer>, PlanTable> Optimizer::optimize(const QueryGraph &G) const
{
    const auto num_sources = G.sources().size();
    PlanTable plan_table(num_sources);

    if (num_sources == 0)
        return { std::make_unique<ProjectionOperator>(G.projections()), std::move(plan_table) };

    /*----- Initialize plan table and compute plans for data sources. ------------------------------------------------*/
    Producer **source_plans = new Producer*[num_sources];
    for (auto ds : G.sources()) {
        Subproblem s(1UL << ds->id());
        if (auto bt = cast<const BaseTable>(ds)) {
            /* Produce a scan for base tables. */
            auto &store = bt->table().store();
            plan_table[s].cost = 0;
            plan_table[s].size = store.num_rows();
            source_plans[ds->id()] = new ScanOperator(store, bt->alias());
        }
        else {
            /* Recursively solve nested queries. */
            auto Q = as<const Query>(ds);
            auto [sub_plan, sub_table] = optimize(*Q->query_graph());
            auto &sub = sub_table.get_final();

            /* Prefix every attribute of the nested query with the nested query's alias. */
            Schema S;
            for (auto &e : sub_plan->schema())
                S.add({Q->alias(), e.id.name}, e.type);
            sub_plan->schema() = S;

            /* Update the plan table with the size and cost of the nested query and save the plan in the array of source
             * plans. */
            plan_table[s].cost = sub.cost;
            plan_table[s].size = sub.size;
            source_plans[ds->id()] = sub_plan.release();
        }
        /* Apply filter, if any. */
        if (ds->filter().size()) {
            auto filter = new FilterOperator(ds->filter());
            filter->add_child(source_plans[ds->id()]);
            source_plans[ds->id()] = filter;
        }
    }

    optimize_locally(G, plan_table);
    auto plan = construct_plan(G, plan_table, source_plans).release();

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

    /* Perform projection */
    if (not G.projections().empty() or G.projection_is_anti()) {
        auto projection = new ProjectionOperator(G.projections(), G.projection_is_anti());
        projection->add_child(plan);
        plan = projection;
    }

    /* Limit. */
    if (G.limit().limit or G.limit().offset) {
        auto limit = new LimitOperator(G.limit().limit, G.limit().offset);
        limit->add_child(plan);
        plan = limit;
    }

    plan->minimize_schema();
    delete[] source_plans;
    return std::make_pair(std::unique_ptr<Producer>(plan), std::move(plan_table));
}

void Optimizer::optimize_locally(const QueryGraph &G, PlanTable &plan_table) const
{
    plan_enumerator()(G, cost_function(), plan_table);
}

std::unique_ptr<Producer>
Optimizer::construct_plan(const QueryGraph &G, PlanTable &plan_table, Producer **source_plans) const
{
    auto joins = G.joins(); // create a "set" of all joins

    /* Use nested lambdas to implement recursive lambda using CPS. */
    const auto construct_recursive = [&](Subproblem s) -> Producer* {
        auto construct_plan_impl = [&](Subproblem s, auto &construct_plan_rec) -> Producer* {
            auto subproblems = plan_table[s].get_subproblems();
            if (subproblems.empty()) {
                insist(s.size() == 1);
                return source_plans[*s.begin()];
            } else {
                /* Compute plan for each sub problem.  Must happen *before* calculating the join predicate. */
                std::vector<Producer*> sub_plans;
                for (auto sub : subproblems)
                    sub_plans.push_back(construct_plan_rec(sub, construct_plan_rec));

                /* Calculate the join predicate. */
                cnf::CNF join_condition;
                for (auto it = joins.begin(); it != joins.end(); ) {
                    Subproblem join_sources;
                    /* Compute subproblem of sources to join. */
                    for (auto ds : (*it)->sources())
                        join_sources.set(ds->id());

                    if (join_sources.is_subset(s)) { // possible join
                        join_condition = join_condition and (*it)->condition();
                        it = joins.erase(it);
                    } else {
                        ++it;
                    }
                }

                /* Construct the join. */
                auto join = new JoinOperator(join_condition, JoinOperator::J_NestedLoops); // TODO use better algo
                for (auto sub_plan : sub_plans)
                    join->add_child(sub_plan);
                return join;
            }
        };
        return construct_plan_impl(s, construct_plan_impl);
    };

    return std::unique_ptr<Producer>(construct_recursive(Subproblem((1UL << G.sources().size()) - 1)));
}
