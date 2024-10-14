#include <mutable/catalog/YannakakisHeuristic.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/util/ADT.hpp>


using namespace m;


double YannakakisHeuristic::estimate_decompose_costs(const QueryGraph &G, Subproblem complete_problem, const PlanTableEntry& entry, const CardinalityEstimator &CE)
{
    // Single Problems do not have to be decomposed
    if (complete_problem.size() == 1) return 0;
    auto model = CE.estimate_full_reduction(G, *entry.model);
    return 2 * double(CE.predict_cardinality(*model)) * double(entry.tuple_size);
}

/*======================================================================================================================
 * DecomposeHeuristic
 *====================================================================================================================*/
template<typename PlanTable>
DecomposeHeuristic::DecomposeHeuristic(const PlanTable &PT, Subproblem problem , const QueryGraph &G, const CardinalityEstimator &CE) {

}

template<typename PlanTable>
double
DecomposeHeuristic::operator()(estimate_tag, const PlanTable &PT, Subproblem left, Subproblem right, const QueryGraph &G, const CardinalityEstimator &CE) const
{
    return estimate_decompose_costs(G, left, PT[left], CE) + estimate_decompose_costs(G, left,PT[right], CE);
}

template
double
DecomposeHeuristic::operator()(estimate_tag, const PlanTableSmallOrDense&, Subproblem, Subproblem, const QueryGraph&, const CardinalityEstimator&) const;
template
double
DecomposeHeuristic::operator()(estimate_tag, const PlanTableLargeAndSparse&, Subproblem, Subproblem, const QueryGraph&, const CardinalityEstimator&) const;

template
DecomposeHeuristic::DecomposeHeuristic(const PlanTableSmallOrDense&, Subproblem, const QueryGraph&, const CardinalityEstimator&);

template
DecomposeHeuristic::DecomposeHeuristic(const PlanTableLargeAndSparse&, Subproblem, const QueryGraph&, const CardinalityEstimator&);

/*======================================================================================================================
 *  SizeHeuristic
 *====================================================================================================================*/
template<typename PlanTable>
SizeHeuristic::SizeHeuristic(const PlanTable &PT, Subproblem problem , const QueryGraph &G, const CardinalityEstimator &CE) {

}

template<typename PlanTable>
double
SizeHeuristic::operator()(estimate_tag, const PlanTable &PT, Subproblem left, Subproblem right, const QueryGraph &G, const CardinalityEstimator &CE) const
{
    double decompose_costs = estimate_decompose_costs(G, left, PT[left], CE) + estimate_decompose_costs(G, right, PT[right], CE);
    auto reduction_costs = [&](Subproblem main, Subproblem other) {
        auto main_size = CE.predict_cardinality(*PT[main].model);
        auto neighbors_without_other = (G.adjacency_matrix().neighbors(main) - other);
        double costs = (neighbors_without_other.size() + 1) * main_size;
        return costs;
    };
    return decompose_costs + reduction_costs(left, right) + reduction_costs(right, left);
}

template
double
SizeHeuristic::operator()(estimate_tag, const PlanTableSmallOrDense&, Subproblem, Subproblem, const QueryGraph&, const CardinalityEstimator&) const;
template
double
SizeHeuristic::operator()(estimate_tag, const PlanTableLargeAndSparse&, Subproblem, Subproblem, const QueryGraph&, const CardinalityEstimator&) const;

template
SizeHeuristic::SizeHeuristic(const PlanTableSmallOrDense&, Subproblem, const QueryGraph&, const CardinalityEstimator&);

template
SizeHeuristic::SizeHeuristic(const PlanTableLargeAndSparse&, Subproblem, const QueryGraph&, const CardinalityEstimator&);

/*======================================================================================================================
 * WeakCardinalityHeuristic
 *====================================================================================================================*/

template<typename PlanTable>
WeakCardinalityHeuristic::WeakCardinalityHeuristic(const PlanTable &PT, Subproblem problem , const QueryGraph &G, const CardinalityEstimator &CE) {
    for (auto neighbor: G.adjacency_matrix().neighbors(problem)) {
        auto neighbor_model = CE.copy(*PT[Subproblem::Singleton(neighbor)].model);
        auto reduced_neighbor_model = CE.estimate_full_reduction(G, *neighbor_model, problem);
        card_order.emplace_back(neighbor, CE.predict_cardinality(*CE.estimate_semi_join(G, *PT[problem].model, *neighbor_model, {}
        )));
        models.emplace(neighbor, std::move(neighbor_model));
    }
    auto cmp = [](std::pair<std::size_t, std::size_t> a, std::pair<std::size_t, std::size_t>  b){
        return a.second < b.second;
    };
    std::sort(card_order.begin(), card_order.end(), cmp);
}

template<typename PlanTable>
double
WeakCardinalityHeuristic::operator()(estimate_tag, const PlanTable &PT, Subproblem left, Subproblem right, const QueryGraph &G, const CardinalityEstimator &CE) const
{
    if (right.empty()) {
        auto decompose_costs = estimate_decompose_costs(G, left, PT[left], CE);
        auto main_neighbors = G.adjacency_matrix().neighbors(left);
        /* If no neighbors are present, you can simply use the reduction from the neighbor */
        auto main_model = CE.copy(*PT[left].model);
        if (main_neighbors.empty()) {
            return decompose_costs;
        }
        double reduction_costs = 0;
        for (auto neighbor : card_order) {
            if (not (Subproblem::Singleton(neighbor.first) & main_neighbors)) continue;
            reduction_costs += CE.predict_cardinality(*main_model);
            main_model = CE.estimate_semi_join(G, *main_model, *models.find(neighbor.first)->second, {});
        }
        return decompose_costs + reduction_costs;
    }
    auto decompose_costs = estimate_decompose_costs(G, left, PT[left], CE) + estimate_decompose_costs(G, right, PT[right], CE);
    auto reduction_costs = [&](Subproblem main, Subproblem other) {
        auto main_neighbors = G.adjacency_matrix().neighbors(main) - other;
        /* If no neighbors are present, you can simply use the reduction from the neighbor */
        auto main_model = CE.copy(*PT[main].model);
        if (main_neighbors.empty()) {
            return double(CE.predict_cardinality(*main_model));
        }
        auto other_model = CE.copy(*PT[other].model);
        for (auto other_neighbor: G.adjacency_matrix().neighbors(other) - main) {
            other_model = CE.estimate_semi_join(G, *other_model, *models.find(other_neighbor)->second, {});
        }
        auto other_card = CE.predict_cardinality(*CE.estimate_semi_join(G, *main_model, *other_model, {}));
        double costs = 0;
        for (auto neighbor : card_order) {
            if (not (Subproblem::Singleton(neighbor.first) & main_neighbors)) continue;
            costs += CE.predict_cardinality(*main_model);
            if (other_card < neighbor.second) {
                main_model = CE.estimate_semi_join(G, *main_model, *PT[other].model, {});
                other_card = std::numeric_limits<std::size_t>::max();
            } else {
                main_model = CE.estimate_semi_join(G, *main_model, *models.find(neighbor.first)->second, {});
            }
        }
        return costs;
    };
    return decompose_costs + reduction_costs(left, right) + reduction_costs(right, left);
}

template
double
WeakCardinalityHeuristic::operator()(estimate_tag, const PlanTableSmallOrDense&, Subproblem, Subproblem, const QueryGraph&, const CardinalityEstimator&) const;
template
double
WeakCardinalityHeuristic::operator()(estimate_tag, const PlanTableLargeAndSparse&, Subproblem, Subproblem, const QueryGraph&, const CardinalityEstimator&) const;

template
WeakCardinalityHeuristic::WeakCardinalityHeuristic(const PlanTableSmallOrDense&, Subproblem, const QueryGraph&, const CardinalityEstimator&);

template
WeakCardinalityHeuristic::WeakCardinalityHeuristic(const PlanTableLargeAndSparse&, Subproblem, const QueryGraph&, const CardinalityEstimator&);

/*======================================================================================================================
 * StrongCardinalityHeuristic
 *====================================================================================================================*/
template<typename PlanTable>
StrongCardinalityHeuristic::StrongCardinalityHeuristic(const PlanTable &PT, Subproblem problem , const QueryGraph &G, const CardinalityEstimator &CE) {
    auto rec = [&](std::size_t node, std::size_t parent, auto&& rec)->std::unique_ptr<DataModel> {
        auto node_problem = Subproblem::Singleton(node);
        auto node_model = CE.copy(*PT[node_problem].model);
        for (auto neighbor : G.adjacency_matrix().neighbors(node_problem) - problem) {
            if (neighbor == parent) continue;
            auto neighbor_model = rec(neighbor, node, rec);
            node_model = CE.estimate_semi_join(G, *node_model, *neighbor_model, {});
        }
        return std::move(node_model);
    };
    for (auto neighbor: G.adjacency_matrix().neighbors(problem)) {
        auto neighbor_model = rec(neighbor, neighbor, rec);
        models.emplace(neighbor, std::move(neighbor_model));
    }
}

template<typename PlanTable>
double
StrongCardinalityHeuristic::operator()(estimate_tag, const PlanTable &PT, Subproblem left, Subproblem right, const QueryGraph &G, const CardinalityEstimator &CE) const
{
    // TODO
    return 10 * (estimate_decompose_costs(G, left | right, PT[left], CE) + estimate_decompose_costs(G, left |right,  PT[right], CE));
}

template
double
StrongCardinalityHeuristic::operator()(estimate_tag, const PlanTableSmallOrDense&, Subproblem, Subproblem, const QueryGraph&, const CardinalityEstimator&) const;
template
double
StrongCardinalityHeuristic::operator()(estimate_tag, const PlanTableLargeAndSparse&, Subproblem, Subproblem, const QueryGraph&, const CardinalityEstimator&) const;

template
StrongCardinalityHeuristic::StrongCardinalityHeuristic(const PlanTableSmallOrDense&, Subproblem, const QueryGraph&, const CardinalityEstimator&);

template
StrongCardinalityHeuristic::StrongCardinalityHeuristic(const PlanTableLargeAndSparse&, Subproblem, const QueryGraph&, const CardinalityEstimator&);

