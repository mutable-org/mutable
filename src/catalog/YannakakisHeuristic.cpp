#include <mutable/catalog/YannakakisHeuristic.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/util/ADT.hpp>


using namespace m;


template<typename PlanTable>
double YannakakisHeuristic::estimate_decompose_costs(const QueryGraph &G, const PlanTable &PT, const CardinalityEstimator &CE, Subproblem problem)
{
    /* Single Tables do not need to be decomposed again */
    auto model = CE.estimate_full_reduction(G, *PT[problem].model);
    return CE.predict_cardinality(*model) * G.get_tuple_size_of_subproblem(problem) * (problem.size() - 1);
}

template
double YannakakisHeuristic::estimate_decompose_costs(const QueryGraph &G, const PlanTableSmallOrDense&, const CardinalityEstimator &CE, Subproblem problem);

template
double YannakakisHeuristic::estimate_decompose_costs(const QueryGraph &G, const PlanTableLargeAndSparse&, const CardinalityEstimator &CE, Subproblem problem);

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
    return 10 * (estimate_decompose_costs(G, PT, CE, left) + estimate_decompose_costs(G, PT, CE, right));
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
    double decompose_costs = 5 * (estimate_decompose_costs(G, PT, CE, left) + estimate_decompose_costs(G, PT, CE, right));
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
    double decompose_costs = 2 * (estimate_decompose_costs(G, PT, CE, left) + estimate_decompose_costs(G, PT, CE, right));
    auto reduction_costs = [&](Subproblem main, Subproblem other) {
        auto main_neighbors = G.adjacency_matrix().neighbors(main);
        auto main_model = CE.copy(*PT[main].model);
        auto main_model_red_card = CE.predict_cardinality(*CE.estimate_full_reduction(G, *main_model));
        auto other_model = CE.copy(*PT[other].model);
        for (auto other_neighbor: G.adjacency_matrix().neighbors(other) - main) {
            other_model = CE.estimate_semi_join(G, *other_model, *models.find(other_neighbor)->second, {});
        }
        auto other_card = CE.predict_cardinality(*CE.estimate_semi_join(G, *main_model, *other_model, {}));
        double costs = 0;
        for (auto neighbor : card_order) {
            if (not (Subproblem::Singleton(neighbor.first) & main_neighbors)) continue;
            costs += CE.predict_cardinality(*main_model) + main_model_red_card;
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
    return 10 * (estimate_decompose_costs(G, PT, CE, left) + estimate_decompose_costs(G, PT, CE, right));
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

