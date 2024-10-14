#pragma once

#include <mutable/mutable-config.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/YannakakisHeuristic.hpp>
#include <mutable/IR/PlanEnumerator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <unordered_set>


namespace m {

/** The optimizer interface.
 *
 * The `Optimizer` applies a join ordering algorithm to a query graph to compute a join order that minimizes the costs
 * under a given logical cost function.
 * Additionally, the optimizer may apply several semantics preserving transformations to improve performance.  Such
 * transformations include query unnesting and predicate inference.
 */
struct M_EXPORT Optimizer
{
    using Subproblem = QueryGraph::Subproblem;
    using projection_type = QueryGraph::projection_type;
    using order_type = QueryGraph::order_type;

    private:
    const pe::PlanEnumerator &pe_;
    const CostFunction &cf_;
    ///> additionally created expressions; static to match lifetime of optimized logical plan
    static thread_local inline std::vector<std::unique_ptr<const ast::Expr>> created_exprs_;
    mutable bool needs_projection_ = false; ///< flag to determine whether current query needs a projection as root

    public:
    Optimizer(const pe::PlanEnumerator &pe, const CostFunction &cf) : pe_(pe), cf_(cf) { }

    auto & plan_enumerator() const { return pe_; }
    auto & cost_function() const { return cf_; }

    /** Applies this optimizer to the given query graph \p G to compute an optimal logical operator tree. */
    std::unique_ptr<Producer> operator()(QueryGraph &G) const { return optimize(G).first; }

    /** Computes and constructs an optimal logical plan for the given query graph \p G.  Selects a `PlanTableBase`
     * type to represent the internal state of planning progress, then delegates to `optimize_with_plantable<>()`. */
    std::pair<std::unique_ptr<Producer>, PlanTableEntry> optimize(QueryGraph &G) const;

    /** Recursively computes and constructs an optimal logical plan for the given query graph \p G, using the given
     * \tparam PlanTable type to represent the state of planning progress. */
    template<typename PlanTable>
    std::pair<std::unique_ptr<Producer>, PlanTable> optimize_with_plantable(QueryGraph &G) const;

    /** Optimizes the filter \p filter by splitting it into smaller filters and ordering them. */
    static std::vector<cnf::CNF> optimize_filter(cnf::CNF filter);

    private:
    /** Initializes the plan table \p PT with the data source entries contained in \p G.  Returns the
     * (potentially recursively optimized) logical plan for each data source. */
    template<typename PlanTable>
    std::unique_ptr<Producer*[]> optimize_source_plans(const QueryGraph &G, PlanTable &PT) const;

    /** Optimizes the join order using the plan table \p PT which already contains entries for all data sources of
     * the query graph \p G. */
    template<typename PlanTable>
    void optimize_join_order(const QueryGraph &G, PlanTable &PT) const;

    /** Constructs a join operator tree given a solved plan table \p PT and the plans to compute the data sources
     * \p source_plans of the query graph \p G. */
    template<typename PlanTable>
    std::unique_ptr<Producer> construct_join_order(const QueryGraph &G, const PlanTable &PT,
                                                   const std::unique_ptr<Producer*[]> &source_plans) const;

    /** Optimizes and constructs an operator tree given a join operator tree \p plan and the final plan table entry
     * \p entry for the query graph \p G. */
    std::unique_ptr<Producer> optimize_plan(QueryGraph &G, std::unique_ptr<Producer> plan, PlanTableEntry &entry) const;

    /** Computes and returns a `std::vector` of additional projections required *before* evaluating the ORDER BY clause.
     * The returned `std::vector` may be empty, in which case *no* additional projection is required. */
    static std::vector<projection_type>
    compute_projections_required_for_order_by(const std::vector<projection_type> &projections,
                                              const std::vector<order_type> &order_by);
};

struct M_EXPORT SemiJoinCostFunction
{
    double estimate_semi_join_probe_costs(const CardinalityEstimator &CE, const DataModel &model);
    double estimate_semi_join_hash_costs(const CardinalityEstimator &CE, const DataModel &model);
    double estimate_semi_join_costs(const CardinalityEstimator &CE, const DataModel &left, const DataModel &right);
};

struct M_EXPORT TreeEnumerator
{
    using cost_table_t = std::vector<std::unordered_map<std::size_t, std::pair<double, bool>>>;
    using model_table_t = std::vector<std::unordered_map<std::size_t, std::unique_ptr<DataModel>>>;
    using semi_join_order_t = SemiJoinReductionOperator::semi_join_order_t;
    using card_order_t = std::vector<std::size_t>;

    private:
    cost_table_t cost_table_;
    model_table_t model_table_;
    public:
    TreeEnumerator(std::size_t n) : cost_table_(n), model_table_(n) {}
    std::pair<std::size_t, double> find_best_root(QueryGraph &G, std::unordered_set<std::size_t> required_reductions, const AdjacencyMatrix& adj_matrix, const CardinalityEstimator &CE, SemiJoinCostFunction &SC, card_order_t card_orders[], std::vector<std::unique_ptr<DataModel>> &base_models);
    void update_cost_table(std::size_t parent, std::size_t child, double costs, bool needs_reduction) {
        cost_table_[parent].emplace(child, std::pair(costs, needs_reduction));
    }

    void compute_cardinality_order(const QueryGraph &G, const AdjacencyMatrix& adj_matrix, const CardinalityEstimator &CE, std::size_t node, std::vector<std::size_t> &card_order, std::vector<std::unique_ptr<DataModel>> &base_models);

    void determine_reduced_models(QueryGraph &G, const AdjacencyMatrix& adj_matrix, const CardinalityEstimator &CE, card_order_t card_orders[], std::vector<std::unique_ptr<DataModel>> &base_models);

    void update_model_table(std::size_t parent, std::size_t child, std::unique_ptr<DataModel> model) {
        model_table_[parent].emplace(child, std::move(model));
    }

    auto parent_node_costs(std::size_t parent, std::size_t child) {
        return cost_table_[parent].find(child);
    }

    auto parent_node_model(std::size_t parent, std::size_t child) {
        return model_table_[parent].find(child);
    }

    auto cost_begin(std::size_t parent) const { return cost_table_[parent].begin(); }
    auto model_begin(std::size_t parent) const { return model_table_[parent].begin(); }
    auto cost_end(std::size_t parent) const { return cost_table_[parent].end(); }
    auto model_end(std::size_t parent) const { return model_table_[parent].end(); }
};

namespace Optimizer_ResultDB_utils {
    using semi_join_order_t = SemiJoinReductionOperator::semi_join_order_t;
    using fold_t = std::unordered_set<std::size_t>;
    using card_order_t =  std::vector<std::size_t>;
    using bc_forest_t = std::unordered_map<Subproblem, std::vector<Subproblem>, SubproblemHash>;

    std::vector<fold_t> compute_folds(const QueryGraph &G);
    DataSource & choose_root_node(QueryGraph &G, SemiJoinReductionOperator &op);
    void fold_query_graph(QueryGraph &G, std::vector<fold_t> &folds);
    void combine_joins(QueryGraph &G);

    std::vector<semi_join_order_t> compute_semi_join_reduction_order(QueryGraph &G, SemiJoinReductionOperator &op);
    std::pair<std::vector<semi_join_order_t>, double> enumerate_semi_join_reduction_order(QueryGraph &G, std::unordered_set<std::size_t> required_reductions, const AdjacencyMatrix& adj_matrix, const CardinalityEstimator &CE, std::vector<std::unique_ptr<DataModel>> &base_models);
    std::pair<std::unique_ptr<Producer*[]>, double>  compute_and_solve_biconnected_components(QueryGraph &G, std::vector<std::unique_ptr<DataModel>> &base_models);
    std::unique_ptr<Producer*[]> solve_cycles_without_enum(QueryGraph &G, std::vector<std::unique_ptr<DataModel>> &base_models);
    void find_vertex_cuts(QueryGraph &G, Subproblem block, Subproblem &already_used, std::vector<fold_t> &folds);
    void find_and_apply_vertex_cuts(QueryGraph &G, std::vector<Subproblem> &blocks, Subproblem &cut_vertices);
    void build_bc_forest(std::vector<Subproblem> &blocks, Subproblem &cut_vertices, bc_forest_t &bc_forest);
    void visit_bc_forest(bc_forest_t &bc_forest, std::vector<Subproblem> &blocks, Subproblem &visited, std::vector<Subproblem> &folding_problems, std::vector<fold_t> &folds);
    template<typename PlanTable>
    void optimize_join_order(const QueryGraph &G, PlanTable &PT, Subproblem folding_problem);
    template<typename PlanTable>
    Producer* construct_join_order(const QueryGraph &G, const PlanTable &PT, Subproblem problem, std::unique_ptr<Producer*[]> &source_plans);
    double optimize(QueryGraph &G, std::vector<Subproblem> &folding_problems, std::unique_ptr<Producer*[]> &source_plans, std::vector<std::unique_ptr<DataModel>> &base_models);
    template<typename PlanTable>
    double optimize_with_plantable(QueryGraph &G, std::vector<Subproblem> &folding_problems, std::unique_ptr<Producer*[]> &source_plans,  std::vector<std::unique_ptr<DataModel>> &base_models);
    template<typename PlanTable>
    std::unique_ptr<Producer*[]> optimize_source_plans(const QueryGraph &G, PlanTable &PT);
    std::pair<std::unique_ptr<Producer>, bool> dp_resultdb(QueryGraph &G);
    template<typename PlanTable>
    std::pair<std::unique_ptr<Producer>, bool> dp_resultdb_with_plantable(QueryGraph &G);
    void get_folds_and_folding_problems(QueryGraph &G, std::vector<Optimizer_ResultDB_utils::fold_t> &folds, std::unordered_set<Subproblem, SubproblemHash> &folding_problems);
    void get_greedy_folds_and_folding_problems(QueryGraph &G, std::vector<Optimizer_ResultDB_utils::fold_t> &folds, std::unordered_map<Subproblem, std::vector<Subproblem>, SubproblemHash> &restricted_folding_problems);
    void create_folded_adjacency_matrix(std::vector<fold_t> folds, AdjacencyMatrix &old_matrix, AdjacencyMatrix& new_matrix, std::vector<Subproblem>& new_to_old_mapping);

}

/** The optimizer interface for SELECT RESULTDB queries.
 *
 * The optimizer constructs an operator tree containing a single `SemiJoinReductionOperator' with the base tables as
 * inputs (children).
 */
struct M_EXPORT Optimizer_ResultDB
{
    using semi_join_order_t = SemiJoinReductionOperator::semi_join_order_t;
    using fold_t = std::unordered_set<std::size_t>;

    public:
    Optimizer_ResultDB() { }

    /** Apply this optimizer to the given query graph to compute an operator tree. It computes and constructs an optimal
     * semi-join reduction plan. In addition to the plan, a boolean value indicating if the underlying query graph is
     * compatible is returned. In case the query is not compatible, the optimizer falls back to the standard `Optimizer`
     * and `false` is returned. */
    std::pair<std::unique_ptr<Producer>, bool> operator()(QueryGraph &G) const;
};

}
