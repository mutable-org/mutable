#include "mutable/util/macro.hpp"
#include <mutable/IR/Optimizer.hpp>

#include <algorithm>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/Options.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/storage/Store.hpp>
#include <numeric>
#include <tuple>
#include <vector>
#include <queue>
#include <unordered_set>


using namespace m;
using namespace m::ast;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

struct WeighExpr
{
    private:
    unsigned weight_ = 0;

    public:
    operator unsigned() const { return weight_; }

    void operator()(const cnf::CNF &cnf) {
        for (auto &clause : cnf)
            (*this)(clause);
    }

    void operator()(const cnf::Clause &clause) {
        for (auto pred : clause)
            (*this)(*pred);
    }

    void operator()(const ast::Expr &e) {
        visit(overloaded {
            [](const ErrorExpr&) { M_unreachable("no errors at this stage"); },
            [this](const Designator &d) {
                if (auto cs = cast<const CharacterSequence>(d.type()))
                    weight_ += cs->length;
                else
                    weight_ += 1;
            },
            [this](const Constant &e) {
                if (auto cs = cast<const CharacterSequence>(e.type()))
                    weight_ += cs->length;
                // fixed-size constants are considered free, as they may be encoded as immediate constants in the instr
            },
            [this](const FnApplicationExpr&) {
                weight_ += 1;
            },
            [this](const UnaryExpr&) { weight_ += 1; },
            [this](const BinaryExpr&) { weight_ += 1; },
            [this](const QueryExpr&) { weight_ += 1000; } // XXX: this should never happen because of unnesting
        }, e, tag<ConstPreOrderExprVisitor>{});
    }
};

std::vector<cnf::CNF> Optimizer::optimize_filter(cnf::CNF filter)
{
    constexpr unsigned MAX_WEIGHT = 12; // equals to 4 comparisons of fixed-length values
    M_insist(not filter.empty());

    /* Compute clause weights. */
    std::vector<unsigned> clause_weights;
    clause_weights.reserve(filter.size());
    for (auto &clause : filter) {
        WeighExpr W;
        W(clause);
        clause_weights.emplace_back(W);
    }

    /* Sort clauses by weight using an index vector. */
    std::vector<std::size_t> order(filter.size(), 0);
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&clause_weights](std::size_t first, std::size_t second) -> bool {
        return clause_weights[first] < clause_weights[second];
    });

    /* Dissect filter into sequence of filters. */
    std::vector<cnf::CNF> optimized_filters;
    unsigned current_weight = 0;
    cnf::CNF current_filter;
    for (std::size_t i = 0, end = filter.size(); i != end; ++i) {
        const std::size_t idx = order[i];
        cnf::Clause clause(std::move(filter[idx]));
        M_insist(not clause.empty());
        const unsigned clause_weight = clause_weights[idx];

        if (not current_filter.empty() and current_weight + clause_weight > MAX_WEIGHT) {
            optimized_filters.emplace_back(std::move(current_filter)); // empties current_filter
            current_weight = 0;
        }

        current_filter.emplace_back(std::move(clause));
        current_weight += clause_weight;
    }
    if (not current_filter.empty())
        optimized_filters.emplace_back(std::move(current_filter));

    M_insist(not optimized_filters.empty());
    return optimized_filters;
}

std::vector<Optimizer::projection_type>
Optimizer::compute_projections_required_for_order_by(const std::vector<projection_type> &projections,
                                                     const std::vector<order_type> &order_by)
{
    std::vector<Optimizer::projection_type> required_projections;

    /* Collect all required `Designator`s which are not included in the projection. */
    auto get_required_designator = overloaded {
        [&](const ast::Designator &d) -> void {
            if (auto t = std::get_if<const Expr*>(&d.target())) { // refers to another expression?
                /*----- Find `t` in projections. -----*/
                for (auto &[expr, _] : projections) {
                    if (*t == &expr.get())
                        return; // found
                }
            }
            /*----- Find `d` in projections. -----*/
            for (auto &[expr, alias] : projections) {
                if (not alias.has_value() and d == expr.get())
                    return; // found
            }
            required_projections.emplace_back(d, ThreadSafePooledOptionalString{});
        },
        [&](const ast::FnApplicationExpr &fn) -> void {
            /*----- Find `fn` in projections. -----*/
            for (auto &[expr, alias] : projections) {
                if (not alias.has_value() and fn == expr.get())
                    throw visit_skip_subtree(); // found
            }
            required_projections.emplace_back(fn, ThreadSafePooledOptionalString{});
            throw visit_skip_subtree();
        },
        [](auto&&) -> void { /* nothing to be done */ },
    };
    /* Process the ORDER BY clause. */
    for (auto [expr, _] : order_by)
        visit(get_required_designator, expr.get(), m::tag<m::ast::ConstPreOrderExprVisitor>());

    return required_projections;
}


/*======================================================================================================================
 * Optimizer_ResultDB
 *==============================================================    return PT.======================================================*/

double SemiJoinCostFunction::estimate_semi_join_costs(const CardinalityEstimator &CE, const DataModel &left, const DataModel &right)
{
    return estimate_semi_join_hash_costs(CE, right) + estimate_semi_join_probe_costs(CE, left);
}

double SemiJoinCostFunction::estimate_semi_join_probe_costs(const CardinalityEstimator &CE, const DataModel &model)
{
    return double(CE.predict_cardinality(model));
}

double SemiJoinCostFunction::estimate_semi_join_hash_costs(const CardinalityEstimator &CE, const DataModel &model)
{
    return 2 * double(CE.predict_cardinality(model));
}

void TreeEnumerator::determine_reduced_models(QueryGraph &G, const AdjacencyMatrix& adj_matrix, const CardinalityEstimator &CE, card_order_t card_orders[], std::vector<std::unique_ptr<DataModel>> &base_models)
{
    auto rec = [&](std::size_t parent, std::size_t node, auto&& rec) {
        /* Check whether the current parent/subtree pair was already evaluated at some point */
        if (auto it = parent_node_model(parent, node); it != model_end(parent)) {
            return it;
        }

        auto node_model = CE.copy(*base_models[node]);

        /* Account for bottom-up and top-down semi-joins */
        for (std::size_t child : adj_matrix.neighbors(Subproblem::Singleton(node))) {
            /* Ignore parent */
            if (child == parent) continue;
            auto it_model = rec(node, child, rec);
            node_model = CE.estimate_semi_join(G, *node_model, *it_model->second,  {});
        }

        update_model_table(parent, node, std::move(node_model));
        return parent_node_model(parent, node);

    };

    /* Enumerate trough each possible root node */
    for (auto &root : G.sources()) {
        rec(root->id(), root->id(), rec);
        compute_cardinality_order(G, adj_matrix, CE, root->id(), card_orders[root->id()], base_models);
    }

}

void TreeEnumerator::compute_cardinality_order(const QueryGraph &G, const AdjacencyMatrix& adj_matrix, const CardinalityEstimator &CE, std::size_t node, std::vector<std::size_t> &card_order, std::vector<std::unique_ptr<DataModel>> &base_models)
{
    std::unordered_map<size_t, size_t> cardinalities;
    auto node_problem = Subproblem::Singleton(node);
    auto node_model = CE.copy(*base_models[node]);
    /* Add all neighbors to the vector */
    for (auto neighbor : adj_matrix.neighbors(node_problem)) {
        card_order.emplace_back(neighbor);
        auto neighbor_model = parent_node_model(node, neighbor);
        auto neighbor_cardinality = CE.predict_cardinality(*(CE.estimate_semi_join(G, *node_model, *neighbor_model->second, {})));
        cardinalities[neighbor] = neighbor_cardinality;
    }

    auto cmp = [&cardinalities](std::size_t a, std::size_t b){
        return cardinalities[a] < cardinalities[b];
    };
    std::sort(card_order.begin(), card_order.end(), cmp);
}

std::pair<std::size_t, double> TreeEnumerator::find_best_root(QueryGraph &G, std::unordered_set<std::size_t> required_reductions, const AdjacencyMatrix& adj_matrix, const CardinalityEstimator &CE, SemiJoinCostFunction &SJ, card_order_t card_orders[], std::vector<std::unique_ptr<DataModel>> &base_models)
{
    std::vector<semi_join_order_t> semi_join_reduction_order;
    double lowest_costs = std::numeric_limits<double>::infinity();
    std::unordered_map<std::size_t, std::unordered_map<std::size_t, std::unique_ptr<DataModel>>> reduced_models;
    std::size_t best_root = 0; // First node

    determine_reduced_models(G, adj_matrix, CE, card_orders, base_models);

    auto estimate_subtree_costs = [&](std::size_t parent, std::size_t node, auto&& estimate_subtree_costs) {

        /* Check whether the current parent/subtree pair was already evaluated at some point */
        if (auto it = parent_node_costs(parent, node); it != cost_end(parent)) {
            return it;
        }

        auto node_model = CE.copy(*base_models[node]);
        auto node_fully_reduced_model = CE.estimate_full_reduction(G, *node_model);
        bool reduction_required = required_reductions.contains(node);
        double costs = 0;

        /* Account for bottom-up and top-down semi-joins */
        for (std::size_t child : card_orders[node]) {
            /* Ignore parent */
            if (child == parent) continue;
            /* Recursively evaluate the child subtree */
            auto it_costs = estimate_subtree_costs(node, child, estimate_subtree_costs);
            auto it_model = parent_node_model(node, child);
            costs += it_costs->second.first + // Recursive Tree costs
                     SJ.estimate_semi_join_costs(CE, *node_model, *it_model->second); // Bottom-up Semi-join costs between node and child
            node_model = CE.estimate_semi_join(G, *node_model, *it_model->second, {});
            if (it_costs->second.second && parent != node) {
                // Top-down Semi-join costs between child and node
                costs += SJ.estimate_semi_join_costs(CE, *it_model->second, *node_fully_reduced_model);
                reduction_required = true;
            }
        }

        update_cost_table(parent, node, costs, reduction_required);
        return parent_node_costs(parent, node);

    };

    /* Enumerate trough each possible root node to find the best one */
    for (auto &root : G.sources()) {

        /* Folded node id */
        if (card_orders[root->id()].empty()) {
            continue;
        }

        auto it = estimate_subtree_costs(root->id(), root->id(), estimate_subtree_costs);
        if (it->second.first < lowest_costs) {
            best_root = root->id();
            lowest_costs = it->second.first;
        }
    }

    return std::make_pair(best_root, lowest_costs);
}

DataSource & Optimizer_ResultDB_utils::choose_root_node(QueryGraph &G, SemiJoinReductionOperator &op)
{
    const Schema &S = op.schema();
    std::size_t current_root_idx = -1UL;
    for (std::size_t idx = 0; idx < G.sources().size(); ++idx) {
        auto intersection = S & op.child(idx)->schema();
        if (not intersection.empty()) { // contained in projections
            if (current_root_idx == -1UL or (G.sources()[idx]->joins().size() > G.sources()[current_root_idx]->joins().size()))
                current_root_idx = idx;
        }
    }
    if (current_root_idx == -1UL) { // no relation found that is part of the projections (likely SELECT *)
        current_root_idx = 0;
        for (std::size_t idx = 1; idx < G.sources().size(); ++idx) { // fallback to highest degree
            if (G.sources()[idx]->joins().size() > G.sources()[current_root_idx]->joins().size())
                current_root_idx = idx;
        }
    }
    return *G.sources()[current_root_idx];
}


/** Identifies a set of joins that have to be computed such that the join graph becomes acyclic. Currently, the
 * implementation heuristically chooses the node `x` with the highest degree first and subsequently, identifies the node
 * `y` with the highest degree from the neighbors of `x`. The rationale behind this is that nodes with a high degree are
 * more likely to be part of a cycle.
 *
 * TODO: Instead of repeatedly choosing two nodes heuristically and checking if the resulting graph is acyclic, we can
 * use Tarjans bridge-finding algorithm:
 * https://en.wikipedia.org/wiki/Bridge_%28graph_theory%29#Tarjan's_bridge-finding_algorithm
 * Using this, we can successively remove bridges (i.e. joins that are not part of a cycle) and with that, narrow down
 * the set of relations such that we know that the resulting nodes are part a cycle.
 * Note, the query graph could only have joins that are part of a cycle, i.e. the algorithm would not be able to remove
 * any edges. In this case, it might still matter which folds are computed.
 */
std::vector<Optimizer_ResultDB::fold_t> Optimizer_ResultDB_utils::compute_folds(const QueryGraph &G)
{
    M_insist(G.is_cyclic(), "join graph must be cyclic");
    M_insist(G.num_sources() > 2, "join graph with two or less data sources cannot be cyclic");

    /*----- Get the ids of the data sources in `G` and put them in individual sets. -----*/
    std::vector<fold_t> folds;
    for (auto &ds : G.sources())
        folds.push_back({ds->id()});

    /*----- Compute the folds of the query graph. -----*/
    AdjacencyMatrix mat(G.adjacency_matrix()); // copy the query graphs adjacency matrix
    do {
        /*----- Compute `x` and `y` using the adjacency matrix. -----*/
        std::size_t x_id = mat.highest_degree_node(SmallBitset::All(folds.size()));
        std::size_t y_id = mat.highest_degree_node(mat.neighbors(SmallBitset::Singleton(x_id)));
        if (y_id < x_id)
            std::swap(x_id, y_id); // `x_id` contains the smaller id

        /*----- Merge the two folds. -----*/
        folds[x_id].merge(folds[y_id]);
        folds.erase(folds.begin() + y_id);

        mat = mat.merge_nodes(x_id, y_id); // merge the two nodes of the adjacency matrix
    } while (mat.is_cyclic());

    return folds;
}


/** Modify the `QueryGraph` based on the folds. Concretely, the data sources that are part of a fold are put together in
 * a (nested) query and the joins are adapted accordingly. */
void Optimizer_ResultDB_utils::fold_query_graph(QueryGraph &G, std::vector<fold_t> &folds)
{
    auto &C = Catalog::Get();

    /* Retrieve and reset data sources and joins in `G`. */
    const auto joins = std::exchange(G.joins(), std::vector<std::unique_ptr<Join>>());
    const auto sources = std::exchange(G.sources(), std::vector<std::unique_ptr<DataSource>>());

    /* Create a new `BaseTable` or `Query` with the same information as the data source at position `ds_id_in_G` in
     * `sources` and add it to the query graph `G`. Returns the id of the newly inserted data source. */
    auto add_ds_to_query_graph = [&sources](QueryGraph &G, std::size_t ds_id_in_G) -> std::size_t {
        auto &ds = sources[ds_id_in_G];
        auto new_id = G.sources().size();
        if (auto bt = cast<BaseTable>(ds.get())) {
            auto &new_bt = G.add_source(bt->name(), bt->table());
            new_bt.update_filter(std::move(bt->filter()));
        } else {
            auto &Q = as<Query>(*ds);
            auto &new_Q = G.add_source(Q.name(), Q.extract_query_graph());
            new_Q.update_filter(std::move(Q.filter()));
        }
        return new_id;
    };

    /* The `mod_nested_id` data structure stores the following information:
     *      (data source id in modified `G`, data source id in `G_nested` of modified `G`)
     * The index into the array corresponds to the id of the original data source in `G`.
     * The second value does not hold any meaning for base tables. */
    std::pair<std::size_t, std::size_t> ds2mod_nested_id[sources.size()];
    /*----- Create new data sources for the modified query graph. -----*/
    for (auto fold : folds) {
        if (fold.size() == 1) { // base table
            auto id = *fold.begin();
            auto ds_id = add_ds_to_query_graph(G, id);
            ds2mod_nested_id[id] = std::make_pair(ds_id, 0);
        } else { // nested query
            /*----- Create new `QueryGraph` and add as `DataSource` to `G`. -----*/
            auto G_nested = std::make_unique<QueryGraph>();
            G_nested->transaction(G.transaction()); // nested query requires same transaction ID
            auto ds_id_in_G_mod = G.sources().size();
            std::ostringstream oss;
            oss << '$';
            std::size_t count = 0;
            for (auto ds_id_in_G : fold) { // add data sources
                if (count != 0)
                    oss << '_';
                oss << sources[ds_id_in_G]->name();
                auto ds_id_in_G_nested = add_ds_to_query_graph(*G_nested, ds_id_in_G);
                ds2mod_nested_id[ds_id_in_G] = std::make_pair(ds_id_in_G_mod, ds_id_in_G_nested);
                ++count;
            }
            G.add_source(C.pool(oss.str().c_str()), std::move(G_nested));
        }
    }

    /*----- Create joins between the new data sources in `G`. -----*/
    for (auto &join : joins) {
        M_insist(join->sources().size() == 2);
        /*----- Create new `Join` and add to corresponding data source. -----*/
        auto G_lhs_id = join->sources()[0].get().id();
        auto G_rhs_id = join->sources()[1].get().id();

        auto [mod_lhs_id, nested_lhs_id] = ds2mod_nested_id[G_lhs_id];
        auto [mod_rhs_id, nested_rhs_id] = ds2mod_nested_id[G_rhs_id];
        auto &mod_lhs_ds = *G.sources()[mod_lhs_id];
        auto &mod_rhs_ds = *G.sources()[mod_rhs_id];

        auto add_join = [&join](QueryGraph &G, DataSource &lhs_ds, DataSource &rhs_ds) {
            /*----- Create new joins sources. -----*/
            Join::sources_t new_join_sources;
            new_join_sources.push_back(lhs_ds);
            new_join_sources.push_back(rhs_ds);

            /*----- Construct new join and add to `G`. -----*/
            auto &new_join = G.emplace_join(std::move(join->condition()), std::move(new_join_sources));

            /*----- Add new join to its respective data sources. -----*/
            lhs_ds.add_join(new_join);
            rhs_ds.add_join(new_join);
        };

        if (mod_lhs_id == mod_rhs_id) { // both data sources have the same `id` in `G` -> join inside nested query
            auto &Q_nested = as<Query>(mod_lhs_ds);
            auto &G_nested = Q_nested.query_graph();

            auto &nested_lhs = *G_nested.sources()[nested_lhs_id];
            auto &nested_rhs = *G_nested.sources()[nested_rhs_id];
            Join::sources_t nested_sources;
            nested_sources.push_back(nested_lhs);
            nested_sources.push_back(nested_rhs);
            add_join(G_nested, nested_lhs, nested_rhs);
            continue;
        }

        /*----- Check if this join already exists in `G`. -----*/
        auto it = std::find_if(G.joins().begin(), G.joins().end(), [&mod_lhs_ds, &mod_rhs_ds](auto &j){
            M_insist(j->sources().size() == 2);
            return (j->sources()[0].get() == mod_lhs_ds and j->sources()[1].get() == mod_rhs_ds) or
                   (j->sources()[1].get() == mod_lhs_ds and j->sources()[0].get() == mod_rhs_ds);
        });

        if (it != G.joins().end()) // already in `G` -> update condition
            (*it)->update_condition(std::move(join->condition()));
        else // construct new join and add to `G`
            add_join(G, mod_lhs_ds, mod_rhs_ds);
    }
}

/** If the `QueryGraph` contains multiple joins between two specific data sources, combine them into *one* join that
 * concatenates the individual conditions using a logical AND operation. */
void Optimizer_ResultDB_utils::combine_joins(QueryGraph &G)
{
    std::vector<std::unique_ptr<Join>> modified_joins;
    for (auto &j : G.joins()) {
        auto it = std::find_if(modified_joins.cbegin(), modified_joins.cend(), [&j](auto &mod_j){
            M_insist(j->sources().size() == 2);
            M_insist(mod_j->sources().size() == 2);
            return (j->sources()[0].get() == mod_j->sources()[0] and j->sources()[1].get() == mod_j->sources()[1]) or
                   (j->sources()[1].get() == mod_j->sources()[0] and j->sources()[0].get() == mod_j->sources()[1]);
        });
        if (it != modified_joins.cend())
            (*it)->update_condition(j->condition()); // `j` is already in `modified_joins` -> update condition
        else
            modified_joins.push_back(std::move(j)); // `j` is not in `modified_joins` -> add
    }
    G.joins() = std::move(modified_joins);
}

std::vector<Optimizer_ResultDB::semi_join_order_t>
Optimizer_ResultDB_utils::compute_semi_join_reduction_order(QueryGraph &G, SemiJoinReductionOperator &op)
{
    std::vector<semi_join_order_t> semi_join_reduction_order;

    /*----- Choose root node that is part of the projections and has the highest degree. -----*/
    auto &root = choose_root_node(G, op);

    /*----- Compute BFS ordering starting at `root`. -----*/
    auto &mat = G.adjacency_matrix();
    std::unordered_set<std::reference_wrapper<DataSource>, DataSourceHash, DataSourceEqualTo> visited;
    std::queue<std::reference_wrapper<DataSource>> Q;
    std::vector<std::reference_wrapper<DataSource>> BFS_ordering;
    Q.push(root);

    while (not Q.empty()) {
        auto x = Q.front();
        Q.pop();
        BFS_ordering.push_back(x);
        visited.insert(x);
        auto neighbors = mat.neighbors(SmallBitset::Singleton(x.get().id()));
        for (auto n_id : neighbors) {
            auto &n = *G.sources()[n_id];
            if (visited.contains(n))
                continue;
            Q.push(n);
        }
    }

    /*----- Use the BFS ordering of the data sources to construct the order in which the semi-joins are applied. -----*/
    std::unordered_set<std::reference_wrapper<Join>, JoinHash, JoinEqualTo> handled_joins;
    for (auto ds : BFS_ordering) {
        for (auto j : ds.get().joins()) {
            if (not handled_joins.contains(j)) {
                /*----- Check which join source (lhs or rhs) contains the current `ds` (closer to the root). -----*/
                using ds_it_t = decltype(j.get().sources().begin());
                auto [lhs, rhs] = [&j, &ds]() -> std::pair<ds_it_t, ds_it_t> {
                    M_insist(j.get().sources().size() == 2);
                    auto lhs = j.get().sources().begin();
                    auto rhs = std::next(lhs);
                    if (lhs->get() == ds.get())
                        return { lhs, rhs };
                    else
                        return { rhs, lhs };
                }();
                semi_join_reduction_order.emplace_back(*lhs, *rhs);
                handled_joins.insert(j);
            }
        }
    }
    return semi_join_reduction_order;
}

std::pair<std::vector<Optimizer_ResultDB::semi_join_order_t>, double>
Optimizer_ResultDB_utils::enumerate_semi_join_reduction_order(QueryGraph &G, std::unordered_set<std::size_t> required_reductions, const AdjacencyMatrix& adj_matrix, const CardinalityEstimator &CE, std::vector<std::unique_ptr<DataModel>> &base_models)
{
    auto &C = Catalog::Get();
    auto SJ = SemiJoinCostFunction();

    /* Contains for each possible data source (id) the best order in which to apply
     * adjacent semi-joins */
    card_order_t card_orders[G.num_sources()];
    
    auto tree_enumerator = TreeEnumerator(G.num_sources());

    auto [best_root, costs] = tree_enumerator.find_best_root(G, required_reductions, adj_matrix, CE, SJ, card_orders, base_models);
    std::vector<Optimizer_ResultDB::semi_join_order_t> semi_join_reduction_order;

    /* Create semi-join order */
    auto construct_semi_join_order = [&](std::size_t parent, std::size_t node, auto&& construct_semi_join_order) -> void {
        /* Join with neighbors */
        for (auto it = card_orders[node].rbegin(); it != card_orders[node].rend(); it++) {
            if (parent == *it) continue;
            semi_join_reduction_order.emplace_back(G[node], G[*it]);
        }

        /* Recursive descent */
        for (auto it = card_orders[node].rbegin(); it != card_orders[node].rend(); it++) {
            if (parent == *it) continue;
            construct_semi_join_order(node, *it, construct_semi_join_order);
        }
    };

    construct_semi_join_order(best_root, best_root, construct_semi_join_order);

    return std::make_pair(semi_join_reduction_order, costs);

}

void Optimizer_ResultDB_utils::find_vertex_cuts(QueryGraph &G, Subproblem block, Subproblem &already_used, std::vector<fold_t> &folds) {

    M_insist(block.size() > 2, "Block must contain at least 3 nodes!");

    auto &M = G.adjacency_matrix();

    std::vector<Subproblem> vertex_cuts;
    M.find_two_vertex_cuts(vertex_cuts, block, already_used);
    std::unordered_map<std::size_t, std::size_t> node_counts;

    /* Check for overlaps between different vertex cut pairs, and sort them according to the lowest overlap count */
    for (Subproblem pair : vertex_cuts) {
       for (size_t pair_node : pair) {
           if (node_counts.contains(pair_node)) {
               node_counts[pair_node] += 1;
           } else {
               node_counts.emplace(pair_node, 0);
           }
       }
    }

    std::unordered_map<Subproblem, std::size_t, SubproblemHash> cut_overlaps;
    for (Subproblem pair : vertex_cuts) {
        cut_overlaps.emplace(pair, 0);
        for (size_t pair_node : pair) {
            cut_overlaps[pair] += node_counts[pair_node];
        }
    }

    /* Use Counting Sort */
    std::vector<std::size_t> helper(vertex_cuts.size(), 0);
    std::vector<Subproblem> sorted_vertex_cuts(vertex_cuts.size());

    for (size_t i = 0; i < vertex_cuts.size(); i++) {
        helper[cut_overlaps[vertex_cuts[i]]] += 1;
    }

    for (size_t i = 1; i < helper.size(); i++) {
        helper[i] += helper[i-1];
    }

    for (int i = vertex_cuts.size() - 1; i >= 0; i--) {
        sorted_vertex_cuts[helper[cut_overlaps[vertex_cuts[i]]] - 1] = vertex_cuts[i];
        helper[cut_overlaps[vertex_cuts[i]]] -= 1;
    }

    for (Subproblem pair : sorted_vertex_cuts) {
        /* One of the pairs might already be used earlier due to overlaps */
        if ((pair & already_used).empty()) {
            std::unordered_set<std::size_t> fold;
            for (std::size_t pair_node : pair) fold.emplace(pair_node);
            folds.emplace_back(fold);
            already_used |= pair; // no longer use that pair for other greedy joins
        }
    }
}

void Optimizer_ResultDB_utils::find_and_apply_vertex_cuts(QueryGraph &G, std::vector<Subproblem> &blocks, Subproblem &cut_vertices) {

    /* Go through each block and greedily apply 2-vertex cuts to "maximally" reduce the number of joins one has to compute
    * in order to remove the cycle. Naturally, one could also search for n-vertex cuts, which are highly unlikely to appear */
    Subproblem already_used = Subproblem();
    std::vector<fold_t> folds;
    for (auto block: blocks) {
        find_vertex_cuts(G, block, already_used, folds);
    }

    if (not already_used.empty()) {
        /* Also add unaffected sources to the folds */
        for (auto node_id : Subproblem::All(G.num_sources()) - already_used) {
            std::unordered_set<std::size_t> fold;
            fold.emplace(node_id);
            folds.emplace_back(fold);
        }
        /* Fold join graph according to two-vertex cuts */
        fold_query_graph(G, folds);
        blocks = std::vector<Subproblem>(0);
        cut_vertices = Subproblem();

        /* Get update adjacency matrix */
        auto &M = G.adjacency_matrix();
        M.compute_blocks_and_cut_vertices(blocks, cut_vertices, 3);
    }
}

void Optimizer_ResultDB_utils::build_bc_forest(std::vector<Subproblem> &blocks, Subproblem &cut_vertices, bc_forest_t &bc_forest) {
    auto add_edge_to_forest = [&bc_forest](Subproblem left, Subproblem right) {
        auto it = bc_forest.find(left);
        if (it == bc_forest.end()) {
            it = bc_forest.try_emplace(left).first;
        }
        it->second.emplace_back(right);
    };

    for (auto block: blocks) {
        Subproblem block_cut_vertices = cut_vertices & block;

        /* Add an edge between the block node and the cut vertex node */
        for (auto node_id: block_cut_vertices) {
            auto node_problem = Subproblem::Singleton(node_id);
            /* Undirected graph */
            add_edge_to_forest(block, node_problem);
            add_edge_to_forest(node_problem, block);
        }

    }
}

void Optimizer_ResultDB_utils::visit_bc_forest(bc_forest_t &bc_forest, std::vector<Subproblem> &blocks, Subproblem &visited, std::vector<Subproblem> &folding_problems, std::vector<fold_t> &folds)
{
    auto rec = [&](Subproblem current_node, Subproblem parent, auto&& rec) -> void {
        /* Go through each child and visit recursively */
        if (current_node.size() == 1) {
            for (auto child: bc_forest[current_node]) {
                if (child == parent) continue;
                visited |= child;
                fold_t child_fold;
                for (auto graph_node: child - current_node) {
                    child_fold.emplace(graph_node);
                }
                folds.emplace_back(child_fold);
                rec(child, current_node, rec);
            }
            return;
        }
        for (auto child: bc_forest[current_node]) {
            if (child == parent) continue;
            visited |= child;
            rec(child, current_node, rec);
        }
    };

    /* We might have multiple roots due to the block cut forest consisting of multiple trees */
    for (auto block: blocks) {
        if (not (visited & block).empty()) continue;
        visited |= block;
        folding_problems.emplace_back(block);
        rec(block, block, rec);
    }
}

void Optimizer_ResultDB_utils::get_folds_and_folding_problems(QueryGraph &G, std::vector<Optimizer_ResultDB_utils::fold_t> &folds, std::unordered_set<Subproblem, SubproblemHash> &folding_problems) {
    std::vector<Subproblem> blocks;
    Subproblem cut_vertices = Subproblem();
    auto &M = G.adjacency_matrix();

    M.compute_blocks_and_cut_vertices(blocks, cut_vertices, 3);

    /* Use a block-cut-forest to assign each cut vertex to exactly one of its biconnected components */
    bc_forest_t bc_forest;
    build_bc_forest(blocks, cut_vertices, bc_forest);

    std::vector<Subproblem> folding_subproblems;
    /* Define visitor to go through each node within the tree to assign the vertex cuts */
    Subproblem visited;

    /* Sort the blocks by the number of relations they contain */
    auto cmp = [](Subproblem left, Subproblem right) {
        return left.size() > right.size();
    };
    std::sort(blocks.begin(), blocks.end(), cmp);

    std::vector<fold_t> complete_join_folds;
    /* We might have multiple roots due to the block cut forest consisting of multiple trees */
    visit_bc_forest(bc_forest, blocks, visited, folding_subproblems, folds);

    Subproblem already_used = Subproblem();
    for (auto folding_subproblem: folding_subproblems) {
        folding_problems.emplace(folding_subproblem);
        already_used |= folding_subproblem;
    }

    /* We also need to add the existing base tables that are not part of any fold to the fold */
    for (auto node_id: Subproblem::All(G.num_sources()) - visited) {
        fold_t fold = {node_id};
        folds.emplace_back(fold);
    }
}

void Optimizer_ResultDB_utils::create_folded_adjacency_matrix(std::vector<fold_t> folds, AdjacencyMatrix &old_matrix, AdjacencyMatrix& new_matrix, std::vector<Subproblem>& new_to_old_mapping) {
    std::vector<std::size_t> pairs(old_matrix.size(), 0);
    Subproblem folded_nodes = Subproblem();
    for (auto fold: folds) {
        std::vector<std::size_t> node_ids;
        std::size_t reference_node = *fold.begin();
        Subproblem fold_problem;
        for (auto it = fold.begin(); it != fold.end(); it++) {
            pairs[*it] = reference_node;
            fold_problem |= Subproblem::Singleton(*it);
        }
        new_to_old_mapping[reference_node] = fold_problem;
        folded_nodes |= fold_problem;
    }
    /* Also add unaffected sources to the new adjacency matrix */
    for (auto node_id : Subproblem::All(old_matrix.size()) - folded_nodes) {
        new_to_old_mapping[node_id] = Subproblem::Singleton(node_id);
        pairs[node_id] = node_id;
    }
    /* Create matrix */
    for (auto node_id: folded_nodes) {
        /* It was decided that this node was removed, only required case */
        if (pairs[node_id] != node_id) {
            new_matrix[node_id] = Subproblem(0);
            auto node_problem = Subproblem::Singleton(node_id);
            auto node_partner_problem = Subproblem::Singleton(pairs[node_id]);
            for (auto neighbor_id: old_matrix[node_id]) {
                // Only update unaffected nodes
                if (neighbor_id == pairs[neighbor_id]) {
                    // Reference node must not be to be included in itself
                    if (neighbor_id != pairs[node_id]) new_matrix[neighbor_id] = (old_matrix[neighbor_id] - node_problem) | node_partner_problem;
                    else new_matrix[neighbor_id] = (old_matrix[neighbor_id] - node_problem);
                }
            }
        }
    }
}

void Optimizer_ResultDB_utils::get_greedy_folds_and_folding_problems(QueryGraph &G, std::vector<Optimizer_ResultDB_utils::fold_t> &folds, std::unordered_map<Subproblem, std::vector<Subproblem>, SubproblemHash> &restricted_folding_problems) {
    std::vector<Subproblem> blocks;
    Subproblem cut_vertices = Subproblem();
    auto &M = G.adjacency_matrix();

    M.compute_blocks_and_cut_vertices(blocks, cut_vertices, 3);

    Subproblem already_used = Subproblem();
    std::vector<fold_t> cut_folds;
    for (auto block: blocks) {
        find_vertex_cuts(G, block, already_used, cut_folds);
    }
    /* When no nodes have been folded, there is no difference compared to DP_Fold */
    if (already_used.empty()) return;

    // Use old matrix as default for new matrix!
    AdjacencyMatrix greedy_matrix(M);
    already_used = Subproblem();
    std::vector<Subproblem> new_to_old_mapping(G.num_sources(), Subproblem(0));
    /* Fold Adcaceny Matrix */
    create_folded_adjacency_matrix(cut_folds, M, greedy_matrix, new_to_old_mapping);
    /* Reset old blocks */
    blocks = std::vector<Subproblem>();
    cut_vertices = Subproblem();
    /* Compute new blocks */
    greedy_matrix.compute_blocks_and_cut_vertices(blocks, cut_vertices, 3);

    /* Use a block-cut-forest to assign each cut vertex to exactly one of its biconnected components */
    bc_forest_t bc_forest;
    build_bc_forest(blocks, cut_vertices, bc_forest);

    std::vector<Subproblem> folding_subproblems;
    /* Define visitor to go through each node within the tree to assign the vertex cuts */
    Subproblem visited;

    /* Sort the blocks by the number of relations they contain */
    auto cmp = [](Subproblem left, Subproblem right) {
        return left.size() > right.size();
    };
    std::sort(blocks.begin(), blocks.end(), cmp);

    /* We might have multiple roots due to the block cut forest consisting of multiple trees */
    visit_bc_forest(bc_forest, blocks, visited, folding_subproblems, folds);

    /* The folds might contain two-vertex cut folds, and thus only node, which we need to fix */
    for (auto fold: folds) {
        for (auto node_id: fold) {
            Subproblem old_problem = new_to_old_mapping[node_id];
            for (auto old_node_id: old_problem) {
                fold.emplace(old_node_id);
            }
            already_used |= old_problem;
        }
    }

    /* Create final block fold enumeration problems */
    for (size_t i = 0; i < folding_subproblems.size(); i++) {
        std::vector<Subproblem> conditions;
        for (auto node_id: folding_subproblems[i]) {
            auto old_problem = new_to_old_mapping[node_id];
            auto node_problem = Subproblem::Singleton(node_id);
            if (old_problem != node_problem) {
                conditions.emplace_back(old_problem);
                folding_subproblems[i] |= old_problem;
            }
        }
        restricted_folding_problems.emplace(folding_subproblems[i], conditions);
        already_used |= folding_subproblems[i];
    }

    /* We also need to add the existing base tables that are not part of any fold to the fold */
    for (auto node_id: Subproblem::All(G.num_sources()) - already_used) {
        fold_t fold;
        Subproblem old_problem = new_to_old_mapping[node_id];
        if (old_problem.empty()) continue;
        for (auto old_node_id: old_problem) {
            fold.emplace(old_node_id);
        }
        folds.emplace_back(fold);
    }
}

std::pair<std::unique_ptr<Producer*[]>, double> Optimizer_ResultDB_utils::compute_and_solve_biconnected_components(QueryGraph &G, std::vector<std::unique_ptr<DataModel>> &base_models)
{
    std::vector<Subproblem> blocks;
    Subproblem cut_vertices = Subproblem();
    auto &M = G.adjacency_matrix();

    M.compute_blocks_and_cut_vertices(blocks, cut_vertices, 3);

    if (Options::Get().greedy_cuts) {
        find_and_apply_vertex_cuts(G, blocks, cut_vertices);
    }

    /* Use a block-cut-forest to assign each cut vertex to exactly one of its biconnected components */
    bc_forest_t bc_forest;
    build_bc_forest(blocks, cut_vertices, bc_forest);

    std::vector<Subproblem> folding_problems;
    /* Define visitor to go through each node within the tree to assign the vertex cuts */
    Subproblem visited;

    /* Sort the blocks by the number of relations they contain */
    auto cmp = [](Subproblem left, Subproblem right) {
        return left.size() > right.size();
    };
    std::sort(blocks.begin(), blocks.end(), cmp);

    std::vector<fold_t> complete_join_folds;
    /* We might have multiple roots due to the block cut forest consisting of multiple trees */
    visit_bc_forest(bc_forest, blocks, visited, folding_problems, complete_join_folds);

    std::vector<fold_t> intermediate_folds;
    std::vector<Subproblem> updated_folding_problems;
    auto count = 0;
    for (auto problem: folding_problems) {
        Subproblem new_problem;
        for (auto node_id: problem) {
            new_problem |= Subproblem::Singleton(count);
            count++;
            intermediate_folds.emplace_back(fold_t({node_id}));
        }
        updated_folding_problems.emplace_back(new_problem);
    }

    intermediate_folds.insert(
            intermediate_folds.end(),
            std::make_move_iterator(complete_join_folds.begin()),
            std::make_move_iterator(complete_join_folds.end())
    );


    auto singletons = Subproblem::All(G.num_sources()) - visited;

    for (auto node_id: singletons) {
        intermediate_folds.emplace_back(fold_t({node_id}));
    }
    fold_query_graph(G, intermediate_folds);

    std::size_t rel_no = singletons.size() + complete_join_folds.size() + 2 * folding_problems.size();
    auto final_source_plans = std::make_unique<Producer*[]>(rel_no);
    auto costs = optimize(G, updated_folding_problems, final_source_plans, base_models);

    std::vector<fold_t> final_folds;

    /* Add sources that are not part of any block to the folds */
    for (std::size_t id = 0; id < rel_no; id++) {
        std::unordered_set<std::size_t> fold;
        auto fold_problem = final_source_plans[id]->info().subproblem;
        for (auto problem_id : fold_problem) {
            fold.emplace(problem_id);
        }
        final_folds.emplace_back(fold);
    }

    fold_query_graph(G, final_folds);
    return std::make_pair(std::move(final_source_plans), costs);
}

double Optimizer_ResultDB_utils::optimize(QueryGraph &G, std::vector<Subproblem> &folding_problems, std::unique_ptr<Producer*[]> &source_plans, std::vector<std::unique_ptr<DataModel>> &base_models)
{
    switch (Options::Get().plan_table_type)
    {
        case Options::PT_auto: {
            /* Select most suitable type of plan table depending on the query graph structure.
             * Currently a simple heuristic based on the number of data sources.
             * TODO: Consider join edges too.  Eventually consider #CSGs. */
            if (G.num_sources() <= 15) {
                return optimize_with_plantable<PlanTableSmallOrDense>(G, folding_problems, source_plans, base_models);
            } else {
                return optimize_with_plantable<PlanTableLargeAndSparse>(G, folding_problems, source_plans, base_models);
            }
        }

        case Options::PT_SmallOrDense: {
            return optimize_with_plantable<PlanTableSmallOrDense>(G, folding_problems, source_plans, base_models);
        }

        case Options::PT_LargeAndSparse: {
            return optimize_with_plantable<PlanTableLargeAndSparse>(G, folding_problems, source_plans, base_models);
        }
    }
}

template<typename PlanTable>
double Optimizer_ResultDB_utils::optimize_with_plantable(QueryGraph &G, std::vector<Subproblem> &folding_problems, std::unique_ptr<Producer*[]> &source_plans, std::vector<std::unique_ptr<DataModel>> &base_models)
{
    PlanTable PT(G);
    auto &C = Catalog::Get();
    auto &CE = C.get_database_in_use().cardinality_estimator();

    /* Create source plans for base relations */
    auto current_source_plans = optimize_source_plans(G, PT);
    auto singletons = Subproblem::All(G.num_sources());

    std::size_t next_idx = 0;
    double costs = 0;
    auto add_to_producers_and_models = [&](Subproblem problem) {
        source_plans[next_idx] = construct_join_order(G, PT, problem, current_source_plans);
        costs += PT[problem].cost;
        PT[problem].model->assign_to(Subproblem::Singleton(next_idx));
        base_models.emplace_back(CE.copy((*PT[problem].model)));
        next_idx++;
    };
    /*----- Compute join order and construct plan containing all joins. -----*/
    for (auto folding_problem : folding_problems) {
        optimize_join_order(G, PT, folding_problem);
        singletons -= folding_problem;
        add_to_producers_and_models(PT[folding_problem].left_fold);
        add_to_producers_and_models(PT[folding_problem].right_fold);
        }

    for (auto singleton_id : singletons) {
        source_plans[next_idx] = current_source_plans[singleton_id];
        PT[Subproblem::Singleton(singleton_id)].model->assign_to(Subproblem::Singleton(next_idx));
        base_models.emplace_back(std::move(PT[Subproblem::Singleton(singleton_id)].model));
        next_idx++;
    }
    return costs;
}

template<typename PlanTable>
void Optimizer_ResultDB_utils::optimize_join_order(const QueryGraph &G, PlanTable &PT, Subproblem folding_problem) {
    Catalog &C = Catalog::Get();
    auto &CE = C.get_database_in_use().cardinality_estimator();
    std::unique_ptr<YannakakisHeuristic> YH;
    PT[folding_problem].model = CE.estimate_join_all(G, PT, folding_problem, {});

    switch (Options::Get().yannakakis_heuristic)
    {
        case Options::YH_Decompose:
        {
            YH = std::make_unique<DecomposeHeuristic>(DecomposeHeuristic(PT, folding_problem, G, CE));
            break;
        }
        case Options::YH_Size:
        {
            YH = std::make_unique<SizeHeuristic>(SizeHeuristic(PT, folding_problem, G, CE));
            break;
        }
        case Options::YH_WeakCardinality:
        {
            YH = std::make_unique<WeakCardinalityHeuristic>(WeakCardinalityHeuristic(PT, folding_problem, G, CE));
            break;
        }
        case Options::YH_StrongCardinality:
        {
            YH = std::make_unique<StrongCardinalityHeuristic>(StrongCardinalityHeuristic(PT, folding_problem, G, CE));
            break;
        }
    }

#ifndef NDEBUG
    if (Options::Get().statistics) {
        std::size_t num_CSGs = 0, num_CCPs = 0;
        auto inc_CSGs = [&num_CSGs](Subproblem) { ++num_CSGs; };
        auto inc_CCPs = [&num_CCPs](Subproblem, Subproblem) { ++num_CCPs; };
        G.adjacency_matrix().for_each_CSG_undirected(folding_problem, inc_CSGs);
        G.adjacency_matrix().for_each_CSG_pair_undirected(folding_problem, inc_CCPs);
        std::cout << num_CSGs << " CSGs, " << num_CCPs << " CCPs" << std::endl;
    }
#endif

    auto callback = [&](Subproblem left, Subproblem right) {
        cnf::CNF condition; // TODO use join condition
        if ((left | right) != folding_problem) {
            PT.update(G, CE, C.cost_function(), left, right, condition);
        } else {
            PT.update_for_cycle_folding(G, CE, C.cost_function(), *YH, left, right, condition);
        }
    };
    M_TIME_EXPR(G.adjacency_matrix().for_each_CSG_pair_undirected(folding_problem, callback), "Plan for RESULTDB enumeration", C.timer());

    PT[folding_problem].tuple_size = PT[PT[folding_problem].left].tuple_size + PT[PT[folding_problem].right].tuple_size;

    if (Options::Get().statistics) {
        std::cout << "Est. total cost: " << PT.get_final().cost
                  << "\nPlan cost: " << PT[PT.get_final().left].cost + PT[PT.get_final().right].cost
                  << std::endl;
    }
}

template<typename PlanTable>
Producer* Optimizer_ResultDB_utils::construct_join_order(const QueryGraph &G, const PlanTable &PT, Subproblem problem,
                                                          std::unique_ptr<Producer*[]> &source_plans)
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    std::vector<std::reference_wrapper<Join>> joins;
    for (auto &J : G.joins()) joins.emplace_back(*J);

    /* Use nested lambdas to implement recursive lambda using CPS. */
    const auto construct_recursive = [&](Subproblem s) -> Producer* {
        auto construct_plan_impl = [&](Subproblem s, auto &construct_plan_rec) -> Producer* {
            auto subproblems = PT[s].get_subproblems();
            if (subproblems.empty()) {
                M_insist(s.size() == 1);
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
                    for (auto ds : it->get().sources())
                        join_sources(ds.get().id()) = true;

                    if (join_sources.is_subset(s)) { // possible join
                        join_condition = join_condition and it->get().condition();
                        it = joins.erase(it);
                    } else {
                        ++it;
                    }
                }

                /* Construct the join. */
                auto join = std::make_unique<JoinOperator>(join_condition);
                for (auto sub_plan : sub_plans)
                    join->add_child(sub_plan);
                auto join_info = std::make_unique<OperatorInformation>();
                join_info->subproblem = s;
                join_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model);
                join->info(std::move(join_info));
                return join.release();
            }
        };
        return construct_plan_impl(s, construct_plan_impl);
    };

    return construct_recursive(problem);
}

template<typename PlanTable>
std::unique_ptr<Producer*[]> Optimizer_ResultDB_utils::optimize_source_plans(const QueryGraph &G, PlanTable &PT)
{
    auto &C = Catalog::Get();
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    const auto num_sources = G.sources().size();
    auto source_plans = std::make_unique<Producer*[]>(num_sources);
    std::vector<size_t> tuple_sizes;
    G.get_projection_sizes_of_subproblems(tuple_sizes);
    for (auto &ds : G.sources()) {
        Subproblem s = Subproblem::Singleton(ds->id());
        if (auto bt = cast<BaseTable>(ds.get())) {
            /* Produce a scan for base tables. */
            PT[s].cost = 0;
            PT[s].model = CE.estimate_scan(G, s);
            PT[s].tuple_size = tuple_sizes[ds->id()];
            auto &store = bt->table().store();
            auto source = new ScanOperator(store, bt->name().assert_not_none());
            source_plans[ds->id()] = source;

            /* Set operator information. */
            auto source_info = std::make_unique<OperatorInformation>();
            source_info->subproblem = s;
            source_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model);
            source->info(std::move(source_info));
        } else {
            /* Recursively solve nested queries. */
            auto &Q = as<Query>(*ds);
            Optimizer Opt(C.plan_enumerator(), C.cost_function());
            auto [sub_plan, sub] = Opt.optimize(Q.query_graph());

            /* If an alias for the nested query is given and the nested query was not introduced as a fold, i.e. it does
             * not start with '$', prefix every attribute with the alias. */
            if (Q.alias().has_value() and *Q.alias()[0] != '$') {
                M_insist(is<ProjectionOperator>(sub_plan), "only projection may rename attributes");
                Schema S;
                for (auto &e : sub_plan->schema())
                    S.add({ Q.alias(), e.id.name }, e.type, e.constraints);
                sub_plan->schema() = S;
            }

            /* Update the plan table with the `DataModel` and cost of the nested query and save the plan in the array of
            * source plans. */
            PT[s].cost = sub.cost;
            sub.model->assign_to(s); // adapt model s.t. it describes the result of the current subproblem
            PT[s].model = std::move(sub.model);
            PT[s].tuple_size = tuple_sizes[ds->id()];
            /* Save the plan in the array of source plans. */
            source_plans[ds->id()] = sub_plan.release();
        }

        /* Apply filter, if any. */
        if (ds->filter().size()) {
            /* Update data model with filter. */
            auto new_model = CE.estimate_filter(G, *PT[s].model, ds->filter());
            PT[s].model = std::move(new_model);

            /* Optimize the filter by splitting into smaller filters and ordering them. */
            std::vector<cnf::CNF> filters = Optimizer::optimize_filter(ds->filter());
            Producer *filtered_ds = source_plans[ds->id()];

            /* Construct a plan as a sequence of filters. */
            for (auto &&filter : filters) {
                if (filter.size() == 1 and filter[0].size() > 1) { // disjunctive filter
                    auto tmp = std::make_unique<DisjunctiveFilterOperator>(std::move(filter));
                    tmp->add_child(filtered_ds);
                    filtered_ds = tmp.release();
                } else {
                    auto tmp = std::make_unique<FilterOperator>(std::move(filter));
                    tmp->add_child(filtered_ds);
                    filtered_ds = tmp.release();
                }
            }

            source_plans[ds->id()] = filtered_ds;
        }

        /* Set operator information. */
        auto source = source_plans[ds->id()];
        auto source_info = std::make_unique<OperatorInformation>();
        source_info->subproblem = s;
        source_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model); // includes filters, if any
        source->info(std::move(source_info));
    }
    return source_plans;
}

std::unique_ptr<Producer*[]> Optimizer_ResultDB_utils::solve_cycles_without_enum(QueryGraph &G, std::vector<std::unique_ptr<DataModel>> &base_models)
{
    std::vector<fold_t> folds = Optimizer_ResultDB_utils::compute_folds(G);
    Optimizer_ResultDB_utils::fold_query_graph(G, folds);
    auto source_plans = std::make_unique<Producer*[]>(G.num_sources());
    std::vector<Subproblem> folding_problems;
    optimize(G, folding_problems, source_plans, base_models);
    return source_plans;
}

std::pair<std::unique_ptr<Producer>, bool> Optimizer_ResultDB_utils::dp_resultdb(QueryGraph &G) {
    switch (Options::Get().plan_table_type)
    {
        case Options::PT_auto: {
            /* Select most suitable type of plan table depending on the query graph structure.
             * Currently a simple heuristic based on the number of data sources.
             * TODO: Consider join edges too.  Eventually consider #CSGs. */
            if (G.num_sources() <= 15) {
                return dp_resultdb_with_plantable<PlanTableSmallOrDense>(G);
            } else {
                return dp_resultdb_with_plantable<PlanTableLargeAndSparse>(G);
            }
        }

        case Options::PT_SmallOrDense: {
            return dp_resultdb_with_plantable<PlanTableSmallOrDense>(G);
        }

        case Options::PT_LargeAndSparse: {
            return dp_resultdb_with_plantable<PlanTableLargeAndSparse>(G);
        }
    }
}

template<typename PlanTable>
std::pair<std::unique_ptr<Producer>, bool> Optimizer_ResultDB_utils::dp_resultdb_with_plantable(QueryGraph &G) {
    PlanTable PT(G);

    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

    /* Create source plans for base relations */
    auto current_source_plans = optimize_source_plans(G, PT);
    auto complete_problem = Subproblem::All(G.num_sources());

    auto decompose_plan = [&](Subproblem complete_problem) -> std::pair<std::unique_ptr<Producer>, bool> {
        auto decompose_op = std::make_unique<DecomposeOperator>(std::cout, std::move(G.projections()),
        std::move(G.sources()));
        auto single_table_plan = construct_join_order(G, PT, complete_problem, current_source_plans);
        decompose_op->add_child(single_table_plan);
        return { std::move(decompose_op), true };
    };

    /* Is G is acyclic, simply run DP_CCP and TD_Fold */
    if (not G.is_cyclic()) {
        /* Add all models for the acyclic enumeration */
        std::vector<std::unique_ptr<DataModel>> base_models;
        for (std::size_t i = 0; i < G.num_sources(); i++) {
            base_models.emplace_back(CE.copy(*PT[Subproblem::Singleton(i)].model));
        }

        std::unordered_set<std::size_t> required_reductions;
        for (auto node_id: Subproblem::All(G.num_sources())) {
            if (PT[Subproblem::Singleton(node_id)].tuple_size != 0)
                required_reductions.emplace();
        }
        auto [reducer_order, reducer_costs] = enumerate_semi_join_reduction_order(G, required_reductions, G.adjacency_matrix(), CE, base_models);

        auto create_semi_join_plan = [&]() -> std::pair<std::unique_ptr<Producer>, bool> {
            auto semi_join_reduction_op = std::make_unique<SemiJoinReductionOperator>(std::move(G.projections()));

            const auto num_sources = G.num_sources();
            semi_join_reduction_op->semi_join_reduction_order() = std::move(reducer_order);
            semi_join_reduction_op->sources() = std::move(G.sources());
            semi_join_reduction_op->joins() = std::move(G.joins());


            /* Add source plans as children. */
            for (std::size_t i = 0; i < num_sources; ++i)
                semi_join_reduction_op->add_child(current_source_plans[i]);
            return { std::move(semi_join_reduction_op), true };
        };

        if (Options::Get().result_db_optimizer == Options::TD_Root) {
            return create_semi_join_plan();
        }

        /* Get best joins plans for single-table result */
        C.plan_enumerator()(G, C.cost_function(), PT);

        /* Estimate final decompose costs */
        double decompose_costs = PT[complete_problem].cost + YannakakisHeuristic::estimate_decompose_costs(G, complete_problem, PT[complete_problem], CE);
        
        if (reducer_costs < decompose_costs) {
            return create_semi_join_plan();
        } else {
            return decompose_plan(complete_problem);
        }
    }

    /* TD_Fold-Greedy */
    std::vector<fold_t> dp_fold_greedy_complete_folds;
    std::unordered_map<Subproblem, std::vector<Subproblem>, SubproblemHash> dp_fold_greedy_restricted_problems;
    if (Options::Get().result_db_optimizer == Options::DP_ResultDB || Options::Get().result_db_optimizer == Options::DP_Fold_Greedy)
        get_greedy_folds_and_folding_problems(G, dp_fold_greedy_complete_folds, dp_fold_greedy_restricted_problems);

    /* TD_Fold */
    std::vector<fold_t> dp_fold_complete_folds;
    std::unordered_set<Subproblem, SubproblemHash> dp_fold_problems;
    if (Options::Get().result_db_optimizer == Options::DP_ResultDB || dp_fold_greedy_restricted_problems.empty())
        get_folds_and_folding_problems(G, dp_fold_complete_folds, dp_fold_problems);

    // Create Yannakakis-Heuristics
    std::unordered_map<Subproblem, std::unique_ptr<YannakakisHeuristic>, SubproblemHash> heuristics;

    auto get_tuple_size = [&](Subproblem problem) {
        auto tuple_size = 0;
        for (auto relation: problem) {
            tuple_size += PT[Subproblem::Singleton(relation)].tuple_size;
        }
        return tuple_size;
    };

    auto solve_folding_problem = [&](Subproblem folding_problem, std::vector<Subproblem> restrictions) {
        /* Problem solved already */
        if (not PT[folding_problem].left_fold.empty()) {
            return;
        }
        if (Options::Get().result_db_optimizer == Options::DP_ResultDB) {
            auto callback_without_greedy = [&](Subproblem left, Subproblem right) {
                PT.update_for_cycle_folding(G, CE, C.cost_function(), *heuristics[folding_problem], left, right, cnf::CNF{});
            };
            auto callback_with_greedy = [&](Subproblem left, Subproblem right) {
                for (auto condition: restrictions) {
                    if (not(condition.is_subset(left) or condition.is_subset(right))) return;
                }
                PT.update_for_cycle_folding(G, CE, C.cost_function(), *heuristics[folding_problem], left, right, cnf::CNF{});
            };
            if (restrictions.empty()) {
                MinCutAGaT{}.partition(G.adjacency_matrix(), callback_without_greedy, folding_problem);
            } else {
                MinCutAGaT{}.partition(G.adjacency_matrix(), callback_with_greedy, folding_problem);
            }
        } else {
            if (restrictions.empty()) {
                auto callback = [&](Subproblem left, Subproblem right) {
                    if ((left | right) != folding_problem) {
                        PT.update(G, CE, C.cost_function(), left, right, cnf::CNF{});
                    } else {
                        PT.update(G, CE, C.cost_function(), left, right, cnf::CNF{});
                        PT.update_for_cycle_folding(G, CE, C.cost_function(), *heuristics[folding_problem], left, right, cnf::CNF{});
                    }
                };
                G.adjacency_matrix().for_each_CSG_pair_undirected(folding_problem, callback);
            } else {
                auto callback = [&](Subproblem left, Subproblem right) {
                    if ((left | right) != folding_problem) {
                        PT.update(G, CE, C.cost_function(), left, right, cnf::CNF{});
                    } else {
                        PT.update(G, CE, C.cost_function(), left, right, cnf::CNF{});
                        for (auto condition: restrictions) {
                            if (not(condition.is_subset(left) or condition.is_subset(right))) return;
                        }
                        PT.update_for_cycle_folding(G, CE, C.cost_function(), *heuristics[folding_problem], left, right, cnf::CNF{});
                    }
                };
                G.adjacency_matrix().for_each_CSG_pair_undirected(folding_problem, callback);
            }
        }

    };

    auto get_final_problem_cost = [&](Subproblem problem) -> double {
        return PT[problem].cost + YannakakisHeuristic::estimate_decompose_costs(G, complete_problem,PT[problem], CE);
    };

    /* Finalize the folds for both TD_Fold and TD_Fold-Greedy */
    auto add_subproblem_fold_to_folds = [&](Subproblem problem, std::vector<fold_t>& folds){
        auto create_fold_for_problem = [&](Subproblem problem) {
            fold_t fold;
            for (auto node_id: problem) {
                fold.emplace(node_id);
            }
            return fold;
        };
        /* Decide whether to use complete join or stop to join */
        auto create_two_folds = [&]() {
            folds.emplace_back(create_fold_for_problem(PT[problem].left_fold));
            folds.emplace_back(create_fold_for_problem(PT[problem].right_fold));
        };
        auto join_estimation = get_final_problem_cost(problem) + heuristics[problem]->estimate(G, CE, PT, problem, Subproblem());
        auto no_join_estimation = PT[problem].folding_cost;
        if (join_estimation < no_join_estimation) {
            folds.emplace_back(create_fold_for_problem(problem));
        } else {
            create_two_folds();
        }
    };

    auto get_folding_problem = [&](fold_t &fold) {
        auto folding_problem = Subproblem();
        for (auto node_id: fold) {
            folding_problem |= Subproblem::Singleton(node_id);
        }
        return folding_problem;
    };
    // Estimate the whole folding costs for the given folds
    auto estimate_folding_costs = [&](std::vector<fold_t>& folds) -> double {
        double folding_costs = 0;
        for (auto fold: folds) {
            auto folding_problem = get_folding_problem(fold);
            folding_costs += get_final_problem_cost(folding_problem);
        }
        return folding_costs;
    };

    // Estimate the final costs for the final TD_Root estimation for both DP_Fold and DP_Fold-Greedy
    auto compute_td_root_order_and_costs = [&](std::vector<fold_t>& folds) {
        // Use old matrix as default for new matrix
        AdjacencyMatrix folded_matrix(G.adjacency_matrix());
        std::vector<Subproblem> new_to_old_mapping(G.num_sources(), Subproblem(1));
        create_folded_adjacency_matrix(folds, G.adjacency_matrix(), folded_matrix, new_to_old_mapping);

        // Create new base models for the TD_Root numeration
        std::vector<std::unique_ptr<DataModel>> base_models;
        std::unordered_set<std::size_t> required_reductions;
        for (auto node_id: complete_problem) {
            Subproblem related_problem = new_to_old_mapping[node_id];
            base_models.emplace_back(CE.copy(*PT[related_problem].model));
            if (PT[related_problem].tuple_size != 0) required_reductions.emplace(node_id);
        }
        return enumerate_semi_join_reduction_order(G, required_reductions, folded_matrix, CE, base_models);
    };

    auto semi_join_reducer_plan = [&](std::vector<fold_t> folds) -> std::pair<std::unique_ptr<Producer>, bool> {
        // Create source plans for folds
        auto source_plans = std::make_unique<Producer*[]>(folds.size());
        std::vector<std::unique_ptr<DataModel>> base_models;
        std::unordered_set<std::size_t> required_reductions;
        for (std::size_t i = 0; i < folds.size(); i++) {
            auto folding_problem = get_folding_problem(folds[i]);
            if (PT[folding_problem].tuple_size != 0) required_reductions.emplace(i);
            if (not PT.has_plan(folding_problem)) {
                auto callback = [&](Subproblem left, Subproblem right) {
                    PT.update(G, CE, C.cost_function(), left, right, cnf::CNF{});
                };
                G.adjacency_matrix().for_each_CSG_pair_undirected(folding_problem, callback);
            }
            auto new_model = CE.copy(*PT[folding_problem].model);
            new_model->assign_to(Subproblem::Singleton(i));
            base_models.emplace_back(std::move(new_model));
            source_plans[i] = construct_join_order(G, PT, folding_problem, current_source_plans);
        }
        // Fold the join graph
        fold_query_graph(G, folds);

        auto [reducer_order, _] = enumerate_semi_join_reduction_order(G, required_reductions, G.adjacency_matrix(), CE, base_models);

        const auto num_sources = G.num_sources();

        // Create semi-join reducer plan
        auto semi_join_reduction_op = std::make_unique<SemiJoinReductionOperator>(std::move(G.projections()));
        semi_join_reduction_op->semi_join_reduction_order() = std::move(reducer_order);
        semi_join_reduction_op->sources() = std::move(G.sources());
        semi_join_reduction_op->joins() = std::move(G.joins());
        /* Add source plans as children. */
        for (std::size_t i = 0; i < num_sources; ++i)
            semi_join_reduction_op->add_child(source_plans[i]);
        return { std::move(semi_join_reduction_op), true };
    };
    if (Options::Get().result_db_optimizer == Options::DP_ResultDB) C.plan_enumerator()(G, C.cost_function(), PT);

    for (auto problem: dp_fold_problems) {
        PT[problem].model = CE.estimate_join_all(G, PT, problem, {});
        PT[problem].tuple_size = get_tuple_size(problem);
        heuristics[problem] = std::make_unique<WeakCardinalityHeuristic>(WeakCardinalityHeuristic(PT, problem, G, CE));
        solve_folding_problem(problem, std::vector<Subproblem>());
        add_subproblem_fold_to_folds(problem, dp_fold_complete_folds);
    }
    for (auto problem: dp_fold_greedy_restricted_problems) {
        PT[problem.first].model = CE.estimate_join_all(G, PT, problem.first, {});
        PT[problem.first].tuple_size = get_tuple_size(problem.first);
        heuristics[problem.first] = std::make_unique<WeakCardinalityHeuristic>(WeakCardinalityHeuristic(PT, problem.first, G, CE));
        solve_folding_problem(problem.first, problem.second);
        add_subproblem_fold_to_folds(problem.first, dp_fold_greedy_complete_folds);
    }

    if (Options::Get().result_db_optimizer == Options::DP_Fold) return semi_join_reducer_plan(dp_fold_complete_folds);
    if (Options::Get().result_db_optimizer == Options::DP_Fold_Greedy) {
        if (dp_fold_greedy_complete_folds.empty()) return semi_join_reducer_plan(dp_fold_complete_folds);
        else return semi_join_reducer_plan(dp_fold_greedy_complete_folds);
    }


    auto [dp_fold_reducer_order, dp_fold_reducer_costs] = compute_td_root_order_and_costs(dp_fold_complete_folds);

    double decompose_costs = PT[complete_problem].cost + YannakakisHeuristic::estimate_decompose_costs(G, complete_problem, PT[complete_problem], CE);
    double td_fold_costs = dp_fold_reducer_costs + estimate_folding_costs(dp_fold_complete_folds);
    // No check to dp_fold_greedy if no application can be found
    if (dp_fold_greedy_restricted_problems.empty()) {
        if (decompose_costs < td_fold_costs) {
            return decompose_plan(complete_problem);
        } else {
            // First, we need to create the source plans in order to return them
            return semi_join_reducer_plan(dp_fold_complete_folds);
        }
    }

    auto [dp_fold_greedy_reducer_order, dp_fold_greedy_reducer_costs] = compute_td_root_order_and_costs(dp_fold_greedy_complete_folds);
    double td_fold_greedy_costs = dp_fold_greedy_reducer_costs + estimate_folding_costs(dp_fold_greedy_complete_folds);

    return decompose_costs < td_fold_costs ?
            (decompose_costs < td_fold_greedy_costs ? decompose_plan(complete_problem) :
                semi_join_reducer_plan(dp_fold_greedy_complete_folds)) :
                    (td_fold_costs < td_fold_greedy_costs ? semi_join_reducer_plan(dp_fold_complete_folds) :
                        semi_join_reducer_plan(dp_fold_greedy_complete_folds));



}

std::pair<std::unique_ptr<Producer>, bool>
Optimizer_ResultDB::operator()(QueryGraph &G) const
{
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

    /*----- Perform pre-optimizations on the QueryGraph. -----*/
    for (auto &pre_opt : C.pre_optimizations())
        (*pre_opt.second).operator()(G);

    if (G.sources().size() == 0)
        return { std::make_unique<ProjectionOperator>(G.projections()), false };

    /*----- Check that query graph is compatible. If not, report warning and fallback to standard `Optimizer`. -----*/
    if (G.sources().size() < 2 or
        not G.group_by().empty() or
        not G.aggregates().empty() or
        not G.order_by().empty() or
        G.limit().limit or
        G.limit().offset or
        std::any_of(G.joins().begin(), G.joins().end(), [](auto &join){ return not join->condition().is_equi(); }) or
        std::any_of(G.projections().begin(), G.projections().end(), [](auto &p){
            return not is<const ast::Designator>(p.first) or p.second.has_value(); // only designators without alias supported
        }))
    {
        std::cerr << "WARNING: No compatible query for ResultDB `Optimizer`. Fallback to standard `Optimizer`."
                  << std::endl;

        std::unique_ptr<Producer> producer;
        Optimizer Opt(C.plan_enumerator(), C.cost_function());
        producer = Opt(G);
        return { std::move(producer), false };
    }

    /*----- Fold the query graph and compute semi-join reduction order. -----*/
    Optimizer_ResultDB_utils::combine_joins(G); // in case there are multiple joins between two specific data sources

    std::unique_ptr<Producer*[]> source_plans;
    std::vector<std::unique_ptr<DataModel>> base_models;
    std::unique_ptr<Producer> st_plan;

    if (Options::Get().optimize_result_db) {
        return Optimizer_ResultDB_utils::dp_resultdb(G);
    }


    if (G.is_cyclic())
    {
        source_plans = Optimizer_ResultDB_utils::solve_cycles_without_enum(G, base_models);
    }
    else {
        std::vector<Subproblem> folding_problems;
        source_plans = std::make_unique<Producer*[]>(G.num_sources());
        Optimizer_ResultDB_utils::optimize(G, folding_problems, source_plans, base_models);
    }

    /*----- Compute plans for data sources. -----*/
    const auto num_sources = G.sources().size();

    std::vector<semi_join_order_t> semi_join_reduction_order;
    auto semi_join_reduction_op = std::make_unique<SemiJoinReductionOperator>(std::move(G.projections()));
    /* Add source plans as children. */
    for (std::size_t i = 0; i < num_sources; ++i)
        semi_join_reduction_op->add_child(source_plans[i]);
    semi_join_reduction_order = Optimizer_ResultDB_utils::compute_semi_join_reduction_order(G, *semi_join_reduction_op);


    /* Construct a semi join reduction operator with all necessary information requried by the code generation. */
    semi_join_reduction_op->semi_join_reduction_order() = std::move(semi_join_reduction_order);
    semi_join_reduction_op->sources() = std::move(G.sources());
    semi_join_reduction_op->joins() = std::move(G.joins());



    return { std::move(semi_join_reduction_op), true };
}


/*======================================================================================================================
 * Optimizer
 *====================================================================================================================*/

std::pair<std::unique_ptr<Producer>, PlanTableEntry> Optimizer::optimize(QueryGraph &G) const
{
    switch (Options::Get().plan_table_type)
    {
        case Options::PT_auto: {
            /* Select most suitable type of plan table depending on the query graph structure.
             * Currently a simple heuristic based on the number of data sources.
             * TODO: Consider join edges too.  Eventually consider #CSGs. */
            if (G.num_sources() <= 15) {
                auto [plan, PT] = optimize_with_plantable<PlanTableSmallOrDense>(G);
                return { std::move(plan), std::move(PT.get_final()) };
            } else {
                auto [plan, PT] = optimize_with_plantable<PlanTableLargeAndSparse>(G);
                return { std::move(plan), std::move(PT.get_final()) };
            }
        }

        case Options::PT_SmallOrDense: {
            auto [plan, PT] = optimize_with_plantable<PlanTableSmallOrDense>(G);
            return { std::move(plan), std::move(PT.get_final()) };
        }

        case Options::PT_LargeAndSparse: {
            auto [plan, PT] = optimize_with_plantable<PlanTableLargeAndSparse>(G);
            return { std::move(plan), std::move(PT.get_final()) };
        }
    }
}

template<typename PlanTable>
std::pair<std::unique_ptr<Producer>, PlanTable> Optimizer::optimize_with_plantable(QueryGraph &G) const
{
    PlanTable PT(G);
    const auto num_sources = G.sources().size();
    auto &C = Catalog::Get();
    auto &CE = C.get_database_in_use().cardinality_estimator();

    if (num_sources == 0) {
        PT.get_final().cost = 0; // no sources → no cost
        PT.get_final().model = CE.empty_model(); // XXX: should rather be 1 (single tuple) than empty
        return { std::make_unique<ProjectionOperator>(G.projections()), std::move(PT) };
    }

    /*----- Initialize plan table and compute plans for data sources. -----*/
    auto source_plans = optimize_source_plans(G, PT);

    /*----- Compute join order and construct plan containing all joins. -----*/
    optimize_join_order(G, PT);
    std::unique_ptr<Producer> plan = construct_join_order(G, PT, source_plans);
    auto &entry = PT.get_final();

    /*----- Construct plan for remaining operations. -----*/
    if (Options::Get().decompose) {
        /* Add `DecomposeOperator` on top of plan. */
        if (not G.group_by().empty() or
            not G.aggregates().empty() or
            not G.order_by().empty() or
            G.limit().limit or
            G.limit().offset or
            std::any_of(G.joins().begin(), G.joins().end(), [](auto &join){ return not join->condition().is_equi(); }) or
            std::any_of(G.projections().begin(), G.projections().end(), [](auto &p){
                return not is<const ast::Designator>(p.first) or p.second.has_value(); // only designators without alias supported
            }))
        {
            std::cerr << "WARNING: No compatible query to decompose. Fallback to standard `Optimizer`."
                      << std::endl;

            std::unique_ptr<Producer> producer;
            Optimizer Opt(C.plan_enumerator(), C.cost_function());
            producer = M_TIME_EXPR(Opt(G), "Compute the logical query plan", C.timer());
            return { std::move(producer), std::move(PT) };
        }
        auto decompose_op = std::make_unique<DecomposeOperator>(std::cout, std::move(G.projections()),
                                                                std::move(G.sources()));
        decompose_op->add_child(plan.release());
        plan = std::move(decompose_op);
    } else {
        plan = optimize_plan(G, std::move(plan), entry);
    }

    return { std::move(plan), std::move(PT) };
}

template<typename PlanTable>
std::unique_ptr<Producer*[]> Optimizer::optimize_source_plans(const QueryGraph &G, PlanTable &PT) const
{
    const auto num_sources = G.sources().size();
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    auto source_plans = std::make_unique<Producer*[]>(num_sources);
    std::vector<size_t> tuple_sizes;
    G.get_projection_sizes_of_subproblems(tuple_sizes);
    for (auto &ds : G.sources()) {
        Subproblem s = Subproblem::Singleton(ds->id());
        if (auto bt = cast<BaseTable>(ds.get())) {
            /* Produce a scan for base tables. */
            PT[s].cost = 0;
            PT[s].model = CE.estimate_scan(G, s);
            PT[s].tuple_size = tuple_sizes[ds->id()];
            auto &store = bt->table().store();
            auto source = std::make_unique<ScanOperator>(store, bt->name().assert_not_none());

            /* Set operator information. */
            auto source_info = std::make_unique<OperatorInformation>();
            source_info->subproblem = s;
            source_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model);
            source->info(std::move(source_info));

            source_plans[ds->id()] = source.release();
        } else {
            /* Recursively solve nested queries. */
            auto &Q = as<Query>(*ds);
            const bool old = std::exchange(needs_projection_, Q.alias().has_value()); // aliased nested queries need projection
            auto [sub_plan, sub] = optimize(Q.query_graph());
            needs_projection_ = old;

            /* If an alias for the nested query is given, prefix every attribute with the alias. */
            if (Q.alias().has_value()) {
                M_insist(is<ProjectionOperator>(sub_plan), "only projection may rename attributes");
                Schema S;
                for (auto &e : sub_plan->schema())
                    S.add({ Q.alias(), e.id.name }, e.type, e.constraints);
                sub_plan->schema() = S;
            }

            /* Update the plan table with the `DataModel` and cost of the nested query and save the plan in the array of
             * source plans. */
            PT[s].cost = sub.cost;
            sub.model->assign_to(s); // adapt model s.t. it describes the result of the current subproblem
            PT[s].model = std::move(sub.model);
            PT[s].tuple_size = tuple_sizes[ds->id()];
            source_plans[ds->id()] = sub_plan.release();
        }

        /* Apply filter, if any. */
        if (ds->filter().size()) {
            /* Optimize the filter by splitting into smaller filters and ordering them. */
            std::vector<cnf::CNF> filters = Optimizer::optimize_filter(ds->filter());
            Producer *filtered_ds = source_plans[ds->id()];

            /* Construct a plan as a sequence of filters. */
            for (auto &&filter : filters) {
                /* Update data model with filter. */
                auto new_model = CE.estimate_filter(G, *PT[s].model, filter);
                PT[s].model = std::move(new_model);

                if (filter.size() == 1 and filter[0].size() > 1) { // disjunctive filter
                    auto tmp = std::make_unique<DisjunctiveFilterOperator>(std::move(filter));
                    tmp->add_child(filtered_ds);
                    filtered_ds = tmp.release();
                } else {
                    auto tmp = std::make_unique<FilterOperator>(std::move(filter));
                    tmp->add_child(filtered_ds);
                    filtered_ds = tmp.release();
                }

                /* Set operator information. */
                auto source_info = std::make_unique<OperatorInformation>();
                source_info->subproblem = s;
                source_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model); // includes filters, if any
                filtered_ds->info(std::move(source_info));
            }

            source_plans[ds->id()] = filtered_ds;
        }
    }
    return source_plans;
}

template<typename PlanTable>
void Optimizer::optimize_join_order(const QueryGraph &G, PlanTable &PT) const
{
    Catalog &C = Catalog::Get();
    auto &CE = C.get_database_in_use().cardinality_estimator();

#ifndef NDEBUG
    if (Options::Get().statistics) {
        std::size_t num_CSGs = 0, num_CCPs = 0;
        const Subproblem All = Subproblem::All(G.num_sources());
        auto inc_CSGs = [&num_CSGs](Subproblem) { ++num_CSGs; };
        auto inc_CCPs = [&num_CCPs](Subproblem, Subproblem) { ++num_CCPs; };
        G.adjacency_matrix().for_each_CSG_undirected(All, inc_CSGs);
        G.adjacency_matrix().for_each_CSG_pair_undirected(All, inc_CCPs);
        std::cout << num_CSGs << " CSGs, " << num_CCPs << " CCPs" << std::endl;
    }
#endif

    M_TIME_EXPR(plan_enumerator()(G, cost_function(), PT), "Plan enumeration", C.timer());

    if (Options::Get().statistics) {
        std::cout << "Est. total cost: " << PT.get_final().cost
                  << "\nEst. result set size: " << CE.predict_cardinality(*PT.get_final().model)
                  << "\nPlan cost: " << PT[PT.get_final().left].cost + PT[PT.get_final().right].cost
                  << std::endl;
    }
}

template<typename PlanTable>
std::unique_ptr<Producer> Optimizer::construct_join_order(const QueryGraph &G, const PlanTable &PT,
                                                          const std::unique_ptr<Producer*[]> &source_plans) const
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    std::vector<std::reference_wrapper<Join>> joins;
    for (auto &J : G.joins()) joins.emplace_back(*J);

    /* Use nested lambdas to implement recursive lambda using CPS. */
    const auto construct_recursive = [&](Subproblem s) -> Producer* {
        auto construct_plan_impl = [&](Subproblem s, auto &construct_plan_rec) -> Producer* {
            auto subproblems = PT[s].get_subproblems();
            if (subproblems.empty()) {
                M_insist(s.size() == 1);
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
                    for (auto ds : it->get().sources())
                        join_sources(ds.get().id()) = true;

                    if (join_sources.is_subset(s)) { // possible join
                        join_condition = join_condition and it->get().condition();
                        it = joins.erase(it);
                    } else {
                        ++it;
                    }
                }

                /* Construct the join. */
                auto join = std::make_unique<JoinOperator>(join_condition);
                for (auto sub_plan : sub_plans)
                    join->add_child(sub_plan);
                auto join_info = std::make_unique<OperatorInformation>();
                join_info->subproblem = s;
                join_info->estimated_cardinality = CE.predict_cardinality(*PT[s].model);
                join->info(std::move(join_info));
                return join.release();
            }
        };
        return construct_plan_impl(s, construct_plan_impl);
    };

    return std::unique_ptr<Producer>(construct_recursive(Subproblem::All(G.num_sources())));
}

std::unique_ptr<Producer> Optimizer::optimize_plan(QueryGraph &G, std::unique_ptr<Producer> plan, PlanTableEntry &entry)
const
{
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    /* Perform grouping. */
    if (not G.group_by().empty()) {
        /* Compute `DataModel` after grouping. */
        auto new_model = CE.estimate_grouping(G, *entry.model, G.group_by()); // TODO provide aggregates
        entry.model = std::move(new_model);
        // TODO pick "best" algorithm
        auto group_by = std::make_unique<GroupingOperator>(G.group_by(), G.aggregates());
        group_by->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem::All(G.sources().size());
        info->estimated_cardinality = CE.predict_cardinality(*entry.model);

        group_by->info(std::move(info));
        plan = std::move(group_by);
    } else if (not G.aggregates().empty()) {
        /* Compute `DataModel` after grouping. */
        auto new_model = CE.estimate_grouping(G, *entry.model, std::vector<GroupingOperator::group_type>());
        entry.model = std::move(new_model);
        auto agg = std::make_unique<AggregationOperator>(G.aggregates());
        agg->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem::All(G.sources().size());
        info->estimated_cardinality = CE.predict_cardinality(*entry.model);

        agg->info(std::move(info));
        plan = std::move(agg);
    }

    auto additional_projections = Optimizer::compute_projections_required_for_order_by(G.projections(), G.order_by());
    const bool requires_post_projection = not additional_projections.empty();

    /* Perform projection. */
    if (not additional_projections.empty() or not G.projections().empty()) {
        /* Merge original projections with additional projections. */
        additional_projections.insert(additional_projections.end(), G.projections().begin(), G.projections().end());
        auto projection = std::make_unique<ProjectionOperator>(std::move(additional_projections));
        projection->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem::All(G.sources().size());
        info->estimated_cardinality = projection->child(0)->info().estimated_cardinality;

        projection->info(std::move(info));
        plan = std::move(projection);
    }

    /* Perform ordering. */
    if (not G.order_by().empty()) {
        // TODO estimate data model
        auto order_by = std::make_unique<SortingOperator>(G.order_by());
        order_by->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem::All(G.sources().size());
        info->estimated_cardinality = order_by->child(0)->info().estimated_cardinality;

        order_by->info(std::move(info));
        plan = std::move(order_by);
    }

    /* Limit. */
    if (G.limit().limit or G.limit().offset) {
        /* Compute `DataModel` after limit. */
        auto new_model = CE.estimate_limit(G, *entry.model, G.limit().limit, G.limit().offset);
        entry.model = std::move(new_model);
        // TODO estimate data model
        auto limit = std::make_unique<LimitOperator>(G.limit().limit, G.limit().offset);
        limit->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem::All(G.sources().size());
        info->estimated_cardinality = CE.predict_cardinality(*entry.model);

        limit->info(std::move(info));
        plan = std::move(limit);
    }

    /* Perform post-ordering projection. */
    if (requires_post_projection or (not is<ProjectionOperator>(plan) and needs_projection_)) {
        // TODO estimate data model
        /* Change aliased projections in designators with the alias as name since original projection is
         * performed beforehand. */
        std::vector<projection_type> adapted_projections;
        for (auto [expr, alias] : G.projections()) {
            if (alias.has_value()) {
                Token name(expr.get().tok.pos, alias.assert_not_none(), TK_IDENTIFIER);
                auto d = std::make_unique<const Designator>(Token::CreateArtificial(), Token::CreateArtificial(),
                                                            std::move(name), expr.get().type(), &expr.get());
                adapted_projections.emplace_back(*d, ThreadSafePooledOptionalString{});
                created_exprs_.emplace_back(std::move(d));
            } else {
                adapted_projections.emplace_back(expr, ThreadSafePooledOptionalString{});
            }
        }
        auto projection = std::make_unique<ProjectionOperator>(std::move(adapted_projections));
        projection->add_child(plan.release());

        /* Set operator information. */
        auto info = std::make_unique<OperatorInformation>();
        info->subproblem = Subproblem::All(G.sources().size());
        info->estimated_cardinality = projection->child(0)->info().estimated_cardinality;

        projection->info(std::move(info));
        plan = std::move(projection);
    }
    return plan;
}

#define DEFINE(PLANTABLE) \
template \
std::pair<std::unique_ptr<Producer>, PLANTABLE> \
Optimizer::optimize_with_plantable(QueryGraph&) const; \
template \
std::unique_ptr<Producer*[]> \
Optimizer::optimize_source_plans(const QueryGraph&, PLANTABLE&) const; \
template \
void \
Optimizer::optimize_join_order(const QueryGraph&, PLANTABLE&) const;   \
template \
std::unique_ptr<Producer> \
Optimizer::construct_join_order(const QueryGraph&, const PLANTABLE&, const std::unique_ptr<Producer*[]>&) const; \
template \
std::unique_ptr<Producer*[]> Optimizer_ResultDB_utils::optimize_source_plans(const QueryGraph&, PLANTABLE&); \
template \
void \
Optimizer_ResultDB_utils::optimize_join_order(const QueryGraph&, PLANTABLE&, Subproblem);   \
template \
Producer* \
Optimizer_ResultDB_utils::construct_join_order(const QueryGraph&, const PLANTABLE&, Subproblem, std::unique_ptr<Producer*[]>&)
DEFINE(PlanTableSmallOrDense);
DEFINE(PlanTableLargeAndSparse);
#undef DEFINE
