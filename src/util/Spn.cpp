#include "Spn.hpp"

#include <iomanip>
#include "mutable/util/AdjacencyMatrix.hpp"
#include <mutable/util/fn.hpp>
#include "util/Kmeans.hpp"
#include "util/RDC.hpp"


using namespace m;
using namespace Eigen;


namespace {

std::size_t MIN_INSTANCE_SLICE = 0;
const int MAX_K = 7;
const float RDC_THRESHOLD = 0.3f;

MatrixXf normalize_minmax(const MatrixXf &data)
{
    const RowVectorXf mins = data.colwise().minCoeff();
    const RowVectorXf maxs = data.colwise().maxCoeff() - mins;

    MatrixXf normalized(data.rows(), data.cols());
    for (unsigned i = 0; i != data.cols(); ++i) {
        if (maxs.array()[i] == 0) // min == max  =>  empty range [min, max)
            normalized.col(i) = VectorXf::Zero(data.rows());
        else
            normalized.col(i) = (data.col(i).array() - mins.array()[i]) / maxs.array()[i];
    }
    return normalized;
}

/** Compute the splitting of the columns (attributes) of the given data with the RDC algorithm.
 *
 * @param data the data to be split
 * @param variables the variable scope of the data
 * @return the variable id and column id splitting candidates
 */
std::pair<std::vector<SmallBitset>, std::vector<SmallBitset>>rdc_split(const MatrixXf &data, SmallBitset variables)
{
    const auto num_cols = data.cols();
    AdjacencyMatrix adjacency_matrix(num_cols);
    std::vector<MatrixXf> CDF_matrices(num_cols);

    /* precompute CDF matrices */
    for (int i = 0; i < num_cols; i++) { CDF_matrices[i] = create_CDF_matrix(data.col(i)); }

    /* build a graph with edges between correlated columns (attributes) */
    for (unsigned i = 0; i < num_cols - 1; i++) {
        for (unsigned j = i+1; j < num_cols; j++) {
            const float rdc_value = rdc_precomputed_CDF(CDF_matrices[i], CDF_matrices[j]);
            /* if the rdc value is greater or equal to the threshold, consider columns dependent */
            if (rdc_value >= RDC_THRESHOLD) {
                adjacency_matrix(i,j) = true;
                adjacency_matrix(j,i) = true;
            }
        }
    }

    SmallBitset remaining((1UL << num_cols) - 1UL);
    std::vector<SmallBitset> connected_subgraphs;

    /* build the connected subgraphs(dependent subsets of attributes) */
    while (remaining) {
        auto next = remaining.begin();
        const SmallBitset CSG = adjacency_matrix.reachable(next.as_set());
        connected_subgraphs.emplace_back(CSG);
        remaining -= CSG;
    }

    std::vector<SmallBitset> variable_candidates(connected_subgraphs.size());

    std::vector<std::size_t> temp_variables;
    temp_variables.reserve(num_cols);
    for (auto it = variables.begin(); it != variables.end(); ++it) { temp_variables.emplace_back(*it); }

    /* translate SmallBitset to correct output */
    for (std::size_t i = 0; i < connected_subgraphs.size(); ++i) {
        for (auto it = connected_subgraphs[i].begin(); it != connected_subgraphs[i].end(); ++it) {
            variable_candidates[i][temp_variables[*it]] = true;
        }
    }

    return std::make_pair(connected_subgraphs, variable_candidates);
}

}


void Spn::Node::dump() const { dump(std::cerr); }
void Spn::Node::dump(std::ostream &out) const { print(out, 0); }


/*======================================================================================================================
 * Sum Node
 *====================================================================================================================*/

std::pair<float, float> Spn::Sum::evaluate(const Filter &filter, unsigned leaf_id, EvalType eval_type) const
{
    float expectation_result = 0.f;
    float likelihood_result = 0.f;
    for (auto &child : children) {
        auto [expectation, likelihood] = child->child->evaluate(filter, leaf_id, eval_type);
        expectation_result += child->weight * expectation;
        likelihood_result += child->weight * likelihood;
    }

    return { expectation_result, likelihood_result };
}

void Spn::Sum::update(VectorXf &row, SmallBitset variables, Spn::UpdateType update_type)
{
    /* compute nearest cluster */
    unsigned nearest_centroid = 0;
    std::size_t num_clusters = children.size();
    float delta = (children[0]->centroid - row).squaredNorm();
    for (std::size_t i = 1; i < num_clusters; i++) {
        float next_delta = (children[i]->centroid - row).squaredNorm();
        if (next_delta < delta) {
            delta = next_delta;
            nearest_centroid = i;
        }
    }

    /* adjust weights of the sum nodes */
    children[nearest_centroid]->child->num_rows++;
    num_rows++;

    for (std::size_t i = 0; i < num_clusters; i++) {
        children[i]->weight = children[i]->child->num_rows / float(num_rows);
    }

    children[nearest_centroid]->child->update(row, variables, update_type);
}

std::size_t Spn::Sum::estimate_number_distinct_values(unsigned id) const
{
    std::size_t result = 0;
    for (auto &child : children) {
        result += child->child->estimate_number_distinct_values(id);
    }

    return std::min(result, num_rows);
}

void Spn::Sum::print(std::ostream &out, std::size_t num_tabs) const
{
    for (std::size_t i = 0; i < children.size(); i++) {
        for (std::size_t n = 0; n < num_tabs; n++) { out << "\t"; }
        if (i != 0) { out << "+ "; }
        out << children[i]->weight << "\n";
        children[i]->child->print(out, num_tabs+1);
    }
}


/*======================================================================================================================
 * Product Node
 *====================================================================================================================*/

std::pair<float, float> Spn::Product::evaluate(const Filter &filter, unsigned leaf_id, EvalType eval_type) const
{
    float expectation_result = 1.f;
    float likelihood_result = 1.f;
    for (auto &child : children) {
        for (const auto & it : filter) {
            if (child->variables[it.first]) {
                auto [expectation, likelihood] = child->child->evaluate(filter, it.first, eval_type);
                expectation_result *= expectation;
                likelihood_result *= likelihood;
                break;
            }
        }
    }
    return {expectation_result, likelihood_result };
}

void Spn::Product::update(VectorXf &row, SmallBitset variables, UpdateType update_type)
{
    std::unordered_map<unsigned, unsigned> variable_to_index;
    unsigned index = 0;
    for (auto it = variables.begin(); it != variables.end(); ++it) { variable_to_index.emplace(*it, index++); }

    /* update each child of the product node with the according subset of attributes of the row */
    for (auto &child : children) {
        std::size_t num_cols = child->variables.size();
        VectorXf proj_row(num_cols);
        auto it = child->variables.begin();
        for (std::size_t i = 0; i < num_cols; ++i) {
            proj_row(i) = row(variable_to_index[*it]);
            ++it;
        }
        child->child->update(proj_row, child->variables, update_type);
    }
}

std::size_t Spn::Product::estimate_number_distinct_values(unsigned id) const
{
    for (auto &child : children) {
        if (child->variables[id]) {
            return child->child->estimate_number_distinct_values(id);
        }
    }

    return 0;
}

void Spn::Product::print(std::ostream &out, std::size_t num_tabs) const
{
    for (std::size_t i = 0; i < children.size(); i++) {
        for (std::size_t n = 0; n < num_tabs; n++) { out << "\t"; }
        if (i != 0) { out << "* "; }
        out << "variable scope=(";
        for (auto it = children[i]->variables.begin(); it != children[i]->variables.end(); ++it) { out << *it; }
        out << "):" << "\n";
        children[i]->child->print(out, num_tabs+1);
    }
}


/*======================================================================================================================
 * DiscreteLeaf Node
 *====================================================================================================================*/

std::pair<float, float> Spn::DiscreteLeaf::evaluate(const Filter &filter, unsigned leaf_id, EvalType eval_type) const
{
    auto [spn_operator, value] = filter.at(leaf_id);

    if (spn_operator == IS_NULL) { return { null_probability, null_probability }; }

    if (bins.empty()) { return { 0.f, 0.f }; }

    if (spn_operator == EXPECTATION) {
        float expectation = bins[0].cumulative_probability * bins[0].value;
        float prev_probability = bins[0].cumulative_probability;
        for (std::size_t bin_id = 1; bin_id < bins.size(); bin_id++) {
            float cumulative_probability = bins[bin_id].cumulative_probability;
            float probability = cumulative_probability - prev_probability;
            expectation += probability * bins[bin_id].value;
            prev_probability = cumulative_probability;
        }
        return {expectation, 1.f };
    }
    /* probability of last bin, because the cumulative probability of the last bin is not always 1 because of NULL */
    float last_prob = std::prev(bins.end())->cumulative_probability;
    float probability = 0.f;

    if (spn_operator == SpnOperator::EQUAL) {
        if (bins.begin()->value > value or (std::prev(bins.end()))->value < value) { return { 0.f, 0.f }; }
        auto lower_bound = std::lower_bound(bins.begin(), bins.end(), value);
        if (lower_bound->value == value) {
            probability = lower_bound->cumulative_probability;
            if (lower_bound != bins.begin()) { probability -= (--lower_bound)->cumulative_probability; }
        }
        return { probability, probability };
    }

    if (spn_operator == SpnOperator::LESS) {
        if (bins.begin()->value >= value) { return { 0.f, 0.f }; }
        if ((std::prev(bins.end()))->value < value) { return { last_prob, last_prob }; }
        auto lower_bound = std::lower_bound(bins.begin(), bins.end(), value);
        probability = (--lower_bound)->cumulative_probability;
        return { probability, probability };
    }

    if (spn_operator == SpnOperator::LESS_EQUAL) {
        if (bins.begin()->value > value) { return { 0.f, 0.f }; }
        if ((std::prev(bins.end()))->value <= value) { return { last_prob, last_prob }; }
        auto upper_bound = std::upper_bound(bins.begin(), bins.end(), value,
            [](float bin_value, DiscreteLeaf::Bin bin) { return bin_value < bin.value; }
        );
        probability = (--upper_bound)->cumulative_probability;
        return { probability, probability };
    }

    if (spn_operator == SpnOperator::GREATER) {
        if (bins.begin()->value > value) { return { last_prob, last_prob }; }
        if ((std::prev(bins.end()))->value <= value) { return { 0.f, 0.f }; }
        auto upper_bound = std::upper_bound(bins.begin(), bins.end(), value,
            [](float bin_value, DiscreteLeaf::Bin bin) { return bin_value < bin.value; }
        );
        probability = last_prob - (--upper_bound)->cumulative_probability;
        return { probability, probability };
    }

    if (spn_operator == SpnOperator::GREATER_EQUAL) {
        if (bins.begin()->value >= value) { return { last_prob, last_prob }; }
        if ((std::prev(bins.end()))->value < value) { return { 0.f, 0.f }; }
        auto lower_bound = std::lower_bound(bins.begin(), bins.end(), value);
        probability = last_prob - (--lower_bound)->cumulative_probability;
        return { probability, probability };
    }

    return { 0.f, 0.f };
}

void Spn::DiscreteLeaf::update(VectorXf &row, SmallBitset variables, Spn::UpdateType update_type)
{
    const float value = row(0);

    if (bins.empty()) {
        if (update_type == INSERT) {
            bins.emplace_back(value, 1.f/(num_rows + 1));
            null_probability = (null_probability * num_rows) / (num_rows + 1);
            num_rows++;
        }
        return;
    }

    /* copy bins with the actual number of values in a bin */
    std::vector<Bin> updated_bins;
    updated_bins.reserve(bins.size());
    updated_bins.emplace_back(bins[0].value, bins[0].cumulative_probability * num_rows);
    for (std::size_t i = 1; i < bins.size(); i++) {
        updated_bins.emplace_back(
            bins[i].value,
            (bins[i].cumulative_probability - bins[i-1].cumulative_probability) * num_rows
        );
    }

    /* insert the update value into the correct bin or create a new one if the bin does not exist */
    if (update_type == INSERT) {
        auto lower_bound = std::lower_bound(updated_bins.begin(), updated_bins.end(), value);
        if (lower_bound == updated_bins.end()) { updated_bins.emplace_back(value, 1.f); }
        else if (lower_bound->value != value) { updated_bins.emplace(lower_bound, value, 1.f); }
        else { lower_bound->cumulative_probability += 1; }
        num_rows++;
    }
    /* delete the update value from the correct bin  */
    else {
        auto lower_bound = std::lower_bound(updated_bins.begin(), updated_bins.end(), value);
        if (lower_bound->value != value) { return; }
        lower_bound->cumulative_probability -= 1;
        num_rows--;
    }

    /* calculate the cumulative probability */
    updated_bins[0].cumulative_probability /= float(num_rows);
    for (std::size_t i = 1; i < updated_bins.size(); i++) {
        updated_bins[i].cumulative_probability /= float(num_rows);
        updated_bins[i].cumulative_probability += updated_bins[i-1].cumulative_probability;
    }

    bins = std::move(updated_bins);
}

std::size_t Spn::DiscreteLeaf::estimate_number_distinct_values(unsigned id) const
{
    return bins.size();
}

void Spn::DiscreteLeaf::print(std::ostream &out, std::size_t num_tabs) const
{
    for (std::size_t n = 0; n < num_tabs; n++) { out << "\t"; }
    out << "[";
    if (not bins.empty()) {
        out << bins[0].value << ":" << bins[0].cumulative_probability;
        for (std::size_t i = 1; i < bins.size(); i++) {
            out << ", " << bins[i].value << ":" << bins[i].cumulative_probability - bins[i-1].cumulative_probability;
        }
        out << " ";
    }
    out << "null_prob:" << null_probability;
    out << "]";
    out << "\n";
}


/*======================================================================================================================
 * ContinuousLeaf Node
 *====================================================================================================================*/

std::pair<float, float> Spn::ContinuousLeaf::evaluate(const Filter &filter, unsigned leaf_id, EvalType eval_type) const
{
    auto [spn_operator, value] = filter.at(leaf_id);
    float probability = 0.f;
    if (spn_operator == IS_NULL) { return { null_probability, null_probability }; }
    if (bins.empty()) { return { 0.f, 0.f }; }

    if (spn_operator == SpnOperator::EXPECTATION) {
        float expectation = lower_bound * lower_bound_probability;
        expectation += bins[0].cumulative_probability * ((bins[0].upper_bound + lower_bound) / 2.f);
        float prev_probability = bins[0].cumulative_probability;
        float prev_upper_bound = bins[0].upper_bound;
        for (std::size_t bin_id = 1; bin_id < bins.size(); bin_id++) {
            float upper_bound = bins[bin_id].upper_bound;
            float cumulative_probability = bins[bin_id].cumulative_probability;
            float current_probability = cumulative_probability - prev_probability;
            expectation += current_probability * ((prev_upper_bound + upper_bound) / 2);
            prev_probability = cumulative_probability;
            prev_upper_bound = upper_bound;
        }
        return std::make_pair(expectation, 1.f);
    }

    if (spn_operator == SpnOperator::EQUAL) {
        if (lower_bound > value or (std::prev(bins.end()))->upper_bound < value) { return { 0.f, 0.f }; }
        if (lower_bound == value) { return { lower_bound_probability, lower_bound_probability }; }
        if (value <= bins[0].upper_bound) {
            probability = bins[0].cumulative_probability - lower_bound_probability;
            return { probability, probability };
        }
        auto std_lower_bound = std::lower_bound(bins.begin(), bins.end(), value);
        probability = std_lower_bound->cumulative_probability - (--std_lower_bound)->cumulative_probability;
        return { probability, probability };
    }

    if (spn_operator == LESS or spn_operator == LESS_EQUAL) {
        if (lower_bound >= value) {
            if (lower_bound == value and spn_operator == LESS_EQUAL) {
                return { lower_bound_probability, lower_bound_probability };
            }
            return { 0.f, 0.f };
        }
        if ((std::prev(bins.end()))->upper_bound <= value) {
            probability = std::prev(bins.end())->cumulative_probability;
            return { probability, probability };
        }
        if (value <= bins[0].upper_bound) {
            float prob = bins[0].cumulative_probability - lower_bound_probability;
            probability = lower_bound_probability +
                (prob * ((value - lower_bound) / (bins[0].upper_bound - lower_bound)));
            return { probability, probability };
        }
        auto std_lower_bound = std::lower_bound(bins.begin(), bins.end(), value);
        auto &bin = *std_lower_bound;
        auto &prev_bin = *(--std_lower_bound);
        float bin_probability = bin.cumulative_probability - prev_bin.cumulative_probability;
        float percentile = (value - prev_bin.upper_bound) / (bin.upper_bound - prev_bin.upper_bound);
        switch (eval_type) {
            case APPROXIMATE:
                probability = prev_bin.cumulative_probability + (bin_probability * percentile);
            case UPPER_BOUND:
                probability = bin.cumulative_probability;
            case LOWER_BOUND:
                probability = prev_bin.cumulative_probability;
        }
        return { probability, probability };
    }

    if (spn_operator == GREATER_EQUAL or spn_operator == GREATER) {
        float last_prob = std::prev(bins.end())->cumulative_probability;
        if (lower_bound >= value) {
            if (lower_bound == value and spn_operator == GREATER) {
                probability = last_prob - lower_bound_probability;
                return { probability, probability };
            }
            return { last_prob, last_prob };
        }
        if ((std::prev(bins.end()))->upper_bound <= value) { return { 0.f, 0.f }; }
        if (value <= bins[0].upper_bound) {
            float prob = bins[0].cumulative_probability - lower_bound_probability;
            float split_bin_prob = (prob * (1.f - ((value - lower_bound) / (bins[0].upper_bound - lower_bound))));
            probability = last_prob - bins[0].cumulative_probability + split_bin_prob;
            return { probability, probability };
        }
        auto std_lower_bound = std::lower_bound(bins.begin(), bins.end(), value);
        auto &bin = *std_lower_bound;
        auto &prev_bin = *(--std_lower_bound);
        float bin_probability = bin.cumulative_probability - prev_bin.cumulative_probability;
        float percentile = 1.f - ((value - prev_bin.upper_bound) / (bin.upper_bound - prev_bin.upper_bound));
        switch (eval_type) {
            case APPROXIMATE:
                probability = last_prob - bin.cumulative_probability + (bin_probability * percentile);
            case UPPER_BOUND:
                probability = last_prob - prev_bin.cumulative_probability;
            case LOWER_BOUND:
                probability = last_prob - bin.cumulative_probability;
        }
        return { probability, probability };
    }

    return { 0.f, 0.f };
}

void Spn::ContinuousLeaf::update(VectorXf &row, SmallBitset variables, Spn::UpdateType update_type)
{
    const float value = row(0);

    if (bins.empty()) {
        if (update_type == INSERT) {
            bins.emplace_back(value, 1.f/(num_rows + 1));
            null_probability = (null_probability * num_rows) / (num_rows + 1);
            num_rows++;
        }
        return;
    }

    /* copy bins with the actual number of values in a bin */
    std::vector<Bin> updated_bins;
    float updated_lower_bound_prob = lower_bound_probability * num_rows;
    updated_bins.reserve(bins.size());
    updated_bins.emplace_back(
        bins[0].upper_bound,
        (bins[0].cumulative_probability - lower_bound_probability) * num_rows
    );
    for (std::size_t i = 1; i < bins.size(); i++) {
        updated_bins.emplace_back(
            bins[i].upper_bound,
            (bins[i].cumulative_probability - bins[i-1].cumulative_probability) * num_rows
        );
    }

    /* insert the update value into the correct bin or create a new one if the bin does not exist */
    if (update_type == INSERT) {
        if (value == lower_bound) {
            updated_lower_bound_prob += 1;
        } else if (value < lower_bound) {
            updated_bins[0].cumulative_probability += updated_lower_bound_prob;
            lower_bound = value;
            updated_lower_bound_prob = 1.f;
        } else if (value > updated_bins[updated_bins.size() - 1].upper_bound) {
            updated_bins.emplace_back(value, 1.f);
        } else {
            auto std_lower_bound = std::lower_bound(updated_bins.begin(), updated_bins.end(), value);
            std_lower_bound->cumulative_probability += 1;
        }
        num_rows++;
    }
    /* delete the update value from the correct bin  */
    else {
        if (value < lower_bound or value > updated_bins[updated_bins.size() - 1].upper_bound) { return; }
        if (value == lower_bound) {
            updated_lower_bound_prob += 1;
        } else {
            auto std_lower_bound = std::lower_bound(updated_bins.begin(), updated_bins.end(), value);
            if (std_lower_bound->cumulative_probability == 0) { return; }
            std_lower_bound->cumulative_probability -= 1;
        }
        num_rows--;
    }

    /* calculate the cumulative probability */
    updated_lower_bound_prob /= float(num_rows);
    updated_bins[0].cumulative_probability /= float(num_rows);
    updated_bins[0].cumulative_probability += updated_lower_bound_prob;
    for (std::size_t i = 1; i < updated_bins.size(); i++) {
        updated_bins[i].cumulative_probability /= float(num_rows);
        updated_bins[i].cumulative_probability += updated_bins[i-1].cumulative_probability;
    }
    lower_bound_probability = updated_lower_bound_prob;
    bins = std::move(updated_bins);
}

std::size_t Spn::ContinuousLeaf::estimate_number_distinct_values(unsigned id) const
{
    return num_rows;
}

void Spn::ContinuousLeaf::print(std::ostream &out, std::size_t num_tabs) const
{
    for (std::size_t n = 0; n < num_tabs; n++) { out << "\t"; }
    out << "[";
    if (not bins.empty()) {
        out << lower_bound_probability << ": ";
        out << lower_bound;
        out << " :" << bins[0].cumulative_probability - lower_bound_probability << ": " << bins[0].upper_bound;
        for (std::size_t i = 1; i < bins.size(); i++) {
            out << " :" << bins[i].cumulative_probability - bins[i-1].cumulative_probability;
            out << ": " << bins[i].upper_bound;
        }
        out << " ";
    }
    out << "null_prob:" << null_probability;
    out << "]";
    out << "\n";
}

/*======================================================================================================================
 * Spn
 *====================================================================================================================*/

/*----- Learning helper methods --------------------------------------------------------------------------------------*/

std::unique_ptr<Spn::Product> Spn::create_product_min_slice(LearningData &ld)
{
    std::vector<std::unique_ptr<Product::ChildWithVariables>> children;
    auto variable_it = ld.variables.begin();
    for (auto i = 0; i < ld.data.cols(); i++) {
        const MatrixXf &data = ld.data.col(i);
        const MatrixXf &normalized = ld.normalized.col(i);
        const MatrixXi &null_matrix = ld.null_matrix.col(i);
        SmallBitset variables(variable_it.as_set());
        ++variable_it;
        std::vector<LeafType> split_leaf_types{ld.leaf_types[i]};
        LearningData split_data(
            data,
            normalized,
            null_matrix,
            variables,
            split_leaf_types
        );
        auto child = std::make_unique<Product::ChildWithVariables>(learn_node(split_data), variables);
        children.push_back(std::move(child));
    }
    return std::make_unique<Product>(std::move(children), ld.data.rows());
}


std::unique_ptr<Spn::Product> Spn::create_product_rdc(
    LearningData &ld,
    std::vector<SmallBitset> &column_candidates,
    std::vector<SmallBitset> &variable_candidates
)
{
    std::vector<std::unique_ptr<Product::ChildWithVariables>> children;
    for (std::size_t current_split = 0; current_split < column_candidates.size(); current_split++) {
        std::size_t split_size = column_candidates[current_split].size();
        std::vector<LeafType> split_leaf_types;
        split_leaf_types.reserve(split_size);
        std::vector<unsigned> column_index;
        column_index.reserve(split_size);
        for (auto it = column_candidates[current_split].begin(); it != column_candidates[current_split].end(); ++it) {
            split_leaf_types.push_back(ld.leaf_types[*it]);
            column_index.push_back(*it);
        }

        const MatrixXf &data = ld.data(all, column_index);
        const MatrixXf &normalized = ld.normalized(all, column_index);
        const MatrixXi &null_matrix = ld.null_matrix(all, column_index);
        LearningData split_data(data, normalized, null_matrix, variable_candidates[current_split], split_leaf_types);
        auto child = std::make_unique<Product::ChildWithVariables>(
            learn_node(split_data),
            variable_candidates[current_split]
        );
        children.push_back(std::move(child));
    }
    return std::make_unique<Product>(std::move(children), ld.data.rows());
}

std::unique_ptr<Spn::Sum> Spn::create_sum(Spn::LearningData &ld)
{
    const auto num_rows = ld.data.rows();

    int k = 2;

    unsigned prev_num_split_nodes = 0;
    std::vector<std::vector<SmallBitset>> prev_cluster_column_candidates;
    std::vector<std::vector<SmallBitset>> prev_cluster_variable_candidates;
    std::vector<std::vector<unsigned>> prev_cluster_row_ids;
    MatrixRXf prev_centroids;

    /* increment k of Kmeans until we get a good clustering according to RDC splits in the clusters */
    while (true) {
        unsigned num_split_nodes = 0;

        auto [labels, centroids] = kmeans_with_centroids(ld.normalized, k);

        std::vector<std::vector<SmallBitset>> cluster_column_candidates(k);
        std::vector<std::vector<SmallBitset>> cluster_variable_candidates(k);
        std::vector<std::vector<unsigned>> cluster_row_ids(k);

        for (unsigned current_row = 0; current_row < num_rows; current_row++) {
            cluster_row_ids[labels[current_row]].push_back(current_row);
        }

        /* if only one cluster, split this cluster into k clusters */
        bool only_one_cluster = not cluster_row_ids[0].empty();
        for (int i = 1; i < k; i++)
            only_one_cluster = only_one_cluster and cluster_row_ids[i].empty();

        if (only_one_cluster) {
            const std::size_t subvector_size = cluster_row_ids[0].size() / k;
            std::vector<std::vector<unsigned>> new_cluster_row_ids(k);
            for (std::size_t cluster_id = 0; cluster_id < k - 1; cluster_id++) {
                std::vector<unsigned> subvector(
                    cluster_row_ids[0].begin() + (cluster_id * subvector_size),
                    cluster_row_ids[0].begin() + ((cluster_id + 1) * subvector_size)
                );
                new_cluster_row_ids[cluster_id] = std::move(subvector);
            }
            std::vector<unsigned> last_subvector(
                cluster_row_ids[0].begin() + ((k - 1) * subvector_size),
                cluster_row_ids[0].end()
            );
            new_cluster_row_ids[k - 1] = std::move(last_subvector);

            cluster_row_ids = std::move(new_cluster_row_ids);
        }

        /* check the splitting of attributes in each cluster */
        for (unsigned label_id = 0; label_id < k; label_id++) {
            std::size_t cluster_size = cluster_row_ids[label_id].size();
            if (cluster_size == 0) { continue; }

            const MatrixXf &data = ld.data(cluster_row_ids[label_id], all);

            if (cluster_size <= MIN_INSTANCE_SLICE) {
                num_split_nodes++;
                cluster_column_candidates[label_id] = std::vector<SmallBitset>();
                cluster_variable_candidates[label_id] = std::vector<SmallBitset>();
            } else {
                auto [current_column_candidates, current_variable_candidates] = rdc_split(data, ld.variables);
                if (current_column_candidates.size() > 1) { num_split_nodes++; }
                cluster_column_candidates[label_id] = std::move(current_column_candidates);
                cluster_variable_candidates[label_id] = std::move(current_variable_candidates);
            }
        }

        /* if the number of split attributes does not increase or if there is a split in each cluster, build sum node */
        if (
            ((num_split_nodes <= prev_num_split_nodes or prev_num_split_nodes == prev_cluster_row_ids.size())
             and prev_num_split_nodes != 0) or k >= MAX_K
        ) {
            std::vector<std::unique_ptr<Sum::ChildWithWeight>> children;
            for (std::size_t cluster_id = 0; cluster_id < k - 1; cluster_id++) {
                const MatrixXf &data = ld.data(prev_cluster_row_ids[cluster_id], all);
                const MatrixXf &normalized = ld.normalized(prev_cluster_row_ids[cluster_id], all);
                const MatrixXi &null_matrix = ld.null_matrix(prev_cluster_row_ids[cluster_id], all);
                LearningData cluster_data(data, normalized, null_matrix, ld.variables, ld.leaf_types);
                const float weight = float(data.rows())/float(num_rows);
                std::size_t cluster_vertical_partitions = prev_cluster_column_candidates[cluster_id].size();

                /* since we already determined the node type of each cluster (child of the sum node), we can
                 * directly build the children of the sum node */
                std::unique_ptr<Node> child_node;
                if (cluster_vertical_partitions == 0) {
                    child_node = create_product_min_slice(cluster_data);
                } else if (cluster_vertical_partitions == 1) {
                    child_node = create_sum(cluster_data);
                } else {
                    child_node = create_product_rdc(
                        cluster_data,
                        prev_cluster_column_candidates[cluster_id],
                        prev_cluster_variable_candidates[cluster_id]
                    );
                }
                std::unique_ptr<Sum::ChildWithWeight> child = std::make_unique<Sum::ChildWithWeight>(
                    std::move(child_node),
                    weight,
                    prev_centroids.row(cluster_id)
                );

                children.push_back(std::move(child));
            }

            return std::make_unique<Sum>(std::move(children), num_rows);
        }
        prev_num_split_nodes = num_split_nodes;
        prev_cluster_column_candidates = std::move(cluster_column_candidates);
        prev_cluster_variable_candidates = std::move(cluster_variable_candidates);
        prev_cluster_row_ids = std::move(cluster_row_ids);
        prev_centroids = centroids;
        k++;
    }
}


std::unique_ptr<Spn::Node> Spn::learn_node(LearningData &ld)
{
    const auto num_rows = ld.data.rows();
    const auto num_cols = ld.data.cols();

    /* build leaf */
    if (num_cols == 1) {
        if (ld.leaf_types[0] == DISCRETE) {
            std::vector<DiscreteLeaf::Bin> bins;
            /* If every value is NULL */
            if (ld.null_matrix.minCoeff() == 1) {
                return std::make_unique<DiscreteLeaf>(std::move(bins), 1.f, num_rows);
            }
            unsigned null_counter = 0;
            for (auto i = 0; i < num_rows; i++) {
                if (ld.null_matrix(i, 0) == 1) {
                    null_counter++;
                    continue;
                }
                const auto current_value = ld.data(i, 0);
                /* insert value into sorted vector of bins */
                auto lower_bound = std::lower_bound(bins.begin(), bins.end(), current_value);
                if (lower_bound == bins.end()) { bins.emplace_back(current_value, 1.f); }
                /* if bin already exists, increment counter */
                else if (lower_bound->value == current_value) { lower_bound->cumulative_probability++; }
                else { bins.emplace(lower_bound, current_value, 1.f); }
            }

            bins[0].cumulative_probability /= float(num_rows);
            for (std::size_t i = 1; i < bins.size(); i++) {
                bins[i].cumulative_probability /= float(num_rows);
                bins[i].cumulative_probability += bins[i-1].cumulative_probability;
            }
            return std::make_unique<DiscreteLeaf>(std::move(bins), null_counter / float(num_rows), num_rows);
        } else {
            std::vector<ContinuousLeaf::Bin> bins;
            /* If every value is NULL */
            if (ld.null_matrix.minCoeff() == 1) {
                return std::make_unique<ContinuousLeaf>(std::move(bins), 0.f, 0.f, 1.f, num_rows);
            }
            const float max = ld.data.maxCoeff();
            const float min = ld.data.minCoeff();
            const auto num_bins = 1 + log2_ceil(std::size_t(num_rows));
            const float bin_width = (max - min) / float(num_bins);
            const float lower_bound = min;
            unsigned lower_bound_counter = 0.f;
            unsigned null_counter = 0.f;

            /* initialise bins */
            bins.reserve(num_bins);
            for(std::size_t bin_id = 0; bin_id < num_bins; bin_id++) {
                bins.emplace_back(min + (float(bin_id + 1) * bin_width), 0.f);
            }

            /* fill the values into the existing bins */
            for (unsigned i = 0; i < num_rows; i++) {
                if (ld.null_matrix(i, 0) == 1) {
                    null_counter++;
                    continue;
                }
                const auto current_value = ld.data(i, 0);
                if (current_value == lower_bound) {
                    lower_bound_counter++;
                    continue;
                }
                auto std_lower_bound = std::lower_bound(bins.begin(), bins.end(), current_value);
                std_lower_bound->cumulative_probability++;
            }

            float lower_bound_probability = lower_bound_counter / float(num_rows);
            bins[0].cumulative_probability /= float(num_rows);
            bins[0].cumulative_probability += lower_bound_probability;

            for (std::size_t i = 1; i < bins.size(); i++) {
                bins[i].cumulative_probability /= float(num_rows);
                bins[i].cumulative_probability += bins[i-1].cumulative_probability;
            }
            return std::make_unique<ContinuousLeaf>(
                    std::move(bins),
                    lower_bound,
                    lower_bound_probability,
                null_counter / float(num_rows),
                    num_rows
            );
        }
    }

    /* build product node with the minimum instance slice */
    if (num_rows <= MIN_INSTANCE_SLICE) { return create_product_min_slice(ld); }

    /* build product node with the RDC algorithm */
    auto [column_candidates, variable_candidates] = rdc_split(ld.data, ld.variables);
    if (column_candidates.size() != 1) { return create_product_rdc(ld, column_candidates, variable_candidates); }

    /* build sum node */
    else { return create_sum(ld); }
}

/*----- Learning -----------------------------------------------------------------------------------------------------*/

Spn Spn::learn_spn(Eigen::MatrixXf &data, Eigen::MatrixXi &null_matrix, std::vector<LeafType> &leaf_types)
{
    std::size_t num_rows = data.rows();
    MIN_INSTANCE_SLICE = std::max<std::size_t>((0.1 * num_rows), 1);

    if (num_rows == 0) {
        std::vector<DiscreteLeaf::Bin> bins;
        return Spn(0, std::make_unique<DiscreteLeaf>(std::move(bins), 0, 0));
    }

    /* replace NULL in the data matrix with the mean of the attribute */
    for (std::size_t col_id = 0; col_id < data.cols(); col_id++) {
        if (null_matrix.col(col_id).maxCoeff() == 0) { continue; } // there is no NULL
        if (null_matrix.col(col_id).minCoeff() == 1) { continue; } // there is only NULL
        float mean = 0;
        int num_not_null = 0;
        for (std::size_t row_id = 0; row_id < data.rows(); row_id++) {
            if (null_matrix(row_id, col_id) == 1) { continue; }
            mean += (data(row_id, col_id) - mean) / ++num_not_null; // iterative mean
        }
        for (std::size_t row_id = 0; row_id < data.rows(); row_id++) {
            if (null_matrix(row_id, col_id) == 1) { data(row_id, col_id) = mean; }
        }
    }

    SmallBitset variables((1UL << data.cols()) - 1);

    auto normalized = normalize_minmax(data);
    LearningData ld(
        data,
        normalized,
        null_matrix,
        variables,
        std::move(leaf_types)
    );

    return Spn(num_rows, learn_node(ld));
}

/*----- Inference ----------------------------------------------------------------------------------------------------*/

void Spn::update(VectorXf &row, UpdateType update_type)
{
    SmallBitset variables((1 << row.size()) - 1);
    root_->update(row, variables, update_type);
}

float Spn::likelihood(const Filter &filter) const
{
    return root_->evaluate(filter, filter.begin()->first, APPROXIMATE).second;
}

float Spn::upper_bound(const Filter &filter) const
{
    return root_->evaluate(filter, filter.begin()->first, UPPER_BOUND).second;
}

float Spn::lower_bound(const Filter &filter) const
{
    return root_->evaluate(filter, filter.begin()->first, LOWER_BOUND).second;
}

float Spn::expectation(unsigned attribute_id, const Filter &filter) const
{
    auto filter_copy = filter;
    filter_copy.emplace(attribute_id, std::make_pair(EXPECTATION, 0.f));

    auto [cond_expectation, likelihood] = root_->evaluate(filter_copy, filter_copy.begin()->first, APPROXIMATE);
    float llh = 1.f;
    if (!filter.empty()) {
        if (likelihood == 0) { return 0; }
        llh = likelihood;
    }

    return cond_expectation / llh;
}

void Spn::update_row(VectorXf &old_row, VectorXf &updated_row)
{
    delete_row(old_row);
    insert_row(updated_row);
}

void Spn::insert_row(VectorXf &row)
{
    update(row, INSERT);
    num_rows_++;
}

void Spn::delete_row(VectorXf &row)
{
    update(row, DELETE);
    num_rows_--;
}

std::size_t Spn::estimate_number_distinct_values(unsigned attribute_id) const
{
    return root_->estimate_number_distinct_values(attribute_id);
}

void Spn::dump() const { dump(std::cerr); }

void Spn::dump(std::ostream &out) const
{
    out << "\n";
    root_->dump(out);
}
