#pragma once

#include <Eigen/Core>
#include <iostream>
#include <map>
#include <memory>
#include "mutable/util/ADT.hpp"
#include <set>
#include <type_traits>
#include <unordered_map>
#include <vector>


namespace m {

/**
 * Tree structure for Sum Product Networks
 */
struct Spn
{
    public:
    /** The different types of leaves for an attribute. `AUTO` if `learn_spn` should choose the leaf type by itself. */
    enum LeafType {
        AUTO,
        DISCRETE,
        CONTINUOUS
    };
    enum SpnOperator {
        EQUAL,
        LESS,
        LESS_EQUAL,
        GREATER,
        GREATER_EQUAL,
        IS_NULL,
        EXPECTATION
    };
    enum EvalType {
        APPROXIMATE,
        UPPER_BOUND,
        LOWER_BOUND
    };
    enum UpdateType {
        INSERT,
        DELETE
    };

    using Filter = std::unordered_map<unsigned, std::pair<SpnOperator, float>>;

    private:

    struct LearningData
    {
        const Eigen::MatrixXf &data;
        const Eigen::MatrixXf &normalized;
        const Eigen::MatrixXi &null_matrix;
        SmallBitset variables;
        std::vector<LeafType> leaf_types;

        LearningData(
            const Eigen::MatrixXf &data,
            const Eigen::MatrixXf &normalized,
            const Eigen::MatrixXi &null_matrix,
            SmallBitset variables,
            std::vector<LeafType> leaf_types
        )
            : data(data)
            , normalized(normalized)
            , null_matrix(null_matrix)
            , variables(variables)
            , leaf_types(std::move(leaf_types))
        { }
    };

    struct Node
    {
        std::size_t num_rows;

        explicit Node(std::size_t num_rows) : num_rows(num_rows) { }

        virtual ~Node() = default;
        void dump() const;
        void dump(std::ostream &out) const;

        /** Evaluate the SPN bottom up with a filter condition.
         *
         * @param filter    the filter condition
         * @param eval_type for continuous leaves to test bin accuracy
         * @return          a pair <conditional expectation, likelihood> (cond. expectation undefined if only likelihood
         *                  is evaluated)
         */
        virtual std::pair<float, float> evaluate(const Filter &filter, unsigned leaf_id, EvalType eval_type) const = 0;

        virtual void update(Eigen::VectorXf &row, SmallBitset variables, UpdateType update_type) = 0;

        virtual std::size_t estimate_number_distinct_values(unsigned id) const = 0;

        virtual unsigned height() const = 0;
        virtual unsigned breadth() const = 0;
        virtual unsigned degree() const = 0;
        virtual std::size_t memory_usage() const = 0;

        virtual void print(std::ostream &out, std::size_t num_tabs) const = 0;
    };

    struct Sum : Node
    {
        struct ChildWithWeight
        {
            std::unique_ptr<Node> child;
            float weight; ///< weight of a child of a sum node
            Eigen::VectorXf centroid; ///< centroid of this child according to kmeans cluster

            ChildWithWeight(std::unique_ptr<Node> child, float weight, Eigen::VectorXf centroid)
                : child(std::move(child))
                , weight(weight)
                , centroid(std::move(centroid))
            { }
        };

        std::vector<std::unique_ptr<ChildWithWeight>> children;

        Sum(std::vector<std::unique_ptr<ChildWithWeight>> children, std::size_t num_rows)
            : Node(num_rows)
            , children(std::move(children))
        { }

        std::pair<float, float> evaluate(const Filter &filter, unsigned leaf_id, EvalType eval_type) const override;

        void update(Eigen::VectorXf &row, SmallBitset variables, UpdateType update_type) override;

        std::size_t estimate_number_distinct_values(unsigned id) const override;

        unsigned height() const override {
            unsigned max_height = 0;
            for (auto &child : children) { max_height = std::max(max_height, child->child->height()); }
            return 1 + max_height;
        }
        unsigned breadth() const override {
            unsigned breadth = 0;
            for (auto &child : children) { breadth += child->child->breadth(); }
            return breadth;
        }
        unsigned degree() const override {
            unsigned max_degree = children.size();
            for (auto &child : children) { max_degree = std::max(max_degree, child->child->degree()); }
            return max_degree;
        }
        std::size_t memory_usage() const override {
            std::size_t memory_used = 0;
            memory_used += sizeof *this + children.size() * sizeof(decltype(children)::value_type);
            for (auto &child : children) {
                memory_used += child->child->memory_usage();
            }
            return memory_used;
        }

        void print(std::ostream &out, std::size_t num_tabs) const override;
    };

    struct Product : Node
    {
        struct ChildWithVariables
        {
            std::unique_ptr<Node> child; ///< a child of the Product node
            SmallBitset variables; ///< the set of variables(attributes), that are in this child

            ChildWithVariables(std::unique_ptr<Node> child, SmallBitset variables)
                : child(std::move(child))
                , variables(variables)
            { }
        };

        std::vector<std::unique_ptr<ChildWithVariables>> children;

        Product(std::vector<std::unique_ptr<ChildWithVariables>> children, std::size_t num_rows)
            : Node(num_rows)
            , children(std::move(children))
        { }

        std::pair<float, float> evaluate(const Filter &filter, unsigned leaf_id, EvalType eval_type) const override;

        void update(Eigen::VectorXf &row, SmallBitset variables, UpdateType update_type) override;

        std::size_t estimate_number_distinct_values(unsigned id) const override;

        unsigned height() const override {
            unsigned max_height = 0;
            for (auto &child : children) { max_height = std::max(max_height, child->child->height()); }
            return 1 + max_height;
        }
        unsigned breadth() const override {
            unsigned breadth = 0;
            for (auto &child : children) { breadth += child->child->breadth(); }
            return breadth;
        }
        unsigned degree() const override {
            unsigned max_degree = children.size();
            for (auto &child : children) { max_degree = std::max(max_degree, child->child->degree()); }
            return max_degree;
        }
        std::size_t memory_usage() const override {
            std::size_t memory_used = 0;
            memory_used += sizeof *this + children.size() * sizeof(decltype(children)::value_type);
            for (auto &child : children) {
                memory_used += child->variables.size() * sizeof child->variables;
                memory_used += child->child->memory_usage();
            }
            return memory_used;
        }

        void print(std::ostream &out, std::size_t num_tabs) const override;
    };

    struct DiscreteLeaf : Node
    {
        struct Bin
        {
            float value; ///< the value of this bin
            ///> the cumulative probability of this and all predecessor bins; in the range [0;1]
            float cumulative_probability;

            Bin(float value, float cumulative_probability)
                : value(value)
                , cumulative_probability(cumulative_probability)
            { }

            bool operator<(Bin &other) const { return this->value < other.value; }
            bool operator<(float other) const { return this->value < other; }
        };

        std::vector<Bin> bins; ///< bins of this leaf
        float null_probability; ///< the probability of null values in this leaf

        DiscreteLeaf(std::vector<Bin> bins, float null_probability, std::size_t num_rows)
            : Node(num_rows)
            , bins(std::move(bins))
            , null_probability(null_probability)
        { }

        std::pair<float, float> evaluate(const Filter &bin_value, unsigned leaf_id, EvalType eval_type) const override;

        void update(Eigen::VectorXf &row, SmallBitset variables, UpdateType update_type) override;

        std::size_t estimate_number_distinct_values(unsigned id) const override;

        unsigned height() const override { return 0; }
        unsigned breadth() const override { return 1; }
        unsigned degree() const override { return 0; }
        std::size_t memory_usage() const override {
            return sizeof *this + bins.size() * sizeof(decltype(bins)::value_type);
        }
        void print(std::ostream &out, std::size_t num_tabs) const override;
    };

    struct ContinuousLeaf : Node
    {
        struct Bin
        {
            float upper_bound; ///< the upper bound of this bin
            ///> the cumulative probability of this and all predecessor bins; in the range [0;1]
            float cumulative_probability;

            Bin(float upper_bound, float cumulative_probability)
                : upper_bound(upper_bound)
                , cumulative_probability(cumulative_probability)
            { }

            bool operator<(Bin &other) const { return this->upper_bound < other.upper_bound; }
            bool operator<(float other) const { return this->upper_bound < other; }
        };

        std::vector<Bin> bins; ///< bins of this leaf
        float lower_bound; ///< the lower bound of the first bin
        float lower_bound_probability; ///< probability of the lower_bound
        float null_probability; ///< the probability of null values in this leaf

        ContinuousLeaf(
            std::vector<Bin> bins,
            float lower_bound,
            float lower_bound_probability,
            float null_probability,
            std::size_t num_rows
        )
            : Node(num_rows)
            , bins(std::move(bins))
            , lower_bound(lower_bound)
            , lower_bound_probability(lower_bound_probability)
            , null_probability(null_probability)
        { }

        std::pair<float, float> evaluate(const Filter &filter, unsigned leaf_id, EvalType eval_type) const override;

        void update(Eigen::VectorXf &row, SmallBitset variables, UpdateType update_type) override;

        std::size_t estimate_number_distinct_values(unsigned id) const override;

        unsigned height() const override { return 0; }
        unsigned breadth() const override { return 1; }
        unsigned degree() const override { return 0; }
        std::size_t memory_usage() const override {
            return sizeof *this + bins.size() * sizeof(decltype(bins)::value_type);
        }
        void print(std::ostream &out, std::size_t num_tabs) const override;
    };

    std::size_t num_rows_;
    std::unique_ptr<Node> root_;

    Spn(std::size_t num_rows, std::unique_ptr<Node> root) : num_rows_(num_rows), root_(std::move(root)) { }

    public:

    /** returns the number of rows in the SPN. */
    std::size_t num_rows() const { return num_rows_; }

    /*==================================================================================================================
     * Learning
     *================================================================================================================*/

    private:

    /** Create a product node by splitting all columns */
    static std::unique_ptr<Spn::Product> create_product_min_slice(LearningData &learning_data);

    /** Create a product node with the given candidates (vertical clustering) */
    static std::unique_ptr<Product> create_product_rdc(
        LearningData &learning_data,
        std::vector<SmallBitset> &column_candidates,
        std::vector<SmallBitset> &variable_candidates
    );

    /** Create a sum node by clustering the rows */
    static std::unique_ptr<Spn::Sum> create_sum(LearningData &learning_data);

    /** Recursively learns the nodes of an SPN. */
    static std::unique_ptr<Node> learn_node(LearningData &learning_data);

    public:

    /** Learn an SPN over the given data.
     *
     * @param data              the data
     * @param null_matrix       the NULL values of the data as a matrix
     * @param attribute_to_id   a map from the attributes (random variables) to internal id
     * @param leaf_types        the types of a leaf for a non-primary key attribute
     * @return                  the learned SPN
     */
    static Spn learn_spn(Eigen::MatrixXf &data, Eigen::MatrixXi &null_matrix, std::vector<LeafType> &leaf_types);

    /*==================================================================================================================
     * Inference
     *================================================================================================================*/

    private:

    /** Update the SPN from the top down and adjust weights of sum nodes and the distributions on leaves.
     *
     * @param row the row to update in the SPN
     * @param update_type the type of update (insert or delete)
     */
    void update(Eigen::VectorXf &row, UpdateType update_type);

    public:

    /** Compute the likelihood of the given filter predicates given by a map from attribute to the
     * respective operator and value. The predicates in the map are seen as conjunctions. */
    float likelihood(const Filter &filter) const;

    /** Compute the upper bound probability for continuous domains. */
    float upper_bound(const Filter &filter) const;

    /** Compute the lower bound probability for continuous domains. */
    float lower_bound(const Filter &filter) const;

    /** Compute the expectation of the given attribute. */
    float expectation(unsigned attribute_id, const Filter &filter) const;

    /** Update the SPN with the given row. */
    void update_row(Eigen::VectorXf &old_row, Eigen::VectorXf &updated_row);

    /** Insert the given row into the SPN. */
    void insert_row(Eigen::VectorXf &row);

    /** Delete the given row from the SPN. */
    void delete_row(Eigen::VectorXf &row);

    /** Estimate the number of distinct values of the given attribute. */
    std::size_t estimate_number_distinct_values(unsigned attribute_id) const;

    unsigned height() const { return root_->height(); }
    unsigned breadth() const { return root_->breadth(); }
    unsigned degree() const { return root_->degree(); }
    std::size_t memory_usage() const {
        std::size_t memory_used = 0;
        memory_used += sizeof root_;
        memory_used += root_->memory_usage();
        return memory_used;
    }

    void dump() const;
    void dump(std::ostream &out) const;
};

}
