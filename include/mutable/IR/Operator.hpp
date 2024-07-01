#pragma once

#include <algorithm>
#include <memory>
#include <mutable/mutable-config.hpp>
#include <mutable/catalog/Schema.hpp>
#include <mutable/IR/CNF.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/enum_ops.hpp>
#include <mutable/util/macro.hpp>
#include <functional>
#include <iostream>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>


namespace m {

// forward declarations
struct OperatorVisitor;
struct ConstOperatorVisitor;
struct Tuple;

/** This class provides additional information about an `Operator`, e.g. the tables processed by this operator or the
 * estimated cardinality of its result set. */
struct M_EXPORT OperatorInformation
{
    ///> the subproblem processed by this `Operator`'s subplan
    QueryGraph::Subproblem subproblem;

    ///> the estimated cardinality of the result set of this `Operator`
    double estimated_cardinality;
};

/** This interface allows for attaching arbitrary data to `Operator` instances. */
struct M_EXPORT OperatorData
{
    virtual ~OperatorData() = 0;
};

/** An `Operator` represents an operation in a *query plan*.  A plan is a tree structure of `Operator`s.  `Operator`s
 * can be evaluated to a sequence of tuples and have a `Schema`. */
struct M_EXPORT Operator
{
    friend void swap(Operator &first, Operator &second) {
        using std::swap;
        swap(first.schema_, second.schema_);
        swap(first.info_,   second.info_);
        swap(first.data_,   second.data_);
        swap(first.id_,     second.id_);
    }

    private:
    Schema schema_; ///< the schema of this `Operator`
    std::unique_ptr<OperatorInformation> info_; ///< additional information about this `Operator`
    mutable OperatorData *data_ = nullptr; ///< the data object associated to this `Operator`; may be `nullptr`
    protected:
    mutable std::size_t id_ = -1UL; ///< the ID of this `Operator`; used as index in the DP table of `PhysicalOperator`

    public:
    Operator() = default;
    Operator(Operator &&other) : Operator() { swap(*this, other); }
    virtual ~Operator() { delete data_; }

    /** Returns the `Schema` of this `Operator`. */
    Schema & schema() { return schema_; }
    /** Returns the `Schema` of this `Operator`. */
    const Schema & schema() const { return schema_; }

    bool has_info() const { return bool(info_); }
    OperatorInformation & info() { M_insist(bool(info_)); return *info_; }
    const OperatorInformation & info() const { return const_cast<Operator*>(this)->info(); }
    std::unique_ptr<OperatorInformation> info(std::unique_ptr<OperatorInformation> new_info) {
        using std::swap;
        swap(new_info, info_);
        return new_info;
    }

    /** Assigns IDs to the operator tree rooted in `this` in post-order starting with ID `start_id`. */
    virtual void assign_post_order_ids(std::size_t start_id = 0UL) const { id_ = start_id; }
    /** Resets the IDs of the operator tree rooted in `this`. */
    virtual void reset_ids() const { id_ = -1UL; }
    /** Returns the ID of `this`. */
    std::size_t id() const {
        M_insist(id_ != -1UL, "id must be first set by calling `assign_post_order_ids()`");
        return id_;
    }
    private:
    /** Sets the ID of `this`. */
    void id(std::size_t id) const { id_ = id; }

    public:
    /** Attached `OperatorData` `data` to this `Operator`.  Returns the previously attached `OperatorData`.  May return
     * `nullptr`. */
    OperatorData * data(OperatorData *data) const { std::swap(data, data_); return data; }
    /** Returns the `OperatorData` attached to this `Operator`. */
    OperatorData * data() const { return data_; }

    virtual void accept(OperatorVisitor &v) = 0;
    virtual void accept(ConstOperatorVisitor &v) const = 0;

    friend M_EXPORT std::ostream & operator<<(std::ostream &out, const Operator &op);

    /** Minimizes the `Schema` of this `Operator`.  The `Schema` is reduced to the attributes actually
     * required by ancestors of this `Operator` in the plan. */
    void minimize_schema();

    /** Prints a representation of this `Operator` and its descendants in the dot language. */
    void dot(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;
};

struct Consumer;

/** A `Producer` is an `Operator` that can be evaluated to a sequence of tuples. */
struct M_EXPORT Producer : virtual Operator
{
    private:
    Consumer *parent_; ///< the parent of this `Producer`

    public:
    /** Returns the parent of this `Producer`. */
    Consumer * parent() const { return parent_; }
    /** Sets the parent of this `Producer`.  Returns the previous parent.  May return `nullptr`. */
    Consumer * parent(Consumer *c) { std::swap(parent_, c); return c; }
};

/** A `Consumer` is an `Operator` that can be evaluated on a sequence of tuples. */
struct M_EXPORT Consumer : virtual Operator
{
    private:
    std::vector<Producer*> children_; ///< the children of this `Consumer`

    public:
    Consumer() = default;
    Consumer(Consumer&&) = default;
    virtual ~Consumer() {
        for (auto c : children_)
            delete c;
    }

    /** Adds a `child` to this `Consumer` and updates this `Consumer`s schema accordingly. */
    virtual void add_child(Producer *child) {
        if (not child)
            throw invalid_argument("no child given");
        children_.push_back(child);
        child->parent(this);
        schema() += child->schema();
    }
    /** Sets the `i`-th `child` of this `Consumer`.  Forces a recomputation of this `Consumer`s schema. */
    virtual Producer * set_child(Producer *child, std::size_t i) {
        if (not child)
            throw invalid_argument("no child given");
        if (i >= children_.size())
            throw out_of_range("index i out of bounds");
        auto old = children_[i];
        children_[i] = child;
        child->parent(this);

        /* Recompute operator schema. */
        auto &S = schema();
        S = Schema();
        for (auto c : children_)
            S += c->schema();

        return old;
    }

    /** Returns a reference to the children of this `Consumer`. */
    const std::vector<Producer*> & children() const { return children_; }

    /** Returns the `i`-th child of this `Consumer`. */
    Producer * child(std::size_t i) const {
        if (i >= children_.size())
            throw out_of_range("index i out of bounds");
        return children_[i];
    }

    protected:
    std::vector<Producer*> & children() { return children_; }

    public:
    void assign_post_order_ids(std::size_t start_id = 0UL) const override {
        auto next_start_id = start_id;
        for (auto c : children_) {
            c->assign_post_order_ids(next_start_id);
            next_start_id = c->id() + 1;
        }
        id_ = next_start_id;
    }
    void reset_ids() const override {
        id_ = -1UL;
        for (auto c : children_)
            c->reset_ids();
    }
};

struct M_EXPORT CallbackOperator : Consumer
{
    using callback_type = std::function<void(const Schema &, const Tuple&)>;

    private:
    callback_type callback_;

    public:
    CallbackOperator(callback_type callback) : callback_(std::move(callback)) { }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    CallbackOperator clone_node() const { return CallbackOperator(callback_); }

    const auto & callback() const { return callback_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

/** Prints the produced `Tuple`s to a `std::ostream` instance. */
struct M_EXPORT PrintOperator : Consumer
{
    std::ostream &out;

    PrintOperator(std::ostream &out) : out(out) { }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    PrintOperator clone_node() const { return PrintOperator(out); }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

/** Drops the produced results and outputs only the number of result tuples produced.  This is used for benchmarking. */
struct M_EXPORT NoOpOperator : Consumer
{
    std::ostream &out;

    NoOpOperator(std::ostream &out) : out(out) { }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    NoOpOperator clone_node() const { return NoOpOperator(out); }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT ScanOperator : Producer
{
    private:
    const Store &store_;
    ThreadSafePooledString alias_;

    public:
    ScanOperator(const Store &store, ThreadSafePooledString alias)
        : store_(store)
        , alias_(std::move(alias))
    {
        auto &S = schema();
        for (auto &e : store.table().schema())
            S.add({alias_, e.id.name}, e.type, e.constraints);
    }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    ScanOperator clone_node() const { return ScanOperator(store_, alias_); }

    const Store & store() const { return store_; }
    const ThreadSafePooledString & alias() const { return alias_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT FilterOperator : Producer, Consumer
{
    protected:
    cnf::CNF filter_;

    public:
    FilterOperator(cnf::CNF filter) : filter_(std::move(filter)) { }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    FilterOperator clone_node() const { return FilterOperator(filter_); }

    const cnf::CNF & filter() const { return filter_; }
    const cnf::CNF filter(cnf::CNF f) {
        auto old_filter = std::move(filter_);
        filter_ = std::move(f);
        return old_filter;
    }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT DisjunctiveFilterOperator : FilterOperator
{
    DisjunctiveFilterOperator(cnf::CNF filter)
        : FilterOperator(std::move(filter))
    {
        M_insist(this->filter().size() == 1, "a disjunctive filter must have exactly one clause");
        M_insist(this->filter()[0].size() >= 2, "a disjunctive filter must have at least two predicates ");
    }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    DisjunctiveFilterOperator clone_node() const { return DisjunctiveFilterOperator(filter_); }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT JoinOperator : Producer, Consumer
{
    static constexpr Schema::entry_type::constraints_t REMOVED_CONSTRAINTS =
        Schema::entry_type::UNIQUE | Schema::entry_type::REFERENCES_UNIQUE; // FIXME: still unique for 1:1 joins and depends on what was referenced

    private:
    cnf::CNF predicate_;

    public:
    JoinOperator(cnf::CNF predicate) : predicate_(std::move(predicate)) { }

    /*----- Override child setters to correctly adapt the constraints of the computed schema! ------------------------*/
    virtual void add_child(Producer *child) override {
        const auto old_num_entries = schema().num_entries();
        Consumer::add_child(child); // delegate to inherited method

        /* Remove uniqueness constraints (in added schema part) due to denormalization. */
        for (auto i = old_num_entries; i != schema().num_entries(); ++i)
            schema()[i].constraints &= ~REMOVED_CONSTRAINTS;
    }
    virtual Producer * set_child(Producer *child, std::size_t i) override {
        auto old = Consumer::set_child(child, i); // delegate to inherited method

        /* Remove uniqueness constraints (in entire schema because of recomputation) due to denormalization. */
        for (auto &e : schema())
            e.constraints &= ~REMOVED_CONSTRAINTS;

        return old;
    }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    JoinOperator clone_node() const { return JoinOperator(predicate_); }

    const cnf::CNF & predicate() const { return predicate_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT SemiJoinReductionOperator : Producer, Consumer
{
    using projection_type = QueryGraph::projection_type;

    /** Represents the order in which the semi-join between two `DataSource`s is performed. */
    struct semi_join_order_t
    {
        const DataSource &lhs;
        const DataSource &rhs;
        semi_join_order_t(const DataSource &lhs, const DataSource &rhs) : lhs(lhs), rhs(rhs) { }

        friend std::ostream & operator<<(std::ostream &out, const semi_join_order_t &order) {
            out << '(' << order.lhs.name() << ',' << order.rhs.name() << ')';
            return out;
        }

        void dump(std::ostream &out) const;
        void dump() const;
    };

    private:
    std::vector<projection_type> projections_;
    std::vector<std::unique_ptr<DataSource>> sources_; ///< collection of all data sources
    std::vector<std::unique_ptr<Join>> joins_; ///< collection of all joins
    ///> contains the semi-join reduction order, by convention, the lhs is the `DataSource` ``closer'' to the root
    std::vector<semi_join_order_t> semi_join_reduction_order_;

    public:
    SemiJoinReductionOperator(std::vector<projection_type> projections);

    /*----- Override child setters to *NOT* modify the computed schema! ----------------------------------------------*/
    virtual void add_child(Producer *child) override {
        if (not child)
            throw invalid_argument("no child given");
        children().push_back(child);
        child->parent(this);

        /* Add constraints from child to computed schema. */
        auto &S = schema();
        for (std::size_t i = 0; i < projections_.size(); ++i) {
            if (auto D = cast<const ast::Designator>(projections_[i].first)) {
                Schema::Identifier id(D->table_name.text, D->attr_name.text.assert_not_none());
                if (auto it = child->schema().find(id); it != child->schema().end())
                    S[i].constraints |= it->constraints;
            }
        }
    }
    virtual Producer * set_child(Producer *child, std::size_t i) override {
        if (not child)
            throw invalid_argument("no child given");
        if (i >= children().size())
            throw out_of_range("index i out of bounds");
        auto old = children()[i];
        children()[i] = child;
        child->parent(this);

        /* Add constraints from child to computed schema. */
        auto &S = schema();
        for (std::size_t i = 0; i < projections_.size(); ++i) {
            if (auto D = cast<const ast::Designator>(projections_[i].first)) {
                Schema::Identifier id(D->table_name.text, D->attr_name.text.assert_not_none());
                if (auto it = child->schema().find(id); it != child->schema().end())
                    S[i].constraints |= it->constraints;
            }
        }

        return old;
    }

    std::vector<projection_type> & projections() { return projections_; }
    const std::vector<projection_type> & projections() const { return projections_; }

    std::vector<std::unique_ptr<DataSource>> & sources() { return sources_; }
    const std::vector<std::unique_ptr<DataSource>> & sources() const { return sources_; }

    std::vector<std::unique_ptr<Join>> & joins() { return joins_; }
    const std::vector<std::unique_ptr<Join>> & joins() const { return joins_; }

    std::vector<semi_join_order_t> & semi_join_reduction_order() { return semi_join_reduction_order_; }
    const std::vector<semi_join_order_t> & semi_join_reduction_order() const { return semi_join_reduction_order_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

/** Decompose the single-table result into multiple result sets, i.e. compute the same result as the
 * `SemiJoinReductionOperator`. */
struct M_EXPORT DecomposeOperator : Consumer, Producer
{
    using projection_type = QueryGraph::projection_type;

    std::ostream &out;
    private:
    std::vector<projection_type> projections_;
    std::vector<std::unique_ptr<DataSource>> sources_; ///< collection of all data sources

    public:
    DecomposeOperator(std::ostream &out, std::vector<projection_type> projections,
                      std::vector<std::unique_ptr<DataSource>> sources);

    std::vector<projection_type> & projections() { return projections_; }
    const std::vector<projection_type> & projections() const { return projections_; }

    std::vector<std::unique_ptr<DataSource>> & sources() { return sources_; }
    const std::vector<std::unique_ptr<DataSource>> & sources() const { return sources_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT ProjectionOperator : Producer, Consumer
{
    using projection_type = QueryGraph::projection_type;

    private:
    std::vector<projection_type> projections_;

    public:
    ProjectionOperator(std::vector<projection_type> projections);

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    ProjectionOperator clone_node() const { return ProjectionOperator(projections_); }

    /*----- Override child setters to *NOT* modify the computed schema! ----------------------------------------------*/
    virtual void add_child(Producer *child) override {
        if (not child)
            throw invalid_argument("no child given");
        children().push_back(child);
        child->parent(this);

        /* Add constraints from child to computed schema. */
        auto &S = schema();
        for (std::size_t i = 0; i < projections_.size(); ++i) {
            if (auto D = cast<const ast::Designator>(projections_[i].first)) {
                Schema::Identifier id(D->table_name.text, D->attr_name.text.assert_not_none());
                S[i].constraints |= child->schema()[id].second.constraints;
            }
        }
    }
    virtual Producer * set_child(Producer *child, std::size_t i) override {
        if (not child)
            throw invalid_argument("no child given");
        if (i >= children().size())
            throw out_of_range("index i out of bounds");
        auto old = children()[i];
        children()[i] = child;
        child->parent(this);

        /* Add constraints from child to computed schema. */
        auto &S = schema();
        for (std::size_t i = 0; i < projections_.size(); ++i) {
            if (auto D = cast<const ast::Designator>(projections_[i].first)) {
                Schema::Identifier id(D->table_name.text, D->attr_name.text.assert_not_none());
                S[i].constraints |= child->schema()[id].second.constraints;
            }
        }

        return old;
    }

    const std::vector<projection_type> & projections() const { return projections_; }
    std::vector<projection_type> & projections() { return projections_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT LimitOperator : Producer, Consumer
{
    /* This class is used to unwind the stack when the limit of produced tuples is reached. */
    struct stack_unwind : std::exception { };

    private:
    std::size_t limit_;
    std::size_t offset_;

    public:
    LimitOperator(std::size_t limit, std::size_t offset)
        : limit_(limit)
        , offset_(offset)
    { }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    LimitOperator clone_node() const { return LimitOperator(limit_, offset_); }

    std::size_t limit() const { return limit_; }
    std::size_t offset() const { return offset_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT GroupingOperator : Producer, Consumer
{
    using group_type = QueryGraph::group_type;

    private:
    std::vector<group_type> group_by_; ///< the compound grouping key
    std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates_; ///< the aggregates to compute

    public:
    GroupingOperator(std::vector<group_type> group_by,
                     std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates);

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    GroupingOperator clone_node() const { return GroupingOperator(group_by_, aggregates_); }

    /*----- Override child setters to *NOT* modify the computed schema! ----------------------------------------------*/
    virtual void add_child(Producer *child) override {
        if (not child)
            throw invalid_argument("no child given");
        children().push_back(child);
        child->parent(this);

        /* Add constraints from child to computed schema. */
        auto &S = schema();
        for (std::size_t i = 0; i < group_by_.size(); ++i) {
            if (auto D = cast<const ast::Designator>(group_by_[i].first)) {
                Schema::Identifier id(D->table_name.text, D->attr_name.text.assert_not_none());
                S[i].constraints |= child->schema()[id].second.constraints;
            }
        }
    }
    virtual Producer * set_child(Producer *child, std::size_t i) override {
        if (not child)
            throw invalid_argument("no child given");
        if (i >= children().size())
            throw out_of_range("index i out of bounds");
        auto old = children()[i];
        children()[i] = child;
        child->parent(this);

        /* Add constraints from child to computed schema. */
        auto &S = schema();
        for (std::size_t i = 0; i < group_by_.size(); ++i) {
            if (auto D = cast<const ast::Designator>(group_by_[i].first)) {
                Schema::Identifier id(D->table_name.text, D->attr_name.text.assert_not_none());
                S[i].constraints |= child->schema()[id].second.constraints;
            }
        }

        return old;
    }

    const auto & group_by() const { return group_by_; }
    const auto & aggregates() const { return aggregates_; }
    auto & aggregates() { return aggregates_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT AggregationOperator : Producer, Consumer
{
    private:
    std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates_; ///< the aggregates to compute

    public:
    AggregationOperator(std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates);

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    AggregationOperator clone_node() const { return AggregationOperator(aggregates_); }

    /*----- Override child setters to *NOT* modify the computed schema! ----------------------------------------------*/
    virtual void add_child(Producer *child) override {
        if (not child)
            throw invalid_argument("no child given");
        children().push_back(child);
        child->parent(this);
    }
    virtual Producer * set_child(Producer *child, std::size_t i) override {
        if (not child)
            throw invalid_argument("no child given");
        if (i >= children().size())
            throw out_of_range("index i out of bounds");
        auto old = children()[i];
        children()[i] = child;
        child->parent(this);
        return old;
    }

    const auto & aggregates() const { return aggregates_; }
    auto & aggregates() { return aggregates_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT SortingOperator : Producer, Consumer
{
    using order_type = QueryGraph::order_type;

    private:
    std::vector<order_type> order_by_; ///< the order to sort by

    public:
    SortingOperator(std::vector<order_type> order_by) : order_by_(std::move(order_by)) { }

    /** Creates and returns a copy of this single operator node, i.e. only copies this operator without adding any
     * inherited member fields like the parent or children nodes in the returned copy. */
    SortingOperator clone_node() const { return SortingOperator(order_by_); }

    const auto & order_by() const { return order_by_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

#define M_OPERATOR_LIST(X) \
    X(ScanOperator) \
    X(CallbackOperator) \
    X(PrintOperator) \
    X(NoOpOperator) \
    X(FilterOperator) \
    X(DisjunctiveFilterOperator) \
    X(JoinOperator) \
    X(SemiJoinReductionOperator) \
    X(DecomposeOperator) \
    X(ProjectionOperator) \
    X(LimitOperator) \
    X(GroupingOperator) \
    X(AggregationOperator) \
    X(SortingOperator)

M_DECLARE_VISITOR(OperatorVisitor, Operator, M_OPERATOR_LIST)
M_DECLARE_VISITOR(ConstOperatorVisitor, const Operator, M_OPERATOR_LIST)

namespace {

template<bool C>
struct ThePreOrderOperatorVisitor : std::conditional_t<C, ConstOperatorVisitor, OperatorVisitor>
{
    using super = std::conditional_t<C, ConstOperatorVisitor, OperatorVisitor>;
    template<typename T> using Const = typename super::template Const<T>;
    void operator()(Const<Operator> &op) override {
        try { op.accept(*this); } catch (visit_skip_subtree) { return; }
        if (auto c = cast<Const<Consumer>>(&op)) {
            for (auto child : c->children())
                (*this)(*child);
        }
    }
};

template<bool C>
struct ThePostOrderOperatorVisitor : std::conditional_t<C, ConstOperatorVisitor, OperatorVisitor>
{
    using super = std::conditional_t<C, ConstOperatorVisitor, OperatorVisitor>;
    template<typename T> using Const = typename super::template Const<T>;
    void operator()(Const<Operator> &op) override {
        if (auto c = cast<Const<Consumer>>(&op)) {
            for (auto child : c->children())
                (*this)(*child);
        }
        op.accept(*this);
    }
};

}
using PreOrderOperatorVisitor = ThePreOrderOperatorVisitor<false>;
using ConstPreOrderOperatorVisitor = ThePreOrderOperatorVisitor<true>;
using PostOrderOperatorVisitor = ThePostOrderOperatorVisitor<false>;
using ConstPostOrderOperatorVisitor = ThePostOrderOperatorVisitor<true>;

M_MAKE_STL_VISITABLE(PreOrderOperatorVisitor, Operator, M_OPERATOR_LIST)
M_MAKE_STL_VISITABLE(ConstPreOrderOperatorVisitor, const Operator, M_OPERATOR_LIST)
M_MAKE_STL_VISITABLE(PostOrderOperatorVisitor, Operator, M_OPERATOR_LIST)
M_MAKE_STL_VISITABLE(ConstPostOrderOperatorVisitor, const Operator, M_OPERATOR_LIST)


enum class OperatorKind
{
#define X(Kind) Kind,
    M_OPERATOR_LIST(X)
#undef X
};

}
