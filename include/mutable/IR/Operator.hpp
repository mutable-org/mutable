#pragma once

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
    private:
    Schema schema_; ///< the schema of this `Operator`
    std::unique_ptr<OperatorInformation> info_; ///< additional information about this `Operator`
    mutable OperatorData *data_ = nullptr; ///< the data object associated to this `Operator`; may be `nullptr`
    protected:
    mutable std::size_t id_ = -1UL; ///< the ID of this `Operator`; used as index in the DP table of `PhysicalOperator`

    public:
    virtual ~Operator() { delete data_; }

    /** Returns the `Schema` of this `Operator`. */
    Schema & schema() { return schema_; }
    /** Returns the `Schema` of this `Operator`. */
    const Schema & schema() const { return schema_; }

    bool has_info() const { return bool(info_); }
    const OperatorInformation & info() const { M_insist(bool(info_)); return *info_; }
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
    virtual ~Producer() { }

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
    CallbackOperator(callback_type callback) : callback_(callback) { }

    const auto & callback() const { return callback_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

/** Prints the produced `Tuple`s to a `std::ostream` instance. */
struct M_EXPORT PrintOperator : Consumer
{
    std::ostream &out;

    PrintOperator(std::ostream &out) : out(out) { }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

/** Drops the produced results and outputs only the number of result tuples produced.  This is used for benchmarking. */
struct M_EXPORT NoOpOperator : Consumer
{
    std::ostream &out;

    NoOpOperator(std::ostream &out) : out(out) { }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT ScanOperator : Producer
{
    private:
    const Store &store_;
    const char *alias_;

    public:
    ScanOperator(const Store &store, const char *alias)
        : store_(store)
        , alias_(M_notnull(alias))
    {
        auto &S = schema();
        for (auto &e : store.table().schema())
            S.add({alias, e.id.name}, e.type, e.constraints);
    }

    const Store & store() const { return store_; }
    const char * alias() const { return alias_; }

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT FilterOperator : Producer, Consumer
{
    private:
    cnf::CNF filter_;

    public:
    FilterOperator(cnf::CNF filter) : filter_(std::move(filter)) { }

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

    void accept(OperatorVisitor &v) override;
    void accept(ConstOperatorVisitor &v) const override;
};

struct M_EXPORT JoinOperator : Producer, Consumer
{
    private:
    cnf::CNF predicate_;

    public:
    JoinOperator(cnf::CNF predicate) : predicate_(std::move(predicate)) { }

    const cnf::CNF & predicate() const { return predicate_; }

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
                Schema::Identifier id(D->table_name.text, D->attr_name.text);
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
                Schema::Identifier id(D->table_name.text, D->attr_name.text);
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
                Schema::Identifier id(D->table_name.text, D->attr_name.text);
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
                Schema::Identifier id(D->table_name.text, D->attr_name.text);
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
    void operator()(Const<Operator> &op) {
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
    void operator()(Const<Operator> &op) {
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
