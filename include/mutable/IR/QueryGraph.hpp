#pragma once

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutable/catalog/Scheduler.hpp>
#include <mutable/IR/CNF.hpp>
#include <mutable/mutable-config.hpp>
#include <mutable/util/AdjacencyMatrix.hpp>
#include <mutable/util/ADT.hpp>
#include <sstream>
#include <string>
#include <vector>


namespace m {

namespace ast {

struct Stmt;

}

struct DataSource;
struct Join;
struct QueryGraph;

/** A `DataSource` in a `QueryGraph`.  Represents something that can be evaluated to a sequence of tuples, optionally
 * filtered by a filter condition.  A `DataSource` can be joined with one or more other `DataSource`s by a `Join`. */
struct M_EXPORT DataSource
{
    friend struct QueryGraph;
    friend struct GraphBuilder;
    friend struct Decorrelation;

    private:
    cnf::CNF filter_; ///< filter condition on this data source
    std::vector<std::reference_wrapper<Join>> joins_; ///< joins with this data source
    ThreadSafePooledOptionalString alias_; ///< alias of this data source, may not have a value if this data source has no alias
    std::size_t id_; ///< unique identifier of this data source within its query graph

    bool decorrelated_ = true; ///< indicates whether this source is already decorrelated

    protected:
    DataSource(std::size_t id, ThreadSafePooledOptionalString alias) : alias_(std::move(alias)), id_(id) {
        if (alias_.has_value() and strlen(*alias_) == 0)
            throw invalid_argument("if the data source has an alias, it must not be empty");
    }

    public:
    virtual ~DataSource() { }

    /** Returns the id of this `DataSource`. */
    std::size_t id() const { return id_; }
    /** Returns the alias of this `DataSource`.  May not have a value. */
    const ThreadSafePooledOptionalString & alias() const { return alias_; }
    /** Returns the name of this `DataSource`.  Either the same as `alias()`, if an alias is given, otherwise the name
     * of the referenced `Table`. Returned value might be empty for anonymous nested queries (e.g. in a WHERE clause). */
    virtual ThreadSafePooledOptionalString name() const = 0;
    /** Returns the filter of this `DataSource`.  May be empty. */
    cnf::CNF & filter() { return filter_; }
    const cnf::CNF & filter() const { return filter_; }
    /** Adds `filter` to the current filter of this `DataSource` by logical conjunction. */
    void update_filter(cnf::CNF filter) { filter_ = filter_ and filter; }
    /** Adds `join` to the set of `Join`s of this `DataSource`. */
    void add_join(Join &join) { joins_.emplace_back(join); }
    /** Returns a reference to the `Join`s using this `DataSource`. */
    auto & joins() { return joins_; }
    const auto & joins() const { return joins_; }

    /** Returns the estimated size of a tuple only considering the given projections **/
    virtual std::size_t projection_relations(std::unordered_map<ThreadSafePooledOptionalString, std::unordered_set<ThreadSafePooledOptionalString>>& projections) const = 0;

    /** Returns `true` iff the data source is correlated. */
    virtual bool is_correlated() const = 0;

    void remove_join(Join &join) {
        auto it = std::find_if(joins_.begin(), joins_.end(), [&join](auto j) { return &j.get() == &join; });
        if (it == joins_.end())
            throw invalid_argument("given join not found");
        joins_.erase(it);
    }

    bool operator==(const DataSource &other) const { return this->id_ == other.id_; }
    bool operator!=(const DataSource &other) const { return not operator==(other); }
};

struct DataSourceHash
{
    ///> Mark this callable as *transparent*, allowing for computing the hash of various types that are interoperable.
    ///> See https://en.cppreference.com/w/cpp/container/unordered_map/find.
    using is_transparent = void;

    template<typename U>
    requires requires(U &&u) { static_cast<const DataSource&>(std::forward<U>(u)); }
    std::size_t operator()(U &&u) const { return murmur3_64(static_cast<const DataSource&>(std::forward<U>(u)).id()); }
};

struct DataSourceEqualTo
{
    ///> Mark this callable as *transparent*, allowing for comparing various types that are interoperable. > See
    ///https://en.cppreference.com/w/cpp/container/unordered_map/find.
    using is_transparent = void;

    template<typename U, typename V>
    requires requires(U &&u) { static_cast<const DataSource&>(std::forward<U>(u)); } and
             requires(V &&v) { static_cast<const DataSource&>(std::forward<V>(v)); }
    auto operator()(U &&u, V &&v) const {
        return static_cast<const DataSource&>(std::forward<U>(u)) == static_cast<const DataSource&>(std::forward<V>(v));
    }
};


/** A `BaseTable` is a `DataSource` that is materialized and stored persistently by the database system. */
struct M_EXPORT BaseTable : DataSource
{
    friend struct QueryGraph;
    friend struct GetPrimaryKey;

    private:
    const Table &table_; ///< the table providing the tuples
    ///> list of designators expanded from `GetPrimaryKey::compute()` or `GetAttributes::compute()`
    // std::vector<const ast::Designator*> expansion_;

    private:
    BaseTable(std::size_t id, ThreadSafePooledOptionalString alias, const Table &table)
        : DataSource(id, std::move(alias)), table_(table)
    { }

    public:
    /** Returns a reference to the `Table` providing the tuples. */
    const Table & table() const { return table_; }

    std::size_t projection_relations(std::unordered_map<ThreadSafePooledOptionalString, std::unordered_set<ThreadSafePooledOptionalString>>& projections) const override {
        if (auto it = projections.find(name()); it != projections.end()) {
            return 1UL;
        }
        return 0;
    }

    ThreadSafePooledOptionalString name() const override { return alias().has_value() ? alias() : table_.name(); }

    /** `BaseTable` is never correlated.  Always returns `false`. */
    bool is_correlated() const override { return false; };
};

/** A `Query` in a `QueryGraph` is a `DataSource` that represents a nested query.  As such, a `Query` contains a
 * `QueryGraph`.  A `Query` must be evaluated to acquire its sequence of tuples. */
struct M_EXPORT Query : DataSource
{
    friend struct QueryGraph;

    private:
    std::unique_ptr<QueryGraph> query_graph_; ///< query graph of the sub-query

    private:
    Query(std::size_t id, ThreadSafePooledOptionalString alias, std::unique_ptr<QueryGraph> query_graph)
        : DataSource(id, std::move(alias)), query_graph_(std::move(query_graph))
    { }

    public:
    /** Returns a reference to the internal `QueryGraph`. */
    QueryGraph & query_graph() { M_insist(bool(query_graph_)); return *query_graph_; }
    const QueryGraph & query_graph() const { return const_cast<Query*>(this)->query_graph(); }

    std::unique_ptr<QueryGraph> extract_query_graph() { return std::exchange(query_graph_, nullptr); }

    ThreadSafePooledOptionalString name() const override { return alias(); }

    std::size_t projection_relations(std::unordered_map<ThreadSafePooledOptionalString, std::unordered_set<ThreadSafePooledOptionalString>>& projections) const override;

    bool is_correlated() const override;
};

/** A `Join` in a `QueryGraph` combines `DataSource`s by a join condition. */
struct M_EXPORT Join
{
    using sources_t = std::vector<std::reference_wrapper<DataSource>>;

    private:
    cnf::CNF condition_; ///< join condition
    sources_t sources_; ///< the sources to join

    public:
    Join(cnf::CNF condition, sources_t sources) : condition_(std::move(condition)) , sources_(std::move(sources)) { }

    /** Returns the join condition. */
    cnf::CNF & condition() { return condition_; }
    const cnf::CNF & condition() const { return condition_; }
    /** Adds `condition` to the current condition of this `Join` by logical conjunction. */
    void update_condition(cnf::CNF update) { condition_ = condition_ and update; }
    /** Returns a reference to the joined `DataSource`s. */
    sources_t & sources() { return sources_; }
    const sources_t & sources() const { return sources_; }

    bool operator==(const Join &other) const {
        if (this->condition_ != other.condition_) return false;
        if (this->sources().size() != other.sources().size()) return false;
        for (auto &this_src : sources_) {
            auto it = std::find_if(other.sources().begin(), other.sources().end(), [this_src](auto other_src) {
                return &this_src.get() == &other_src.get();
            });
            if (it == other.sources().end()) return false;
        }
        return true;
    }
    bool operator!=(const Join &other) const { return not operator==(other); }
};

struct JoinHash
{
    ///> Mark this callable as *transparent*, allowing for computing the hash of various types that are interoperable.
    ///> See https://en.cppreference.com/w/cpp/container/unordered_map/find.
    using is_transparent = void;

    template<typename U>
    requires requires(U &&u) { static_cast<const Join&>(std::forward<U>(u)); }
    std::size_t operator()(U &&u) const {
        auto &j = static_cast<const Join&>(std::forward<U>(u));
        M_insist(j.sources().size() == 2);
        return murmur3_64(j.sources()[0].get().id() xor j.sources()[1].get().id());
    }
};

struct JoinEqualTo
{
    ///> Mark this callable as *transparent*, allowing for comparing various types that are interoperable.
    ///> See https://en.cppreference.com/w/cpp/container/unordered_map/find.
    using is_transparent = void;

    template<typename U, typename V>
    requires requires(U &&u) { static_cast<const Join&>(std::forward<U>(u)); } and
             requires(V &&v) { static_cast<const Join&>(std::forward<V>(v)); }
    auto operator()(U &&u, V &&v) const {
        return static_cast<const Join&>(std::forward<U>(u)) == static_cast<const Join&>(std::forward<V>(v));
    }
};

/** The query graph represents all data sources and joins in a graph structure.  It is used as an intermediate
 * representation of a query. */
struct M_EXPORT QueryGraph
{
    friend struct GraphBuilder;
    friend struct Decorrelation;
    friend struct GetPrimaryKey;

    using Subproblem = SmallBitset; ///< encode `QueryGraph::Subproblem`s as `SmallBitset`s
    using projection_type = std::pair<std::reference_wrapper<const ast::Expr>, ThreadSafePooledOptionalString>;
    using order_type = std::pair<std::reference_wrapper<const ast::Expr>, bool>; ///< true means ascending, false means descending
    using group_type = std::pair<std::reference_wrapper<const ast::Expr>, ThreadSafePooledOptionalString>;

    private:
    std::vector<std::unique_ptr<DataSource>> sources_; ///< collection of all data sources in this query graph
    std::vector<std::unique_ptr<Join>> joins_; ///< collection of all joins in this query graph

    std::vector<group_type> group_by_; ///< the grouping keys
    std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates_; ///< the aggregates to compute
    std::vector<projection_type> projections_; ///< the data to compute
    std::vector<order_type> order_by_; ///< the order
    struct { uint64_t limit = 0, offset = 0; } limit_; ///< limit: limit and offset

    mutable std::unique_ptr<AdjacencyMatrix> adjacency_matrix_;

    Scheduler::Transaction *t_ = nullptr; ///< the transaction this query graph belongs to
    ///> Stores the expressions of custom filters that have been added to the `DataSource`s after semantic analysis.
    std::vector<std::unique_ptr<ast::Expr>> custom_filter_exprs_;

    public:
    friend void swap(QueryGraph &first, QueryGraph &second) {
        using std::swap;
        swap(first.sources_,                second.sources_);
        swap(first.joins_,                  second.joins_);
        swap(first.group_by_,               second.group_by_);
        swap(first.aggregates_,             second.aggregates_);
        swap(first.projections_,            second.projections_);
        swap(first.order_by_,               second.order_by_);
        swap(first.limit_,                  second.limit_);
        swap(first.adjacency_matrix_,       second.adjacency_matrix_);
        swap(first.t_,                      second.t_);
        swap(first.custom_filter_exprs_,    second.custom_filter_exprs_);
    }

    QueryGraph();
    ~QueryGraph();

    QueryGraph(const QueryGraph&) = delete;
    QueryGraph(QueryGraph &&other) : QueryGraph() { swap(*this, other); }

    QueryGraph & operator=(QueryGraph &&other) { swap(*this, other); return *this; }

    static std::unique_ptr<QueryGraph> Build(const ast::Stmt &stmt);

    /** Returns the number of `DataSource`s in this graph. */
    std::size_t num_sources() const { return sources_.size(); }

    /** Returns the number of `Join`s in this graph. */
    std::size_t num_joins() const { return joins_.size(); }

    /** Returns true iff the `QueryGraph` is cyclic. */
    bool is_cyclic() const { return adjacency_matrix().is_cyclic(); }

    void add_source(std::unique_ptr<DataSource> source) {
        source->id_ = sources_.size();
        sources_.emplace_back(source.release());
    }
    BaseTable & add_source(ThreadSafePooledOptionalString alias, const Table &table) {
        std::unique_ptr<BaseTable> base(new BaseTable(sources_.size(), std::move(alias), table));
        auto &ref = sources_.emplace_back(std::move(base));
        adjacency_matrix_.reset(); // reset adjacency matrix after modifying the `QueryGraph`
        return as<BaseTable>(*ref);
    }
    Query & add_source(ThreadSafePooledOptionalString alias, std::unique_ptr<QueryGraph> query_graph) {
        std::unique_ptr<Query> Q(new Query(sources_.size(), std::move(alias), std::move(query_graph)));
        auto &ref = sources_.emplace_back(std::move(Q));
        adjacency_matrix_.reset(); // reset adjacency matrix after modifying the `QueryGraph`
        return as<Query>(*ref);
    }

    std::unique_ptr<DataSource> remove_source(std::size_t id) {
        auto it = std::next(sources_.begin(), id);
        auto ds = std::move(*it);
        M_insist(ds->id() == id, "IDs of sources must be sequential");
        sources_.erase(it);
        /* Decrement IDs of all sources after the deleted one to provide sequential IDs. */
        while (it != sources_.end()) {
            --(*it)->id_;
            ++it;
        }
        adjacency_matrix_.reset(); // reset adjacency matrix after modifying the `QueryGraph`
        return ds;
    }

    auto & sources() { return sources_; }
    const auto & sources() const { return sources_; }
    auto & joins() { return joins_; }
    const auto & joins() const { return joins_; }
    auto & group_by() { return group_by_; }
    const auto & group_by() const { return group_by_; }
    auto & aggregates() { return aggregates_; }
    const auto & aggregates() const { return aggregates_; }
    std::vector<projection_type> & projections() { return projections_; }
    const std::vector<projection_type> & projections() const { return projections_; }
    auto & order_by() { return order_by_; }
    const auto & order_by() const { return order_by_; }
    auto limit() const { return limit_; }

    /** Returns a data souce given its id. */
    DataSource & operator[](uint64_t id) {
        auto &ds = sources_[id];
        M_insist(ds->id() == id, "given id and data source id must match");
        return *ds;
    }
    const DataSource & operator[](uint64_t id) const { return const_cast<QueryGraph*>(this)->operator[](id); }

    /** Returns `true` iff the graph contains a grouping. */
    bool grouping() const { return not group_by_.empty() or not aggregates_.empty(); }
    /** Returns the size of a tuple resulting from the join of the subproblem. **/
    void get_projection_sizes_of_subproblems(std::vector<std::size_t>& projection_sizes) const;
    /** Returns `true` iff the graph is correlated, i.e. it contains a correlated source. */
    bool is_correlated() const;

    AdjacencyMatrix & adjacency_matrix() {
        if (not adjacency_matrix_) [[unlikely]]
            compute_adjacency_matrix();
        return *adjacency_matrix_;
    }
    const AdjacencyMatrix & adjacency_matrix() const { return const_cast<QueryGraph*>(this)->adjacency_matrix(); }

    /** Translates the query graph to dot. */
    void dot(std::ostream &out) const;

    /** Translates the query graph to SQL. */
    void sql(std::ostream &out) const;

    /** Set the transaction ID. */
    void transaction(Scheduler::Transaction *t) {
        M_insist(not t_ or *t_ == *t, "QueryGraph is already linked to another transaction");
        t_ = t;

        for (auto &ds : sources_)
            if (auto bt = cast<const Query>(ds.get()))
                bt->query_graph_->transaction(t_);
    }
    /** Returns the transaction ID. */
    Scheduler::Transaction * transaction() const { return t_; }

    /** Creates a `cnf::CNF` from `filter_expr` and adds it to the current filter of the given `DataSource` `ds` by logical conjunction. */
    void add_custom_filter(std::unique_ptr<ast::Expr> filter_expr, DataSource &ds) {
        M_insist(std::find_if(sources_.begin(), sources_.end(), [&](std::unique_ptr<DataSource> &source){
            return *source == ds;
        }) != sources_.end());

        auto filter = cnf::to_CNF(*filter_expr);
        ds.update_filter(filter);
        custom_filter_exprs_.push_back(std::move(filter_expr));
    }

    Join & emplace_join(cnf::CNF condition, Join::sources_t sources) {
        return *joins_.emplace_back(std::make_unique<Join>(std::move(condition), std::move(sources)));
    }

    std::unique_ptr<Join> extract_join(const Join &join) {
        auto it = std::find_if(joins_.begin(), joins_.end(), [&join](auto &j) { return j.get() == &join; });
        if (it == joins_.end())
            throw invalid_argument("given join not found");
        auto tmp = std::move(*it);
        joins_.erase(it);
        return tmp;
    }

    void remove_join(const Join &join) { extract_join(join); }

    void get_base_table_identifiers(std::vector<ThreadSafePooledString> &identifiers) {
        for (std::size_t i = 0; i < num_sources(); i++) {
            auto &ds = sources_[i];
            if (auto bt = cast<BaseTable>(ds.get())) identifiers.emplace_back(bt->name());
            else {
                auto &Q = as<Query>(*ds);
                Q.query_graph().get_base_table_identifiers(identifiers);
            }
        }
    }

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    void compute_adjacency_matrix() const;
    void dot_recursive(std::ostream &out) const;
};

}
