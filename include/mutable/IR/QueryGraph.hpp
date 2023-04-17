#pragma once

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
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
    const char *alias_; ///< the alias of this data source, `nullptr` if this data source has no alias
    std::size_t id_; ///< unique identifier of this data source within its query graph

    bool decorrelated_ = true; ///< indicates whether this source is already decorrelated

    protected:
    DataSource(std::size_t id, const char *alias) : alias_(alias), id_(id) {
        if (alias and strlen(alias) == 0)
            throw invalid_argument("if the data source has an alias, it must not be empty");
    }

    public:
    virtual ~DataSource() { }

    /** Returns the id of this `DataSource`. */
    std::size_t id() const { return id_; }
    /** Returns the alias of this `DataSource`.  May be `nullptr`. */
    const char * alias() const { return alias_; }
    /** Returns the name of this `DataSource`.  Either the same as `alias()`, if an alias is given, otherwise the name
     * of the referenced `Table`.  May return `nullptr` for anonymous nested queries (e.g. in a WHERE clause).  */
    virtual const char * name() const = 0;
    /** Returns the filter of this `DataSource`.  May be empty. */
    const cnf::CNF & filter() const { return filter_; }
    /** Adds `filter` to the current filter of this `DataSource` by logical conjunction. */
    void update_filter(cnf::CNF filter) { filter_ = filter_ and filter; }
    /** Adds `join` to the set of `Join`s of this `DataSource`. */
    void add_join(Join &join) { joins_.emplace_back(join); }
    /** Returns a reference to the `Join`s using this `DataSource`. */
    const auto & joins() const { return joins_; }

    /** Returns `true` iff the data source is correlated. */
    virtual bool is_correlated() const = 0;

    private:
    void remove_join(Join &join) {
        auto it = std::find_if(joins_.begin(), joins_.end(), [&join](auto j) { return &j.get() == &join; });
        if (it == joins_.end())
            throw invalid_argument("given join not found");
        joins_.erase(it);
    }

    public:
    bool operator==(const DataSource &other) const { return this->id_ == other.id_; }
    bool operator!=(const DataSource &other) const { return not operator==(other); }
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
    BaseTable(std::size_t id, const char *alias, const Table &table) : DataSource(id, alias), table_(table) { }

    public:
    /** Returns a reference to the `Table` providing the tuples. */
    const Table & table() const { return table_; }

    const char * name() const override { return alias() ? alias() : table_.name; }

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
    Query(std::size_t id, const char *alias, std::unique_ptr<QueryGraph> query_graph)
        : DataSource(id, alias), query_graph_(std::move(query_graph))
    { }

    public:
    /** Returns a reference to the internal `QueryGraph`. */
    QueryGraph & query_graph() const { return *query_graph_; }

    const char * name() const override { return alias(); }

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
    const cnf::CNF & condition() const { return condition_; }
    /** Adds `condition` to the current condition of this `Join` by logical conjunction. */
    void update_condition(cnf::CNF update) { condition_ = condition_ and update; }
    /** Returns a reference to the joined `DataSource`s. */
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

/** The query graph represents all data sources and joins in a graph structure.  It is used as an intermediate
 * representation of a query. */
struct M_EXPORT QueryGraph
{
    friend struct GraphBuilder;
    friend struct Decorrelation;
    friend struct GetPrimaryKey;

    using Subproblem = SmallBitset; ///< encode `QueryGraph::Subproblem`s as `SmallBitset`s
    using projection_type = std::pair<std::reference_wrapper<const ast::Expr>, const char*>;
    using order_type = std::pair<std::reference_wrapper<const ast::Expr>, bool>; ///< true means ascending, false means descending
    using group_type = std::pair<std::reference_wrapper<const ast::Expr>, const char*>;

    private:
    std::vector<std::unique_ptr<DataSource>> sources_; ///< collection of all data sources in this query graph
    std::vector<std::unique_ptr<Join>> joins_; ///< collection of all joins in this query graph

    std::vector<group_type> group_by_; ///< the grouping keys
    std::vector<std::reference_wrapper<const ast::FnApplicationExpr>> aggregates_; ///< the aggregates to compute
    std::vector<projection_type> projections_; ///< the data to compute
    std::vector<order_type> order_by_; ///< the order
    struct { uint64_t limit = 0, offset = 0; } limit_; ///< limit: limit and offset

    mutable std::unique_ptr<AdjacencyMatrix> adjacency_matrix_;

    public:
    friend void swap(QueryGraph &first, QueryGraph &second) {
        using std::swap;
        swap(first.sources_,        second.sources_);
        swap(first.joins_,          second.joins_);
        swap(first.group_by_,       second.group_by_);
        swap(first.projections_,    second.projections_);
        swap(first.order_by_,       second.order_by_);
        swap(first.limit_,          second.limit_);
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

    void add_source(std::unique_ptr<DataSource> source) {
        source->id_ = sources_.size();
        sources_.emplace_back(source.release());
    }
    BaseTable & add_source(const char *alias, const Table &table) {
        std::unique_ptr<BaseTable> base(new BaseTable(sources_.size(), alias, table));
        auto &ref = sources_.emplace_back(std::move(base));
        return as<BaseTable>(*ref);
    }
    Query & add_source(const char *alias, std::unique_ptr<QueryGraph> query_graph) {
        std::unique_ptr<Query> Q(new Query(sources_.size(), alias, std::move(query_graph)));
        auto &ref = sources_.emplace_back(std::move(Q));
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
        return ds;
    }

    const auto & sources() const { return sources_; }
    const auto & joins() const { return joins_; }
    const auto & group_by() const { return group_by_; }
    const auto & aggregates() const { return aggregates_; }
    const std::vector<projection_type> & projections() const { return projections_; }
    const auto & order_by() const { return order_by_; }
    auto limit() const { return limit_; }

    /** Returns a data souce given its id. */
    const DataSource & operator[](uint64_t id) const {
        auto &ds = sources_[id];
        M_insist(ds->id() == id, "given id and data source id must match");
        return *ds;
    }

    /** Returns `true` iff the graph contains a grouping. */
    bool grouping() const { return not group_by_.empty() or not aggregates_.empty(); }
    /** Returns `true` iff the graph is correlated, i.e. it contains a correlated source. */
    bool is_correlated() const;

    AdjacencyMatrix & adjacency_matrix() {
        if (not adjacency_matrix_) [[unlikely]]
            compute_adjacency_matrix();
        return *adjacency_matrix_;
    }

    const AdjacencyMatrix & adjacency_matrix() const {
        if (not adjacency_matrix_) [[unlikely]]
            compute_adjacency_matrix();
        return *adjacency_matrix_;
    }

    /** Translates the query graph to dot. */
    void dot(std::ostream &out) const;

    /** Translates the query graph to SQL. */
    void sql(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    void compute_adjacency_matrix() const;
    void dot_recursive(std::ostream &out) const;

    void remove_join(Join &join) {
        auto it = std::find_if(joins_.begin(), joins_.end(), [&join](auto &j) { return j.get() == &join; });
        if (it == joins_.end())
            throw invalid_argument("given join not found");
        joins_.erase(it);
    }
};

}
