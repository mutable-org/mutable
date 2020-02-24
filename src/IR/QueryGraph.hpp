#pragma once

#include "IR/CNF.hpp"
#include "util/ADT.hpp"
#include <cstring>
#include <memory>
#include <vector>


namespace db {

struct DataSource;
struct Join;
struct QueryGraph;
struct Stmt;

/** A `DataSource` in a `db::QueryGraph`.  Represents something that can be evaluated to a sequence of tuples,
 * optionally filtered by a filter condition.  A `DataSource` can be joined with one or more other `DataSource`s by a
 * `db::Join`. */
struct DataSource
{
    private:
    cnf::CNF filter_; ///< filter condition on this data source
    std::vector<Join*> joins_; ///< joins with this data source
    const char *alias_; ///< the alias of this data source
    const std::size_t id_; ///< unique identifier of this data source within its query graph

    public:
    DataSource(std::size_t id, const char *alias) : alias_(alias), id_(id) {
        insist(not alias or strlen(alias) != 0, "if the data source has an alias, it must not be empty");
    }

    virtual ~DataSource() { }

    /** Returns the id of this `DataSource`. */
    std::size_t id() const { return id_; }
    /** Returns the alias of this `DataSource`.  May be `nullptr`. */
    const char * alias() const { return alias_; }
    /** Returns the filter of this `DataSource`.  May be empty. */
    cnf::CNF filter() const { return filter_; }
    /** Adds `filter` to the current filter of this `DataSource` by logical conjunction. */
    void update_filter(cnf::CNF filter) { filter_ = filter_ and filter; }
    /** Adds `join` to the set of `db::Join`s of this `DataSource`. */
    void add_join(Join *join) { joins_.emplace_back(join); }
    /** Returns a reference to the `db::Join`s using this `DataSource`. */
    const auto & joins() const { return joins_; }
};

/** A `BaseTable` is a `db::DataSource` that is materialized and stored persistently by the database system. */
struct BaseTable : DataSource
{
    private:
    const Table &table_; ///< the table providing the tuples

    public:
    BaseTable(std::size_t id, const char *alias, const Table &table) : DataSource(id, alias), table_(table) { }

    /** Returns a reference to the `db::Table` providing the tuples. */
    const Table & table() const { return table_; }
};

/** A `Query` in a `db::QueryGraph` is a `db::DataSource` that represents a nested query.  As such, a `Query` contains a
 * `db::QueryGraph`.  A `Query` must be evaluated to acquire its sequence of tuples. */
struct Query : DataSource
{
    private:
    QueryGraph *query_graph_; ///< query graph of the sub-query

    public:
    Query(std::size_t id, const char *alias, QueryGraph *query_graph) : DataSource(id, alias), query_graph_(query_graph) { }
    ~Query();

    /** Returns a reference to the internal `db::QueryGraph`. */
    QueryGraph * query_graph() const { return query_graph_; }
};

/** A `Join` in a `db::QueryGraph` combines `db::DataSource`s by a join condition. */
struct Join
{
    using sources_t = std::vector<DataSource*>;

    private:
    cnf::CNF condition_; ///< join condition
    sources_t sources_; ///< the sources to join

    public:
    Join(cnf::CNF condition, sources_t sources) : condition_(condition) , sources_(sources) { }

    /** Returns the join condition. */
    cnf::CNF condition() const { return condition_; }
    /** Returns a reference to the joined `db::DataSource`s. */
    const sources_t & sources() const { return sources_; }
};

/** The query graph represents all data sources and joins in a graph structure.  It is used as an intermediate
 * representation of a query. */
struct QueryGraph
{
    friend struct GraphBuilder;

    using Subproblem = SmallBitset; ///< encode `QueryGraph::Subproblem`s as `SmallBitset`s

    private:
    using projection_type = std::pair<const Expr*, const char*>;
    using order_type = std::pair<const Expr*, bool>; ///> true means ascending, false means descending

    std::vector<DataSource*> sources_; ///< collection of all data sources in this query graph
    std::vector<Join*> joins_; ///< collection of all joins in this query graph

    std::vector<const Expr*> group_by_; ///< the grouping keys
    std::vector<const Expr*> aggregates_; ///< the aggregates to compute
    std::vector<projection_type> projections_; ///< the data to compute
    std::vector<order_type> order_by_; ///< the order
    struct { uint64_t limit = 0, offset; } limit_; ///< limit: limit and offset
    bool projection_is_anti_ = false;

    public:
    ~QueryGraph();

    static std::unique_ptr<QueryGraph> Build(const Stmt &stmt);

    const auto & sources() const { return sources_; }
    const auto & joins() const { return joins_; }
    const auto & group_by() const { return group_by_; }
    const auto & aggregates() const { return aggregates_; }
    const auto & projections() const { return projections_; }
    const auto & order_by() const { return order_by_; }
    auto limit() const { return limit_; }
    bool projection_is_anti() const { return projection_is_anti_; }

    /** Returns a data souce given its id. */
    const DataSource * operator[](uint64_t id) const {
        auto ds = sources_[id];
        insist(ds->id() == id, "given id and data source id must match");
        return ds;
    }

    /** Translates the query graph to dot. */
    void dot(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    void dot_recursive(std::ostream &out) const;
};


/** An adjacency matrix for a given query graph. Represents the join graph. */
struct AdjacencyMatrix
{
    private:
    SmallBitset m_[SmallBitset::CAPACITY]; ///< matrix entries

    public:
    AdjacencyMatrix() { }
    AdjacencyMatrix(const QueryGraph &query_graph)
    {
        /* Iterate over all joins in the query graph. */
        for (auto join : query_graph.joins()) {
            if (join->sources().size() != 2)
                throw std::invalid_argument("building adjacency matrix for non-binary join");
            /* Take both join inputs and set the appropriate bit in the adjacency matrix. */
            auto i = join->sources()[0]->id(); // first join input
            auto j = join->sources()[1]->id(); // second join input
            set_bidirectional(i, j); // symmetric matrix
        }

    }

    /** Set the bit in row `i` and offset `j` to to one. */
    void set(std::size_t i, std::size_t j) {
        insist(i < SmallBitset::CAPACITY, "offset is out-of-bounds");
        insist(j < SmallBitset::CAPACITY, "offset is out-of-bounds");
        m_[i].set(j);
    }
    /** Set the bit in row `i` and offset `j` and the symmetric bit to one. */
    void set_bidirectional(std::size_t i, std::size_t j) { set(i, j); set(j, i); }

    /** Get the bit in row `i` and offset `j` to to one. */
    bool get(std::size_t i, std::size_t j) const {
        insist(i < SmallBitset::CAPACITY, "offset is out-of-bounds");
        insist(j < SmallBitset::CAPACITY, "offset is out-of-bounds");
        return m_[i].contains(j);
    }

    /** Computes the set of nodes reachable from `src`, i.e.\ the set of nodes reachable from any node in `src`. */
    SmallBitset reachable(SmallBitset src) const {
        SmallBitset R_old(0);
        SmallBitset R_new(src);
        while (auto R = R_new - R_old) {
            R_old = R_new;
            for (auto x : R)
                R_new |= m_[x]; // add all nodes reachable from node `x` to the set of reachable nodes
        }
        return R_new;
    }

    /** Computes the set of nodes in `S` reachable from `src`, i.e.\ the set of nodes in `S` reachable from any node in
     * `src`. */
    SmallBitset reachable(SmallBitset src, SmallBitset S) const {
        SmallBitset R_old(0);
        SmallBitset R_new(src & S);
        while (auto R = R_new - R_old) {
            R_old = R_new;
            for (auto x : R)
                R_new |= m_[x] & S; // add all nodes in `S` reachable from node `x` to the set of reachable nodes
        }
        return R_new;
    }

    /** Computes the neighbors of `S`, i.e.\ all nodes that are connected to `S` via a single edge.  Nodes from `S` are
     * not part of the neighborhood. */
    SmallBitset neighbors(SmallBitset S) const {
        SmallBitset neighbors;
        for (auto it : S)
            neighbors |= m_[it];
        return neighbors - S;
    }

    /** Returns `true` iff the subproblem `S` is connected.  `S` is connected iff any node in `S` can reach all other
     * nodes of `S` using only nodes in `S`.  */
    bool is_connected(SmallBitset S) const { return reachable(SmallBitset(1UL << *S.begin()), S) == S; }

    /** Returns `true` iff there is at least one edge (join) between `left` and `right`. */
    bool is_connected(SmallBitset left, SmallBitset right) const {
        SmallBitset neighbors;
        /* Compute the neighbors of `right`. */
        for (auto it : right)
            neighbors = neighbors | m_[it];
        /* Intersect `left` with the neighbors of `right`.  If the result is non-empty, `left` and `right` are
         * immediately connected by a join. */
        return left & neighbors;
    }

    friend std::ostream & operator<<(std::ostream &out, const AdjacencyMatrix &m) {
        out << "Adjacency Matrix";
        for (auto s : m.m_)
            out << '\n' << s;
        return out;
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

}
