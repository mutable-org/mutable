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

/** A data source in the query graph.  A data source provides a sequence of tuples, optionally with a filter condition
 * they must fulfill.  Data sources can be joined with one another. */
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

    std::size_t id() const { return id_; }
    const char * alias() const { return alias_; }
    cnf::CNF filter() const { return filter_; }
    void update_filter(cnf::CNF filter) { filter_ = filter_ and filter; }
    void add_join(Join *join) { joins_.emplace_back(join); }
    const auto & joins() const { return joins_; }
};

/** A base table is a data source that is materialized and stored persistently. */
struct BaseTable : DataSource
{
    private:
    const Table &table_; ///< the table providing the tuples

    public:
    BaseTable(std::size_t id, const char *alias, const Table &table) : DataSource(id, alias), table_(table) { }

    const Table & table() const { return table_; }
};

/** A (nested) query is a data source that must be computed. */
struct Query : DataSource
{
    private:
    QueryGraph *query_graph_; ///< query graph of the sub-query

    public:
    Query(std::size_t id, const char *alias, QueryGraph *query_graph) : DataSource(id, alias), query_graph_(query_graph) { }
    ~Query();

    QueryGraph * query_graph() const { return query_graph_; }
};

/** A join combines source tables by a join condition. */
struct Join
{
    using sources_t = std::vector<DataSource*>;

    private:
    cnf::CNF condition_; ///< join condition
    sources_t sources_; ///< the sources to join

    public:
    Join(cnf::CNF condition, sources_t sources) : condition_(condition) , sources_(sources) { }

    cnf::CNF condition() const { return condition_; }
    const sources_t & sources() const { return sources_; }
};

/** The query graph represents all data sources and joins in a graph structure.  It is used as an intermediate
 * representation of a query. */
struct QueryGraph
{
    friend struct GraphBuilder;

    using Subproblem = SmallBitset; ///< export a type declaration for subproblems encoded as bitsets

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

    /** Returns true if there is at least one edge between one of the data sources in each subset. */
    bool is_connected(SmallBitset left, SmallBitset right) {
        SmallBitset reachable;
        /* Compute the set of reachable data sources from the data sources in `right`. */
        for (auto it : right)
            reachable = reachable | m_[it];
        /* Intersect the data source of `left` with the data source reachable from `right`.  If the result is non-empty,
         * `left` and `right` are connected. */
        return left & reachable;
    }

    /** Returns true if the subproblem `s` is connected. */
    bool is_connected(SmallBitset s) {
        SmallBitset reachable;
        /* Subproblem with single relation is trivially connected. */
        if (s.size() == 1) return true;
        /* Compute the set of reachable data sources from the data sources in `s`. */
        for (auto it : s)
            reachable = reachable | m_[it];
        /* If `reachable` is a subset of `s`, then it is connected. */
        return s.is_subset(reachable);
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
