#pragma once

#include "IR/CNF.hpp"
#include <memory>
#include <vector>


namespace db {

struct DataSource;
struct Join;
struct Stmt;

/** A data source in the join graph.  A data source provides a sequence of tuples, optionally with a filter condition
 * they must fulfill.  Data sources can be joined with one another. */
struct DataSource
{
    private:
    cnf::CNF filter_; ///< filter condition on this data source
    std::vector<Join*> joins_; ///< joins with this data source
    const char *alias_; ///< the alias of this data source

    public:
    DataSource(const char *alias) : alias_(alias) { }
    virtual ~DataSource() { }

    const char * alias() const { return alias_; }
    cnf::CNF filter() const { return filter_; }
    void update_filter(cnf::CNF filter) { filter_ = filter_ and filter; }
    void add_join(Join *join) { joins_.emplace_back(join); }
};

/** A base table is a data source that is materialized and stored persistently. */
struct BaseTable : DataSource
{
    private:
    const Relation &relation_;

    public:
    BaseTable(const Relation &relation, const char *alias) : DataSource(alias), relation_(relation) { }

    const Relation & relation() const { return relation_; }
};

/** A (nested) query is a data source that must be computed. */
struct Query : DataSource
{
    Query(const char *alias) : DataSource(alias) { }
};

/** A join combines source relations by a join condition. */
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

/** The join graph represents all data sources and joins in a graph structure.  It is used as an intermediate
 * representation of a query. */
struct JoinGraph
{
    friend struct GraphBuilder;

    private:
    const Stmt *stmt_; ///< the original statement the join graph was constructed from
    std::vector<DataSource*> sources; ///< collection of all data sources in this join graph
    std::vector<Join*> joins; ///< collection of all joins in this join graph

    public:
    ~JoinGraph();

    static std::unique_ptr<JoinGraph> Build(const Stmt *stmt);

    const Stmt * get_stmt() const { return stmt_; }

    /** Translates the join graph to dot. */
    void dot(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;
};

}
