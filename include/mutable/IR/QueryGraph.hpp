#pragma once

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <mutable/IR/CNF.hpp>
#include <mutable/util/ADT.hpp>
#include <sstream>
#include <string>
#include <vector>


namespace m {

struct DataSource;
struct Join;
struct QueryGraph;
struct Stmt;
struct GetCorrelationInfo;

/** A `DataSource` in a `QueryGraph`.  Represents something that can be evaluated to a sequence of tuples, optionally
 * filtered by a filter condition.  A `DataSource` can be joined with one or more other `DataSource`s by a `Join`. */
struct DataSource
{
    friend struct QueryGraph;
    friend struct GraphBuilder;
    friend struct Decorrelation;

    private:
    cnf::CNF filter_; ///< filter condition on this data source
    std::vector<Join*> joins_; ///< joins with this data source
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
    cnf::CNF filter() const { return filter_; }
    /** Adds `filter` to the current filter of this `DataSource` by logical conjunction. */
    void update_filter(cnf::CNF filter) { filter_ = filter_ and filter; }
    /** Adds `join` to the set of `Join`s of this `DataSource`. */
    void add_join(Join *join) { joins_.emplace_back(join); }
    /** Returns a reference to the `Join`s using this `DataSource`. */
    const auto & joins() const { return joins_; }

    /** Returns `true` iff the data source is correlated. */
    virtual bool is_correlated() const = 0;

    private:
    void remove_join(Join *join) {
        auto it = std::find(joins_.begin(), joins_.end(), join);
        if (it == joins_.end())
            throw invalid_argument("given join not found");
        joins_.erase(it);
    }

    public:
    bool operator==(const DataSource &other) const { return this->id_ == other.id_; }
    bool operator!=(const DataSource &other) const { return not operator==(other); }
};

/** A `BaseTable` is a `DataSource` that is materialized and stored persistently by the database system. */
struct BaseTable : DataSource
{
    friend struct QueryGraph;
    friend struct GetPrimaryKey;

    private:
    const Table &table_; ///< the table providing the tuples
    ///> list of designators expanded from `GetPrimaryKey::compute()` or `GetAttributes::compute()`
    std::vector<const Designator*> expansion_;

    private:
    BaseTable(std::size_t id, const char *alias, const Table &table) : DataSource(id, alias), table_(table) { }
    public:
    ~BaseTable();

    /** Returns a reference to the `Table` providing the tuples. */
    const Table & table() const { return table_; }

    const char * name() const override { return alias() ? alias() : table_.name; }

    bool is_correlated() const override { return false; };
};

/** A `Query` in a `QueryGraph` is a `DataSource` that represents a nested query.  As such, a `Query` contains a
 * `QueryGraph`.  A `Query` must be evaluated to acquire its sequence of tuples. */
struct Query : DataSource
{
    friend struct QueryGraph;

    private:
    QueryGraph *query_graph_; ///< query graph of the sub-query

    private:
    Query(std::size_t id, const char *alias, QueryGraph *query_graph)
        : DataSource(id, alias), query_graph_(query_graph)
    { }
    public:
    ~Query();

    /** Returns a reference to the internal `QueryGraph`. */
    QueryGraph * query_graph() const { return query_graph_; }

    const char * name() const override { return alias(); }

    bool is_correlated() const override;
};

/** A `Join` in a `QueryGraph` combines `DataSource`s by a join condition. */
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
    /** Adds `condition` to the current condition of this `Join` by logical conjunction. */
    void update_condition(cnf::CNF update) { condition_ = condition_ and update; }
    /** Returns a reference to the joined `DataSource`s. */
    const sources_t & sources() const { return sources_; }

    bool operator==(const Join &other) const {
        return this->condition_ == other.condition_ and equal(this->sources_, other.sources_);
    }
    bool operator!=(const Join &other) const { return not operator==(other); }
};

/** The query graph represents all data sources and joins in a graph structure.  It is used as an intermediate
 * representation of a query. */
struct QueryGraph
{
    friend struct GraphBuilder;
    friend struct Decorrelation;
    friend struct GetPrimaryKey;

    using Subproblem = SmallBitset; ///< encode `QueryGraph::Subproblem`s as `SmallBitset`s
    using projection_type = std::pair<const Expr*, const char*>;
    using order_type = std::pair<const Expr*, bool>; ///< true means ascending, false means descending

    private:
    std::vector<DataSource*> sources_; ///< collection of all data sources in this query graph
    std::vector<Join*> joins_; ///< collection of all joins in this query graph

    std::vector<const Expr*> group_by_; ///< the grouping keys
    std::vector<const Expr*> aggregates_; ///< the aggregates to compute
    std::vector<projection_type> projections_; ///< the data to compute
    std::vector<order_type> order_by_; ///< the order
    struct { uint64_t limit = 0, offset = 0; } limit_; ///< limit: limit and offset

    GetCorrelationInfo *info_ = nullptr; ///< the correlation information about all sources in this graph

    public:
    friend void swap(QueryGraph &first, QueryGraph &second) {
        using std::swap;
        swap(first.sources_,    second.sources_);
        swap(first.joins_,      second.joins_);
        swap(first.group_by_,   second.group_by_);
        swap(first.projections_,   second.projections_);
        swap(first.order_by_,   second.order_by_);
        swap(first.limit_,   second.limit_);
    }

    QueryGraph();
    ~QueryGraph();

    QueryGraph(const QueryGraph&) = delete;
    QueryGraph(QueryGraph &&other) : QueryGraph() { swap(*this, other); }

    QueryGraph & operator=(QueryGraph &&other) { swap(*this, other); return *this; }

    static std::unique_ptr<QueryGraph> Build(const Stmt &stmt);

    /** Returns the number of `DataSource`s in this graph. */
    std::size_t num_sources() const { return sources_.size(); }

    /** Returns the number of `Join`s in this graph. */
    std::size_t num_joins() const { return joins_.size(); }

    void add_source(std::unique_ptr<DataSource> source) {
        source->id_ = sources_.size();
        sources_.emplace_back(source.release());
    }
    BaseTable & add_source(const char *alias, const Table &table) {
        auto base = new BaseTable(sources_.size(), alias, table);
        sources_.emplace_back(base);
        return *base;
    }
    Query & add_source(const char *alias, QueryGraph *query_graph) {
        auto q = new Query(sources_.size(), alias, query_graph);
        sources_.emplace_back(q);
        return *q;
    }

    std::unique_ptr<DataSource> remove_source(std::size_t id) {
        auto it = std::next(sources_.begin(), id);
        auto ds = std::unique_ptr<DataSource>(*it);
        M_insist(ds->id() == id, "IDs of sources must be sequential");
        sources_.erase(it);
        /* Increment IDs of all sources after the deleted one to provide sequential IDs. */
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
    const DataSource * operator[](uint64_t id) const {
        auto ds = sources_[id];
        M_insist(ds->id() == id, "given id and data source id must match");
        return ds;
    }

    /** Returns `true` iff the graph contains a grouping. */
    bool grouping() const { return not group_by_.empty() or not aggregates_.empty(); }
    /** Returns `true` iff the graph is correlated, i.e. it contains a correlated source. */
    bool is_correlated() const;

    private:
    void remove_join(Join *join) {
        auto it = std::find(joins_.begin(), joins_.end(), join);
        if (it == joins_.end())
            throw invalid_argument("given join not found");
        joins_.erase(it);
    }

    public:
    /** Translates the query graph to dot. */
    void dot(std::ostream &out) const;

    /** Translates the query graph to SQL. */
    void sql(std::ostream &out) const;

    void dump(std::ostream &out) const;
    void dump() const;

    private:
    void dot_recursive(std::ostream &out) const;
};


/** An adjacency matrix for a given query graph. Represents the join graph. */
struct AdjacencyMatrix
{
    /** A proxy to access single entries in the `AdjacencyMatrix`. */
    template<bool C>
    struct Proxy
    {
        friend struct AdjacencyMatrix;

        static constexpr bool Is_Const = C;

        private:
        using reference_type = std::conditional_t<Is_Const, const AdjacencyMatrix&, AdjacencyMatrix&>;

        reference_type M_;
        std::size_t i_;
        std::size_t j_;

        Proxy(reference_type M, std::size_t i, std::size_t j) : M_(M) , i_(i), j_(j) {
            M_insist(i < M_.num_vertices_);
            M_insist(j < M_.num_vertices_);
        }

        public:
        operator bool() const { return M_.m_[i_][j_]; }

        template<bool C_ = Is_Const>
        std::enable_if_t<not C_, Proxy&>
        operator=(bool val) { M_.m_[i_][j_] = val; return *this; }

        Proxy & operator=(const Proxy &other) {
            static_assert(not Is_Const, "can only assign to proxy of non-const matrix");
            return operator=(bool(other));
        }
    };

    private:
    std::array<SmallBitset, SmallBitset::CAPACITY> m_; ///< matrix entries
    std::size_t num_vertices_ = 0; ///< number of sources of the `QueryGraph` represented by this matrix

    public:
    AdjacencyMatrix() { }
    AdjacencyMatrix(std::size_t num_vertices) : num_vertices_(num_vertices) { }

    AdjacencyMatrix(const QueryGraph &query_graph)
        : num_vertices_(query_graph.sources().size())
    {
        /* Iterate over all joins in the query graph. */
        for (auto join : query_graph.joins()) {
            if (join->sources().size() != 2)
                throw std::invalid_argument("building adjacency matrix for non-binary join");
            /* Take both join inputs and set the appropriate bit in the adjacency matrix. */
            auto i = join->sources()[0]->id(); // first join input
            auto j = join->sources()[1]->id(); // second join input
            (*this)(i, j) = (*this)(j, i) = true;
        }

    }

    explicit AdjacencyMatrix(const AdjacencyMatrix&) = default;
    AdjacencyMatrix(AdjacencyMatrix&&) = default;

    AdjacencyMatrix & operator=(AdjacencyMatrix&&) = default;

    /** Returns the bit at position `(i, j)`.  Both `i` and `j` must be in bounds. */
    Proxy<true> operator()(std::size_t i, std::size_t j) const { return Proxy<true>(*this, i, j); }

    /** Returns the bit at position `(i, j)`.  Both `i` and `j` must be in bounds. */
    Proxy<false> operator()(std::size_t i, std::size_t j) { return Proxy<false>(*this, i, j); }

    Proxy<true> at(std::size_t i, std::size_t j) const {
        if (i >= num_vertices_ or j >= num_vertices_)
            throw out_of_range("index is out of bounds");
        return operator()(i, j);
    }

    Proxy<false> at(std::size_t i, std::size_t j) {
        if (i >= num_vertices_ or j >= num_vertices_)
            throw out_of_range("index is out of bounds");
        return operator()(i, j);
    }

    /** Computes the set of nodes reachable from `src`, i.e.\ the set of nodes reachable from any node in `src`. */
    SmallBitset reachable(SmallBitset src) const {
        SmallBitset R_old(0);
        SmallBitset R_new(src);
        for (;;) {
            auto R = R_new - R_old;
            if (R.empty()) goto exit;
            R_old = R_new;
            for (auto x : R)
                R_new |= m_[x]; // add all nodes reachable from node `x` to the set of reachable nodes
        }
exit:
        return R_new;
    }

    /** Computes the set of nodes in `S` reachable from `src`, i.e.\ the set of nodes in `S` reachable from any node in
     * `src`. */
    SmallBitset reachable(SmallBitset src, SmallBitset S) const {
        SmallBitset R_old(0);
        SmallBitset R_new(src & S);
        for (;;) {
            auto R = R_new - R_old;
            if (R.empty()) goto exit;
            R_old = R_new;
            for (auto x : R)
                R_new |= m_[x] & S; // add all nodes in `S` reachable from node `x` to the set of reachable nodes
        }
exit:
        return R_new;
    }

    /** Computes the neighbors of `S`.  Nodes from `S` are not part of the neighborhood. */
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
        return not (left & neighbors).empty();
    }

    /** Computes the *transitive closure* of this adjacency matrix.  That is, compute for each pair of vertices *(i, j)*
     * whether *j* can be reached from *i* by any finite path.  Expects that this `AdjacencyMatrix` is symmetric, i.e.
     * that the original graph is undirected.  */
    AdjacencyMatrix transitive_closure_undirected() const;

    /** Enumerate all *connected subgraphs* (CSGs) of the graph induced by vertex super set `super`.  Requires that this
     * matrix is symmetric. */
    void for_each_CSG_undirected(SmallBitset super, std::function<void(SmallBitset)> callback) {
        const SmallBitset X_super = SmallBitset((1UL << num_vertices_) - 1U) - super; // inverse of `super`
        std::deque<std::pair<SmallBitset, SmallBitset>> Q;
        for (auto super_it = super.begin(); super_it != super.end(); ++super_it) {
            SmallBitset I = super_it.as_set();
            Q.emplace_back(I, X_super | ~I.singleton_to_lo_mask()); // vertex i and excluding all "lower" vertices

            while (not Q.empty()) {
                auto [S, X] = Q.front();
                Q.pop_front();

                callback(S);

                const SmallBitset N = neighbors(S) - X;
                for (SmallBitset n = least_subset(N); bool(n); n = next_subset(n, N)) // enumerate 2^{neighbors of S}
                    Q.emplace_back(S | n, X | N);
            }
        }
    }

    /** Compares two `AdjacencyMatrix`s element-wise. */
    bool operator==(const AdjacencyMatrix &other) const {
        if (this->num_vertices_ != other.num_vertices_) return false;
        for (std::size_t i = 0; i != num_vertices_; ++i) {
            if (this->m_[i] != other.m_[i])
                return false;
        }
        return true;
    }
    bool operator!=(const AdjacencyMatrix &other) const { return not operator==(other); }

    friend std::ostream & operator<<(std::ostream &out, const AdjacencyMatrix &M) {
        M.print_fixed_length(out, M.num_vertices_);
        return out;
    }

    friend std::string to_string(const AdjacencyMatrix &M) {
        std::ostringstream oss;
        oss << M;
        return oss.str();
    }

    void print_fixed_length(std::ostream &out, unsigned length) const {
        M_insist(length <= SmallBitset::CAPACITY);
        out << "Adjacency Matrix";
        for (unsigned i = 0; i != length; ++i) {
            out << '\n';
            m_[i].print_fixed_length(out, length);
        }
    }

    void dump(std::ostream &out) const;
    void dump() const;
};

}
