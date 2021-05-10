#include <mutable/IR/QueryGraph.hpp>

#include "catalog/Schema.hpp"
#include "IR/QueryGraph2SQL.hpp"
#include "parse/ASTDumper.hpp"
#include <mutable/parse/AST.hpp>
#include <mutable/util/macro.hpp>
#include <set>
#include <unordered_map>
#include <utility>


using namespace m;

struct Decorrelation;

/*======================================================================================================================
 * BaseTable
 *====================================================================================================================*/

BaseTable::~BaseTable() { for (auto e : expansion_) delete e; };

/*======================================================================================================================
 * Query
 *====================================================================================================================*/

Query::~Query() { delete query_graph_; };

bool Query::is_correlated() const { return query_graph_->is_correlated(); }

/*======================================================================================================================
 * Helper structures
 *====================================================================================================================*/

/** Returns `true` iff both designators has the same textual representation. */
bool equal(const Designator &one, const Designator &two) {
    return streq(to_string(one).c_str(), to_string(two).c_str());
}

/** Like `std::vector::emplace_back()` but adds only iff `pair` is not already contained in `proj`. */
void emplace_back(std::vector<std::pair<const Expr*, const char*>> &pairs,
                  const std::pair<const Designator*, const char*> &pair) {
    for (auto p : pairs) {
        if (auto d = cast<const Designator>(p.first); d and equal(*d, *pair.first))
            return;
    }
    pairs.emplace_back(pair);
}

/** Like `std::vector::emplace_back()` but adds only iff `pair` is not already contained in `proj`. */
void emplace_back(std::vector<std::pair<const Expr*, const char*>> &pairs,
                  const std::pair<const Expr*, const char*> &pair) {
    if (auto d = cast<const Designator>(pair.first))
        emplace_back(pairs, std::pair<const Designator*, const char*>(d, pair.second));
    else if (not contains(pairs, pair))
        pairs.emplace_back(pair);
}

/** Like `std::vector::emplace_back()` but adds only iff `des` is not already contained in `exprs`. */
void emplace_back(std::vector<const Expr*> &exprs, const Designator *des) {
    for (auto e : exprs) {
        if (auto d = cast<const Designator>(e); d and equal(*d, *des))
            return;
    }
    exprs.emplace_back(des);
}

/** Like `std::vector::emplace_back()` but adds only iff `expr` is not already contained in `exprs`. */
void emplace_back(std::vector<const Expr*> &exprs, const Expr *expr) {
    if (auto d = cast<const Designator>(expr))
        emplace_back(exprs, d);
    else if (not contains(exprs, expr))
        exprs.emplace_back(expr);
}

/** Like `std::vector::emplace_back()` but adds only iff `src` is not already contained in `sources`. */
void emplace_back(std::vector<DataSource*> &sources, DataSource *src) {
    for (auto s : sources) {
        if (s->alias() == src->alias())
            return;
    }
    sources.emplace_back(src);
}

/** Like `std::vector::insert()` but adds only those elements of `insertions` which are not already contained in
 * `exprs`. */
void insert(std::vector<const Expr*> &exprs, const std::vector<const Designator*> &insertions) {
    for (auto d : insertions)
        emplace_back(exprs, d);
}

/** Like `std::vector::insert()` but adds only those elements of `insertions` which are not already contained in
 * `exprs`. */
void insert(std::vector<const Expr*> &exprs, const std::vector<const Expr*> &insertions) {
    for (auto e : insertions) {
        if (auto d = cast<const Designator>(e))
            emplace_back(exprs, d);
        else if (not contains(exprs, e))
            exprs.emplace_back(e);
    }
}

/** Helper structure to extract the tables required by an expression. */
struct GetTables : ConstASTExprVisitor
{
    private:
    std::set<const char*> tables_;

    public:
    GetTables() { }

    std::set<const char*> get() { return std::move(tables_); }

    using ConstASTExprVisitor::operator();
    void operator()(Const<ErrorExpr>&) override { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator> &e) override {
        /* check whether this designator refers to an attribute of a table of this query */
        if (not e.is_correlated() and e.has_table_name())
            tables_.emplace(e.get_table_name());
    }

    void operator()(Const<Constant>&) override { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr> &e) override {
        (*this)(*e.fn);
        for (auto arg : e.args)
            (*this)(*arg);
    }

    void operator()(Const<UnaryExpr> &e) override { (*this)(*e.expr); }

    void operator()(Const<BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<QueryExpr>&) override { /* nothing to be done */ }
};

/** Given a clause of a CNF formula, compute the tables that are required by this clause. */
auto get_tables(const cnf::Clause &clause)
{
    using std::begin, std::end;
    GetTables GT;
    for (auto &p : clause)
        GT(*p.expr());
    return GT.get();
}

/** Helper structure to extract the aggregate functions. */
struct GetAggregates : ConstASTExprVisitor, ConstASTClauseVisitor, ConstASTStmtVisitor
{
    private:
    std::vector<const Expr*> aggregates_;

    public:
    GetAggregates() { }

    auto get() { return std::move(aggregates_); }

    using ConstASTExprVisitor::Const;

    /*----- Expressions ----------------------------------------------------------------------------------------------*/
    using ConstASTExprVisitor::operator();
    void operator()(Const<ErrorExpr>&) override { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator>&) override { /* nothing to be done */ }
    void operator()(Const<Constant>&) override { /* nothing to be done */ }
    void operator()(Const<UnaryExpr> &e) override { (*this)(*e.expr); }
    void operator()(Const<BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }
    void operator()(Const<QueryExpr>&) override { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr> &e) override {
        insist(e.has_function());
        if (e.get_function().is_aggregate()) { // test that this is an aggregation
            using std::find_if, std::to_string;
            std::string str = to_string(e);
            auto exists = [&](const Expr *agg) { return to_string(*agg) == str; };
            if (find_if(aggregates_.begin(), aggregates_.end(), exists) == aggregates_.end()) // test if already present
                aggregates_.push_back(&e);
        }
    }

    /*----- Clauses --------------------------------------------------------------------------------------------------*/
    using ConstASTClauseVisitor::operator();
    void operator()(Const<ErrorClause>&) override { unreachable("not implemented"); }
    void operator()(Const<FromClause>&) override { unreachable("not implemented"); }
    void operator()(Const<WhereClause>&) override { unreachable("not implemented"); }
    void operator()(Const<GroupByClause>&) override { unreachable("not implemented"); }
    void operator()(Const<LimitClause>&) override { unreachable("not implemented"); }


    void operator()(Const<SelectClause> &c) override {
        for (auto s : c.select)
            (*this)(*s.first);
    }

    void operator()(Const<HavingClause> &c) override { (*this)(*c.having); }

    void operator()(Const<OrderByClause> &c) override {
        for (auto o : c.order_by)
            (*this)(*o.first);
    }

    /*----- Statements -----------------------------------------------------------------------------------------------*/
    using ConstASTStmtVisitor::operator();
    void operator()(Const<ErrorStmt>&) override { unreachable("not implemented"); }
    void operator()(Const<EmptyStmt>&) override { unreachable("not implemented"); }
    void operator()(Const<CreateDatabaseStmt>&) override { unreachable("not implemented"); }
    void operator()(Const<UseDatabaseStmt>&) override { unreachable("not implemented"); }
    void operator()(Const<CreateTableStmt>&) override { unreachable("not implemented"); }
    void operator()(Const<InsertStmt>&) override { unreachable("not implemented"); }
    void operator()(Const<UpdateStmt>&) override { unreachable("not implemented"); }
    void operator()(Const<DeleteStmt>&) override { unreachable("not implemented"); }
    void operator()(Const<DSVImportStmt>&) override { }

    void operator()(Const<SelectStmt> &s) override {
        if (s.having) (*this)(*s.having);
        (*this)(*s.select);
        if (s.order_by) (*this)(*s.order_by);
    }
};

/** Given a select statement, extract the aggregates to compute while grouping. */
auto get_aggregates(const Stmt &stmt)
{
    GetAggregates GA;
    GA(stmt);
    return GA.get();
}

/** Helper structure to extract the `QueryExpr`s in an expression. */
struct GetQueries : ConstASTExprVisitor
{
    private:
    std::vector<const QueryExpr*> queries_;

    public:
    GetQueries() { }

    std::vector<const QueryExpr*> get() { return std::move(queries_); }

    using ConstASTExprVisitor::operator();
    void operator()(Const<ErrorExpr>&) override { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator>&) override { /* nothing to be done */ }

    void operator()(Const<Constant>&) override { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr>&) override { /* nothing to be done */ }

    void operator()(Const<UnaryExpr> &e) override { (*this)(*e.expr); }

    void operator()(Const<BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<QueryExpr> &e) override { queries_.push_back(&e); }
};

/** Given a clause of a CNF formula, compute the `QueryExpr`s in this clause. */
auto get_queries(const cnf::Clause &clause)
{
    using std::begin, std::end;
    GetQueries GQ;
    for (auto &p : clause)
        GQ(*p.expr());
    return GQ.get();
}

/** Helper structure to compute and provide primary keys. */
struct m::GetPrimaryKey
{
private:
    std::vector<const Expr*> primary_keys_; ///< a list of all primary keys

public:
    GetPrimaryKey() { }

    auto get() { return std::move(primary_keys_); }

    void compute(DataSource *source) {
        if (auto base = cast<BaseTable>(source)) {
            Catalog &C = Catalog::Get();
            Position pos(C.pool("Decorrelation"));
            Token dot(pos, C.pool("."), TK_DOT);
            Token tbl(pos, base->alias(), TK_IDENTIFIER);
            for (auto pkey : base->table().primary_key()) {
                Token attr(pos, pkey->name, TK_IDENTIFIER);
                auto D = new Designator(dot, tbl, attr, pkey->type, pkey);
                primary_keys_.push_back(D);
                base->expansion_.push_back(D);
            }
        } else if (auto query = cast<Query>(source)) {
            auto graph = query->query_graph();
            auto former_size = primary_keys_.size();
            if (graph->grouping()) {
                /* Grouping keys form a new primary key. */
                for (auto e : graph->group_by()) {
                    Catalog &C = Catalog::Get();
                    Position pos(C.pool("Decorrelation"));
                    Token dot(pos, C.pool("."), TK_DOT);
                    Token tbl(pos, query->alias(), TK_IDENTIFIER);
                    std::ostringstream oss;
                    oss << *e;
                    Token attr(pos, C.pool(oss.str().c_str()), TK_IDENTIFIER);
                    auto D = new Designator(dot, tbl, attr, as<const PrimitiveType>(e->type())->as_scalar(), e);
                    primary_keys_.push_back(D);
                }
            } else {
                /* Primary keys of all sources for the primary key. */
                for (auto ds : graph->sources())
                    compute(ds);
            }
            /* Provide all newly inserted primary keys. */
            if (not graph->projection_is_anti() and not graph->projections().empty()) {
                while (former_size != primary_keys_.size()) {
                    emplace_back(graph->projections_,
                                 std::pair<const Expr*, const char*>(primary_keys_.at(former_size),nullptr));
                    ++former_size;
                }
            }
        } else {
            unreachable("invalid variant");
        }
    }
};

/** Given a list of `DataSource`s and a `Query`, compute and provide all primary keys of `source`. */
auto get_primary_key(DataSource *source) {
    GetPrimaryKey GPK;
    GPK.compute(source);
    return GPK.get();
}

/** Helper structure to extract the `Designator`s (and `FnApplicationExpr`s) in an expression or a data source. */
struct GetDesignators : ConstASTExprVisitor
{
    private:
    std::vector<const Expr*> designators_; ///< vector of all designators (and `FnApplicationExpr`s)
    bool aggregates_ = true; ///< indicates whether `FnApplicationExpr`s are considered or only their arguments

    public:
    GetDesignators() { }

    std::vector<const Expr*> get() { return std::move(designators_); }

    void aggregates(bool aggregates) { aggregates_ = aggregates; }

    using ConstASTExprVisitor::operator();
    void operator()(Const<ErrorExpr>&) override { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator> &e) override { designators_.push_back(&e); }

    void operator()(Const<Constant>&) override { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr> &e) override {
        if (aggregates_)
            designators_.push_back(&e);
        else {
            for (auto &arg : e.args)
                (*this)(*arg);
        }
    }

    void operator()(Const<UnaryExpr> &e) override { (*this)(*e.expr); }

    void operator()(Const<BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<QueryExpr>&) override { /* nothing to be done */ }
};

/** Given a query graph and a query within the graph, compute all needed attributes by `graph` and `query`, i.e. those
 * in the filter and joins of `query` plus those in the grouping (or the projection if no grouping exists). */
auto get_needed_attrs(const QueryGraph *graph, const Query &query)
{
    insist(contains(graph->sources(), &query));

    GetDesignators GD;
    auto it = std::find(graph->sources().begin(), graph->sources().end(), &query);
    for (auto &c : (*it)->filter()) {
        for (auto &p : c)
            GD(*p.expr());
    }
    for (auto &j : (*it)->joins()) {
        for (auto &c : j->condition()) {
            for (auto &p : c)
                GD(*p.expr());
        }
    }
    if (not graph->group_by().empty() or not graph->aggregates().empty()) {
        for (auto &e : graph->group_by())
            GD(*e);
        GD.aggregates(false); // to search for needed attributes in the arguments of aggregates
        for (auto &e : graph->aggregates())
            GD(*e);
    } else {
        for (auto &e : *graph->expanded_projections())
            GD(*e);
        for (auto &e : graph->projections())
            GD(*e.first);
    }
    return GD.get();
}

/** Helper structure to replace designators in an `Expr`. */
struct ReplaceDesignators : ConstASTExprVisitor
{
    private:
    const std::vector<const Expr*> *origin_; ///< list of all reachable expressions
    Designator *new_designator_ = nullptr; ///< the newly generated designator to use, `nullptr` if nothing to replace
    std::vector<const Expr*> deletions_; ///< all removed expressions which have to be deleted manually

    public:
    ReplaceDesignators() { }

    /** Replaces all designators all predicates of `cnf` by new ones pointing to the respective designator in `origin`.
     *  Returns all replaced designators.
     *  IMPORTANT: Replaced designators are neither deleted by this structure nor by `~Expr()` anymore;
     *             caller must delete them!!! */
    std::vector<const Expr*> replace(cnf::CNF &cnf, const std::vector<const Expr*> *origin,
                                     bool target_deleted = false)
    {
        origin_ = origin;
        deletions_.clear();
        cnf::CNF cnf_new;
        for (auto &c : cnf) {
            cnf::Clause c_new;
            for (auto &p : c) {
                auto e = p.expr();
                (*this)(*e);
                if (cast<const Designator>(e) and new_designator_) {
                    /* `e` has to be replaced by `new_designator_`. `e` has to be deleted manually iff the target is
                     * deleted (since it is removed from `target`) and `new_designator` has to be deleted manually iff the
                     * target is not deleted. */
                    c_new = c_new or cnf::Clause({cnf::Predicate::Create(new_designator_, p.negative())});
                    if (target_deleted)
                        deletions_.push_back(e);
                    else
                        deletions_.push_back(new_designator_);
                    new_designator_ = nullptr;
                } else {
                    c_new = c_new or cnf::Clause({p});
                }
            }
            cnf_new = cnf_new and cnf::CNF({c_new});
        }
        cnf = std::move(cnf_new);
        return std::move(deletions_);
    }

    /** Replaces all designators in `target` by new ones pointing to the respective designator in `origin`.
     *  Returns all replaced designators. `target_deleted` indicates whether `target` is deleted by another structure.
     *  IMPORTANT: Replaced designators are neither deleted by this structure nor by `~Expr()` anymore;
     *             caller must delete them!!! */
    std::vector<const Expr*> replace(std::vector<const Expr*> &target, const std::vector<const Expr*> *origin,
                               bool target_deleted = false) {
        origin_ = origin;
        deletions_.clear();
        std::vector<const Expr*> target_new;
        for (auto e : target) {
            (*this)(*e);
            if (cast<const Designator>(e) and new_designator_) {
                /* `e` has to be replaced by `new_designator_`. `e` has to be deleted manually iff the target is
                 * deleted (since it is removed from `target`) and `new_designator` has to be deleted manually iff the
                 * target is not deleted. */
                target_new.emplace_back(new_designator_);
                if (target_deleted)
                    deletions_.push_back(e);
                else
                    deletions_.push_back(new_designator_);
                new_designator_ = nullptr;
            } else {
                target_new.emplace_back(e);
            }
        }
        target = std::move(target_new);
        return std::move(deletions_);
    }

    /** Replaces all designators in `target.first` by new ones pointing to the respective designator in `origin`.
     *  Returns all replaced designators.
     *  IMPORTANT: Replaced designators are neither deleted by this structure nor by `~Expr()` anymore;
     *             caller must delete them!!! */
    std::vector<const Expr*> replace(std::vector<std::pair<const Expr*, const char*>> &target,
                               const std::vector<const Expr*> *origin, bool target_deleted = false)
    {
        origin_ = origin;
        deletions_.clear();
        std::vector<std::pair<const Expr*, const char*>> target_new;
        for (auto e : target) {
            (*this)(*e.first);
            if (cast<const Designator>(e.first) and new_designator_) {
                /* `e.first` has to be replaced by `new_designator_`. `e.first` has to be deleted manually iff the
                 * target is deleted (since it is removed from `target`) and `new_designator` has to be deleted manually
                 * iff the target is not deleted. */
                target_new.emplace_back(new_designator_, e.second);
                if (target_deleted)
                    deletions_.push_back(e.first);
                else
                    deletions_.push_back(new_designator_);
                new_designator_ = nullptr;
            } else {
                target_new.emplace_back(e);
            }
        }
        target = std::move(target_new);
        return std::move(deletions_);
    }

    private:
    using ConstASTExprVisitor::operator();
    void operator()(Const<ErrorExpr>&) override { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator> &e) override {
        if (auto d = findDesignator(e))
            new_designator_ = make_designator(d);
    }

    void operator()(Const<Constant>&) override { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr> &e) override {
        std::vector<Expr*> args_new;
        for (auto arg : e.args) {
            (*this)(*arg);
            if (new_designator_) {
                /* Replace `arg` by the newly generated designator. */
                deletions_.push_back(arg);
                args_new.emplace_back(new_designator_);
                new_designator_ = nullptr;
            } else {
                args_new.emplace_back(arg);
            }
        }
        const_cast<FnApplicationExpr*>(&e)->args = std::move(args_new);
    }

    void operator()(Const<UnaryExpr> &e) override {
        (*this)(*e.expr);
        if (new_designator_) {
            /* Replace `expr` by the newly generated designator. */
            deletions_.push_back(e.expr);
            const_cast<UnaryExpr*>(&e)->expr = new_designator_;
            new_designator_ = nullptr;
        }
    }

    void operator()(Const<BinaryExpr> &e) override {
        (*this)(*e.lhs);
        if (new_designator_) {
            /* Replace `expr` by the newly generated designator. */
            deletions_.push_back(e.lhs);
            const_cast<BinaryExpr*>(&e)->lhs = new_designator_;
            new_designator_ = nullptr;
        }
        (*this)(*e.rhs);
        if (new_designator_) {
            /* Replace `expr` by the newly generated designator. */
            deletions_.push_back(e.rhs);
            const_cast<BinaryExpr*>(&e)->rhs = new_designator_;
            new_designator_ = nullptr;
        }
    }

    void operator()(Const<QueryExpr>&) override { /* nothing to be done */ }

    /** Returns a `Designator` equal to `des` iff it exists in `origin_`, and `nullptr` otherwise. */
    const Designator * findDesignator(const Designator &des) {
        for (auto e : *origin_) {
            if (auto d = cast<const Designator>(e)) {
                if (equal(*d, des))
                    return d;
            }
        }
        return nullptr;
    }

    /** Returns a designator with an empty table name, the textual representation of `d` as attribute
     * name, the same type as `d`, and `d` as target. */
     Designator * make_designator(const Designator *d) {
        Catalog &C = Catalog::Get();
        Position pos(C.pool("Decorrelation"));
        Token dot(pos, C.pool("."), TK_DOT);
        std::ostringstream oss;
        oss << *d;
        Token attr(pos, C.pool(oss.str().c_str()), TK_IDENTIFIER);
        auto D = new Designator(dot, Token(), attr, as<const PrimitiveType>(d->type())->as_scalar(), d);
        return D;
    }
};

/** Helper structure to extract correlation information of a `cnf:CNF` formula. */
struct m::GetCorrelationInfo : ConstASTExprVisitor
{
    friend struct Decorrelation;

    /** Holds correlation information of a single `cnf::Clause`. */
    struct CorrelationInfo : ConstASTExprVisitor
    {
        cnf::Clause clause; ///< the `cnf::Clause` the information is about
        std::set<const Expr*> uncorrelated_exprs; ///< a set of all uncorrelated designators and aggregates in the `clause`
        std::set<const char*> sources; ///< a set of all sources of correlated designators occurring in the `clause`
        private:
        const char *table_name_; ///< the new table name used in `replace()`
        std::unordered_map<const Designator*, const Designator*> old_to_new_outer_; ///< maps old designators to new ones generated by the caller
        std::unordered_map<const Designator*, Designator*> old_to_new_inner_; ///< maps old designators to new ones generated by `replace()`
        bool finishDecorrelation_ = false; ///< indicates whether the decorrelation process is finsihed, used to remove the decorrelation flag of designators

        public:
        CorrelationInfo(cnf::Clause clause) : clause(std::move(clause)) { }

        /** Updates `uncorrelated_exprs` according to `old_to_new` and replaces all table names of all designators in
         *  this list by `table_name`. Apply all these changes to the `clause`, too.
         *  IMPORTANT: Replaced designators are neither deleted by this structure nor by `~Expr()` anymore;
         *             caller must delete them!!! */
        void replace(std::unordered_map<const Designator*, const Designator*> &old_to_new, const char *table_name) {
            table_name_ = table_name;
            old_to_new_outer_ = std::move(old_to_new);
            /* Update `clause`. */
            for (auto &p : clause)
                (*this)(*p.expr());
            /* Update `uncorrelated_exprs`. */
            std::set<const Expr*> uncorrelated_exprs_new;
            for (auto e : uncorrelated_exprs) {
                if (auto d = cast<const Designator>(e)) {
                    try {
                        uncorrelated_exprs_new.emplace(old_to_new_inner_.at(get_new(d)));
                    } catch (std::out_of_range) {
                        unreachable("every designator in `uncorrelated_exprs` has to be deleted and, therefore, must "
                                    "occur in `old_to_new_inner_`");
                    }
                } else {
                    uncorrelated_exprs_new.emplace(e);
                }
            }
            uncorrelated_exprs = std::move(uncorrelated_exprs_new);
            old_to_new_inner_.clear();
        }

        /** Remove decorrelation flag of all occurring designators in `clause`. */
        void finishDecorrelation() {
            finishDecorrelation_ = true;
            for (auto &p : clause)
                (*this)(*p.expr());
        }

        private:
        using ConstASTExprVisitor::operator();
        void operator()(Const<ErrorExpr>&) override { unreachable("graph must not contain errors"); }

        void operator()(Const<Designator> &d) override { if (finishDecorrelation_) d.decorrelate(); }

        void operator()(Const<Constant>&) override { /* nothing to be done */ }

        void operator()(Const<FnApplicationExpr>&) override { /* nothing to be done */ }

        void operator()(Const<UnaryExpr> &e) override {
            if (finishDecorrelation_) {
                (*this)(*e.expr);
            } else {
                if (auto d = cast<const Designator>(e.expr); d and contains(uncorrelated_exprs, d)) {
                    /* Replace this designator by an equivalent new one with table name `table_name_`. */
                    const_cast<UnaryExpr *>(&e)->expr = set_table_name(get_new(d));
                } else {
                    (*this)(*e.expr);
                }
            }
        }

        void operator()(Const<BinaryExpr> &e) override {
            if (finishDecorrelation_) {
                (*this)(*e.lhs);
                (*this)(*e.rhs);
            } else {
                if (auto d = cast<const Designator>(e.lhs); d and contains(uncorrelated_exprs, d)) {
                    /* Replace this designator by an equivalent new one with table name `table_name_`. */
                    const_cast<BinaryExpr *>(&e)->lhs = set_table_name(get_new(d));
                } else {
                    (*this)(*e.lhs);
                }
                if (auto d = cast<const Designator>(e.rhs); d and contains(uncorrelated_exprs, d)) {
                    /* Replace this designator by an equivalent new one with table name `table_name_`. */
                    const_cast<BinaryExpr *>(&e)->rhs = set_table_name(get_new(d));
                } else {
                    (*this)(*e.rhs);
                }
            }
        }

        void operator()(Const<QueryExpr>&) override { /* nothing to be done */ }

        const Designator * get_new(const Designator *d) {
            try {
                return old_to_new_outer_.at(d);
            } catch (std::out_of_range) {
                return d;
            }
        }

        Designator * set_table_name(const Designator *d) {
            Catalog &C = Catalog::Get();
            Position pos(C.pool("Decorrelation"));
            Token dot(pos, C.pool("."), TK_DOT);
            Token tbl(pos, C.pool(table_name_), TK_IDENTIFIER);
            auto D = new Designator(dot, tbl, d->attr_name, d->type(), d);
            old_to_new_inner_.emplace(d, D);
            return D;
        }
    };

    private:
    std::vector<CorrelationInfo*> equi_; ///< a list of all correlation information of equi-predicates
    std::vector<CorrelationInfo*> non_equi_; ///< a list of all correlation information of non-equi-predicates

    CorrelationInfo *info_; ///< the current `CorrelationInfo` object

    std::vector<const Expr*> expansion_; ///< list of designators expanded by decorrelation steps

    public:
    GetCorrelationInfo() { }
    ~GetCorrelationInfo() {
        for (auto d : expansion_)
            delete d;
    }

    /** Given a cnf::CNF formula, compute all correlation information of this formula.
     *  Return the remaining `cnf::CNF` which only consists of uncorrelated predicates. */
    cnf::CNF compute(const cnf::CNF &cnf) {
        cnf::CNF res;
        for (auto &c : cnf) {
            info_ = new CorrelationInfo(c);
            bool is_equi = c.size() <= 1; // the clause can only be an equi-predicate if there is no disjunction
            for (auto &p : c) {
                (*this)(*p.expr());
                if (p.negative())
                    is_equi = is_equi and is_not_equal(p.expr());
                else
                    is_equi = is_equi and is_equal(p.expr());
            }
            if (info_->sources.empty()) {
                /* c is not correlated. */
                res = res and cnf::CNF({c});
                delete info_;
            } else {
                /* c is correlated. */
                if (is_equi)
                    equi_.push_back(info_);
                else
                    non_equi_.push_back(info_);
            }
        }
        return res;
    }

    /** Returns a list of all correlation information of equi-predicates. */
    std::vector<CorrelationInfo*> getEqui() { return std::move(equi_); }
    /** Returns a list of all correlation information of non-equi-predicates. */
    std::vector<CorrelationInfo*> getNonEqui() { return std::move(non_equi_); }

    bool empty() { return equi_.empty() and non_equi_.empty(); }
    bool non_equi() { return not non_equi_.empty(); }

    private:
    using ConstASTExprVisitor::operator();
    void operator()(Const<ErrorExpr>&) override { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator> &e) override {
        if (e.is_correlated()) {
            auto names = find_table_name(e);
            info_->sources.insert(names.begin(), names.end());
        } else {
            info_->uncorrelated_exprs.emplace(&e);
        }
    }

    void operator()(Const<Constant>&) override { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr> &e) override { info_->uncorrelated_exprs.emplace(&e); }

    void operator()(Const<UnaryExpr> &e) override { (*this)(*e.expr); }

    void operator()(Const<BinaryExpr> &e) override { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<QueryExpr>&) override { /* nothing to be done */ }

    bool is_equal(const Expr *e) {
        if (auto b = cast<const BinaryExpr>(e))
            return b->op() == TK_EQUAL;
        else
            return true;
    }

    bool is_not_equal(const Expr *e) {
        if (auto b = cast<const BinaryExpr>(e))
            return b->op() == TK_BANG_EQUAL;
        else
            return true;
    }

    std::set<const char*> find_table_name(const Designator &d) {
        if (d.has_table_name()) return {d.get_table_name()};
        if (std::holds_alternative<const Attribute *>(d.target())) {
            return {std::get<const Attribute *>(d.target())->table.name};
        } else if (std::holds_alternative<const Expr*>(d.target())) {
            GetDesignators GD;
            GD(*std::get<const Expr*>(d.target()));
            std::set<const char*> res;
            for (auto des : GD.get()) {
                /* Skip `FnApplicationExpr`s because they have no table name. */
                if (auto d = cast<const Designator>(des)) {
                    auto names = find_table_name(*d);
                    res.insert(names.begin(), names.end());
                }
            }
            return res;
        } else {
            unreachable("target can not be `std::monostate`");
        }
    }
};

/** The method that decorrelates a `Query` in a `QueryGraph`. */
struct m::Decorrelation
{
    private:
    using graphs_t = std::pair<QueryGraph*, Query*>; ///> `first` is the graph of `second`

    Query &query_; ///< the query to decorrelate
    QueryGraph *graph_; ///< the query graph containing `query_`
    std::vector<graphs_t> graphs_; ///< a list of query graphs containing correlated predicates, grows by nested `Query`s
    std::vector<const Expr*> needed_attrs_; ///< a list of all needed attributes of `graph_`

    public:
    Decorrelation(Query &query, QueryGraph *graph)
        : query_(query), graph_(graph), graphs_({{graph, nullptr}}) { }

    /** Decorrelates `query_` by predicate push-up.
     *  Returns `true` if all correlated predicates are decorrelated. */
    bool decorrelate() {
        /* Initialize `graphs_`. */
        searchGraphs(query_.query_graph(), &query_);

        /* Push-up all equi-predicates (starting with the innermost query) until the first non-equi-predicate below a
         * grouping is reached. */
        while (graphs_.size() > 1) {
            auto last = graphs_.back();
            if (last.first->info_->non_equi() and last.first->grouping()) break;
            graphs_.pop_back();
            pushPredicates(last.first, graphs_.back().first, last.second);
        }

        insist(not graphs_.empty());

        if (graphs_.size() > 1) {
            /* Initialize `needed_attrs_` for operator push-up. */
            needed_attrs_ = get_needed_attrs(graph_, query_);
            /* Provide all table names explicitly because the attribute name does not have to be unique anymore. */
            for (auto des : needed_attrs_) {
                /* Skip `FnApplicationExpr`s because they have no table name. */
                if (auto d = cast<const Designator>(des)) {
                    if (not d->has_explicit_table_name()) {
                        if (not d->table_name.text) {
                            /* Table name not deduced. Only possible iff it occurs after grouping and, therefore, set
                             * table name to "HAVING". */
                            Catalog &C = Catalog::Get();
                            const_cast<Designator *>(d)->table_name.text = C.pool("HAVING");
                        }
                        const_cast<Designator *>(d)->table_name.type = TK_IDENTIFIER;
                    }
                }
            }

            /* Push-up all operators until the innermost non-equi-predicate is reached. */
            for (auto it = graphs_.begin(); it != std::prev(graphs_.end());) {
                auto &upper = *it;
                ++it;
                auto &lower = *it;
                pushOperators(lower.first, upper.first, lower.second);
            }

            /* Adapt all designators above the innermost non-equi-predicate. */
            auto *origin = &graphs_.back().first->group_by_;
            insist(not origin->empty());
            for (auto i = graphs_.size() - 1; i != 0; --i) {
                auto &upper = graphs_[i - 1];
                insist(not origin->empty());
                replace_designators(upper.first, origin);
                if (upper.first->grouping())
                    origin = &upper.first->group_by_;
            }
        }

        auto iterator = graphs_.end(); // iterator to the innermost graph which is not completely decorrelated
        for (auto it = graphs_.begin(); it != graphs_.end(); ++it) {
            if (not (*it).first->info_->empty()) {
                /* In this graph not all correlated predicates are decorrelated. */
                iterator = it;
            }
        }
        if (iterator == graphs_.end()) {
            /* All correlated predicates are decorrelated. */
            return true;
        } else {
            /* Not all correlated predicates are decorrelated. Unset `decorrelated_`-flag of all queries until the
             * innermost not decorrelated predicate. */
            for (auto it = graphs_.begin(); it != iterator;)
                (*++it).second->decorrelated_ = false;
            return false;
        }
    }

    private:
    /** Adds all not yet decorrelated query graphs to `graphs_` starting with `graph`. */
    void searchGraphs(QueryGraph *graph, Query *query) {
        graphs_.emplace_back(graph, query);

        bool found = false;
        for (auto ds : graph->sources()) {
            if (ds->decorrelated_) continue;
            insist(not found, "Multiple not yet decorrelated nested queries in one graph should not be possible.");
            found = true;
            auto q = as<Query>(ds);
            searchGraphs(q->query_graph(), q);
        }
    }

    /** Pushes all correlated predicates contained in `lower` up into `upper`. Therefore, the projection and grouping
     *  of `lower` are adapted and a join between `query` and all involved sources of each predicate is added in `upper`
     *  iff all sources are reachable.
     *  Otherwise, the correlated predicate has to be pushed further up and, therefore, is added to `upper.info_`. */
    void pushPredicates(QueryGraph *lower, QueryGraph *upper, Query *query) {
        insist(query->query_graph() == lower);
        insist(contains(upper->sources(), query));

        auto projection = lower->projection_is_anti() or not lower->projections().empty();
        auto grouping = lower->grouping();
        auto equi = lower->info_->getEqui();
        auto non_equi = lower->info_->getNonEqui();
        lower->info_->equi_.clear();
        lower->info_->non_equi_.clear();
        bool is_equi = true;
        insist(non_equi.empty() or not grouping);
        for (auto it = equi.begin(); it != non_equi.end(); ++it) {
            if (it == equi.end()) {
                it = non_equi.begin(); // to iterate over equi and non_equi
                is_equi = false;
                if (it == non_equi.end()) break; // to skip empty non_equi
            }
            auto i = *it;
            /* Adapt projection and grouping. */
            std::unordered_map<const Designator*, const Designator*> old_to_new;
            for (auto e : i->uncorrelated_exprs) {
                auto d = cast<const Designator>(e);
                auto e_new = e;
                if (grouping) {
                    emplace_back(lower->group_by_, e);
                    if (d) {
                        /* Create a new designator pointing to `d` (the grouping key) and replace `d` by it in
                         * `i->uncorrelated_designators`. */
                        Catalog &C = Catalog::Get();
                        std::ostringstream oss;
                        oss << *d;
                        Position pos(C.pool("Decorrelation"));
                        Token attr(pos, C.pool(oss.str().c_str()), TK_IDENTIFIER);
                        auto d_new = new Designator(attr, Token(), attr, as<const PrimitiveType>(d->type())->as_scalar(), d);
                        lower->info_->expansion_.push_back(d_new);
                        old_to_new.emplace(d, d_new);
                        e_new = d_new;
                    }
                }
                if (projection) {
                    emplace_back(lower->projections_, std::pair<const Expr *, const char *>(e_new, nullptr));
                }
                if (d)
                    lower->info_->expansion_.push_back(d); // see comment of `CorrelationInfo::replace()`
            }
            /* Update `i->uncorrelated_expr` and `i->clause` by replacing the table name of all uncorrelated
             * designators by the alias of `query` because it will be pushed up and therefore the former sources are
             * only reachable via `query`. */
            i->replace(old_to_new, query->alias());
            /* Search needed sources. */
            std::vector<DataSource*> sources{query};
            auto all_reachable = true;
            for (auto s : i->sources) {
                if (auto src = findSource(upper->sources(), s)) {
                    emplace_back(sources, src);
                } else {
                    /* The source s is not reachable. */
                    all_reachable = false;
                    break;
                }
            }
            /* Add join or rather filter. */
            if (all_reachable) {
                i->finishDecorrelation();
                if (auto j = findJoin(upper->joins(), sources)) {
                    /* A join with the same data sources already exists so only update the predicate. */
                    j->update_condition(cnf::CNF({i->clause}));
                } else {
                    /* Create a new join for the data sources. */
                    auto J = upper->joins_.emplace_back(new Join(cnf::CNF({i->clause}), sources));
                    for (auto ds : J->sources())
                        ds->add_join(J);
                }
                delete i;
            } else {
                /* Add `i` to the upper correlation information. */
                if (is_equi)
                    upper->info_->equi_.push_back(i);
                else
                    upper->info_->non_equi_.push_back(i);
            }
        }
        query->decorrelated_ = true;
    }

    /** Pushes all operators (projection and grouping) contained in `lower` up above the sources of `upper`. Therefore,
     *  the projection and grouping of `lower` are adapted and the sources of `upper` are added to the sources of
     *  `lower`. Furthermore, the former join of `query` is transformed into its filter and a join between all
     *  involved sources of each predicate is added in `lower` iff all sources are reachable.
     *  Otherwise, the operators of the graph below has to be pushed further up and, therefore, the correlated
     *  predicate is still contained in `lower.info_`. */
    void pushOperators(QueryGraph *lower, QueryGraph *upper, Query *query) {
        insist(query->query_graph() == lower);
        insist(contains(upper->sources(), query));
        auto lower_sources = lower->sources();

        /* Remove joins of `query` and add conditions as filter. */
        for (auto j : query->joins()) {
            query->update_filter(j->condition());
            upper->remove_join(j);
            for (auto ds : j->sources())
                ds->remove_join(j);
            delete j;
        }
        auto grouping = lower->grouping();
        /* Adapt projection of `lower`. */
        if (lower->projection_is_anti() or not lower->projections().empty())
            lower->projection_is_anti_ = true;
        /* Move all sources except `query` from `upper` to `lower` and adapt grouping of `lower`. Change source id
         * such that the id's of all sources of `lower` are sequential. */
        size_t num_sources = lower->sources().size();
        for (auto it = upper->sources_.begin(); it != upper->sources_.end(); ++it) {
            if (*it == query) continue;
            (*it)->id_ = num_sources++;
            lower->sources_.emplace_back(*it);
            if (grouping) {
                auto primary_key = get_primary_key(*it);
                insist(not primary_key.empty(), "can not push-up grouping above source without primary key");
                insert(lower->group_by_, primary_key);
            }
        }
        query->id_ = 0;
        upper->sources_ = {query};
        /* Move all joins of `upper` to `lower`. */
        lower->joins_.insert(lower->joins_.end(), upper->joins_.begin(), upper->joins_.end());
        upper->joins_.clear();
        /* Add all later needed attributes (of `graph_`) to grouping of `lower`. */
        if (grouping)
            insert(lower->group_by_, needed_attrs_);
        /* Add joins for correlated predicates iff possible. */
        auto equi = lower->info_->getEqui();
        auto non_equi = lower->info_->getNonEqui();
        lower->info_->equi_.clear();
        lower->info_->non_equi_.clear();
        bool is_equi = true;
        for (auto it = equi.begin(); it != non_equi.end(); ++it) {
            if (it == equi.end()) {
                it = non_equi.begin(); // to iterate over equi and non_equi
                is_equi = false;
                if (it == non_equi.end()) break; // to skip empty non_equi
            }
            auto i = *it;
            /* Search needed sources. Since `lower` could initially contain multiple sources which contribute to this
             * join we have to check the table names of uncorrelated designators, too. */
            std::vector<DataSource*> sources;
            auto all_reachable = true;
            for (auto e : i->uncorrelated_exprs) {
                if (auto d = cast<const Designator>(e)) {
                    if (auto src = findSource(lower->sources(), d->get_table_name())) {
                        emplace_back(sources, src);
                    } else {
                        /* The source d->get_table_name() is not reachable. */
                        all_reachable = false;
                        break;
                    }
                } else {
                    /* Aggregation can not be assigned to a specific source so add all initially available ones. */
                    sources = lower_sources;
                    break;
                }
            }
            for (auto s : i->sources) {
                if (auto src = findSource(lower->sources(), s)) {
                    emplace_back(sources, src);
                } else {
                    /* The source s is not reachable. */
                    all_reachable = false;
                    break;
                }
            }
            /* Add join. */
            if (all_reachable) {
                i->finishDecorrelation();
                if (auto j = findJoin(lower->joins(), sources)) {
                    /* A join with the same data sources already exists so only update the predicate. */
                    j->update_condition(cnf::CNF({i->clause}));
                } else {
                    /* Create a new join for the data sources. */
                    auto J = lower->joins_.emplace_back(new Join(cnf::CNF({i->clause}), sources));
                    for (auto ds : J->sources())
                        ds->add_join(J);
                }
                delete i;
            } else {
                /* Add `i` again to the correlation information. */
                if (is_equi)
                    lower->info_->equi_.push_back(i);
                else
                    lower->info_->non_equi_.push_back(i);
            }
        }
    }

    /** Returns a `DataSource *` pointing to a data source with the same name as `name` iff it exists in `sources`,
     *  and `nullptr` otherwise. */
    DataSource * findSource(const std::vector<DataSource*> &sources, const char *name) {
        for (auto s : sources) {
            if (auto q = cast<Query>(s); name == s->alias() or (q and findSource(q->query_graph()->sources(), name)))
                return s;
        }
        return nullptr;
    }

    /** Returns a `Join *` pointing to a join with the same sources as `sources` iff it exists in `joins`, and
     * `nullptr` otherwise. */
    Join * findJoin(const std::vector<Join*> &joins, const std::vector<DataSource*> &sources) {
        std::unordered_set<DataSource*> cmp(sources.begin(), sources.end());
        for (auto j : joins) {
            if (cmp == std::unordered_set<DataSource*>(j->sources().begin(), j->sources().end()))
                return j;
        }
        return nullptr;
    }


    /** Replaces all designators in `graph` by new ones pointing to the respective designator in `origin`. */
    void replace_designators(QueryGraph *graph, const std::vector<const Expr*> *origin) {
        insist(graph->sources().size() == 1);
        ReplaceDesignators RP;
        auto ds = *graph->sources().begin();
        auto filter_new = ds->filter();
        auto expansions = RP.replace(filter_new, origin);
        ds->filter_ = filter_new;
        graph->info_->expansion_.insert(graph->info_->expansion_.end(), expansions.begin(), expansions.end());
        if (graph->grouping()) {
            expansions = RP.replace(graph->group_by_, origin);
            graph->info_->expansion_.insert(graph->info_->expansion_.end(), expansions.begin(), expansions.end());
            expansions = RP.replace(graph->aggregates_, origin);
            graph->info_->expansion_.insert(graph->info_->expansion_.end(), expansions.begin(), expansions.end());
            expansions = RP.replace(graph->projections_, &graph->group_by());
            graph->info_->expansion_.insert(graph->info_->expansion_.end(), expansions.begin(), expansions.end());
            expansions = RP.replace(*graph->expanded_projections_, &graph->group_by(), true);
            graph->info_->expansion_.insert(graph->info_->expansion_.end(), expansions.begin(), expansions.end());
        } else {
            expansions = RP.replace(graph->projections_, origin);
            graph->info_->expansion_.insert(graph->info_->expansion_.end(), expansions.begin(), expansions.end());
            expansions = RP.replace(*graph->expanded_projections_, origin, true);
            graph->info_->expansion_.insert(graph->info_->expansion_.end(), expansions.begin(), expansions.end());
        }
    }
};

/*======================================================================================================================
 * GraphBuilder
 *
 * An AST Visitor that constructs the query graph.
 *====================================================================================================================*/

struct m::GraphBuilder : ConstASTStmtVisitor
{
    private:
    std::unique_ptr<QueryGraph> graph_; ///< the constructed query graph
    std::unordered_map<const char*, DataSource*> aliases_; ///< maps aliases to data sources

    public:
    GraphBuilder() : graph_(std::make_unique<QueryGraph>()) { }

    std::unique_ptr<QueryGraph> get() { return std::move(graph_); }

    using ConstASTStmtVisitor::operator();
    void operator()(Const<Stmt> &s) { s.accept(*this); }
    void operator()(Const<ErrorStmt>&) { unreachable("graph must not contain errors"); }

    void operator()(Const<EmptyStmt>&) { /* nothing to be done */ }

    void operator()(Const<CreateDatabaseStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<UseDatabaseStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<CreateTableStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<SelectStmt> &s) {
        Catalog &C = Catalog::Get();
        bool has_nested_query = false;

        /* Create data sources. */
        if (s.from) {
            auto F = as<FromClause>(s.from);
            for (auto &tbl : F->from) {
                if (auto tok = std::get_if<Token>(&tbl.source)) {
                    /* Create a new base table. */
                    insist(tbl.has_table());
                    Token alias = tbl.alias ? tbl.alias : *tok;
                    auto &base = graph_->add_source(alias.text, tbl.table());
                    aliases_.emplace(alias.text, &base);
                } else if (auto stmt = std::get_if<Stmt*>(&tbl.source)) {
                    insist(tbl.alias.text, "every nested statement requires an alias");
                    if (auto select = cast<SelectStmt>(*stmt)) {
                        /* Create a graph for the sub query. */
                        GraphBuilder builder;
                        builder(*select);
                        auto graph = builder.get();
                        auto &q = graph_->add_source(tbl.alias.text, graph.release());
                        insist(tbl.alias);
                        aliases_.emplace(tbl.alias.text, &q);
                    } else
                        unreachable("invalid variant");
                } else {
                    unreachable("invalid variant");
                }
            }
        }

        /* Do some computation before first nested query is decorrelated to ensure that `get_needed_attrs()` works as
         * intended: */

        /* Add groups. */
        if (s.group_by) {
            auto G = as<GroupByClause>(s.group_by);
            graph_->group_by_.insert(graph_->group_by_.begin(), G->group_by.begin(), G->group_by.end());
        }

        /* Add aggregates. */
        graph_->aggregates_ = get_aggregates(s);

        /* Add projections. */
        auto S = as<SelectClause>(s.select);
        graph_->projection_is_anti_ = S->select_all;
        graph_->expanded_projections_ = &S->expansion;
        std::vector<std::pair<const QueryExpr*, const char*>> nested_queries_select;
        for (auto s : S->select) {
            if (auto query = cast<const QueryExpr>(s.first)) {
                nested_queries_select.emplace_back(query, s.second.text);
            } else {
                graph_->projections_.emplace_back(s.first, s.second.text);
            }
        }

        /* Compute CNF of WHERE clause. */
        cnf::CNF cnf_where;
        if (s.where) {
            auto W = as<WhereClause>(s.where);
            cnf_where = cnf::to_CNF(*W->where);
        }

        /* Dissect CNF into joins, filters and nested queries. Decorrelate if possible. */
        /* TODO iterate over clauses in the order joins < filters/nested queries (equi) < nested queries (non-equi) */
        for (auto &clause : cnf_where) {
            auto queries = get_queries(clause);
            if (queries.empty()) {
                auto tables = get_tables(clause);
                /* Compute correlation information of this clause analogously. */
                if (tables.size() == 0) {
                    /* This clause is a filter and constant.  It applies to all data sources. */
                    for (auto &alias : aliases_)
                        alias.second->update_filter(graph_->info_->compute(cnf::CNF{clause}));
                } else if (tables.size() == 1) {
                    /* This clause is a filter condition. */
                    auto t = *begin(tables);
                    auto ds = aliases_.at(t);
                    ds->update_filter(graph_->info_->compute(cnf::CNF({clause})));
                } else {
                    /* This clause is a join condition. */
                    Join::sources_t sources;
                    for (auto t : tables)
                        sources.emplace_back(aliases_.at(t));
                    auto J = graph_->joins_.emplace_back(new Join(graph_->info_->compute(cnf::CNF({clause})), sources));
                    for (auto ds : J->sources())
                        ds->add_join(J);
                }
            } else {
                has_nested_query = true;
                insist(queries.size() == 1, "Multiple nested queries in one clause are not yet supported.");
                auto query = as<const QueryExpr>(*queries.begin());
                if (auto select = cast<SelectStmt>(query->query)) {
                    auto selectClause = as<SelectClause>(select->select);
                    insist(not selectClause->select_all, "* can not yet be used in nested queries.");
                    if (selectClause->select.size() == 1) {
                        /* Replace the alias of the expression by `$res`. */
                        selectClause->select.begin()->second.text = C.pool("$res");
                        /* Create a graph for the nested query. */
                        GraphBuilder builder;
                        builder(*select);
                        auto graph = builder.get();
                        auto q_name = query->alias();
                        auto &q = graph_->add_source(q_name, graph.release());
                        /* Create a join between the nested query and all tables in the clause. */
                        auto tables = get_tables(clause);
                        if (tables.empty()) {
                            /* This clause is a filter to all data sources. Since it contains the nested query we
                             * have to use a join to an arbitrary source. */
                            insist(not aliases_.empty(), "can not join nested query to any source");
                            /* TODO if q is correlated use the referenced table */
                            auto &p = *aliases_.begin();
                            auto J = graph_->joins_.emplace_back(new Join(cnf::CNF({clause}),{&q, p.second}));
                            q.add_join(J);
                            p.second->add_join(J);
                        } else {
                            /* This clause is a join condition. */
                            Join::sources_t sources{&q};
                            for (auto t : tables)
                                sources.emplace_back(aliases_.at(t));
                            auto J = graph_->joins_.emplace_back(new Join(cnf::CNF({clause}), sources));
                            for (auto ds : J->sources())
                                ds->add_join(J);
                        }
                        aliases_.emplace(q_name, &q);
                        /* Decorrelate the nested query iff it is correlated. */
                        if (q.is_correlated())
                            decorrelate(q);
                    } else {
                        unreachable("invalid variant");
                    }
                } else {
                    unreachable("invalid variant");
                }
            }
        }

        if (s.group_by)
            has_nested_query = false; // because additional attributes of nested queries are eliminated by the GROUP BY

        /* Implement HAVING as a regular selection filter on a sub query. */
        cnf::CNF cnf_having;
        if (s.having) {
            auto H = as<HavingClause>(s.having);
            cnf_having = cnf::to_CNF(*H->having);
            auto sub_graph = graph_.release();
            graph_ = std::make_unique<QueryGraph>();
            auto &sub = graph_->add_source(C.pool("HAVING"), sub_graph);
            aliases_.emplace("HAVING", &sub);
            /* Reset former computation of projection and move it after the HAVING. */
            graph_->projections_ = std::move(sub.query_graph()->projections_);
            sub.query_graph()->projections_.clear();
            graph_->projection_is_anti_ = sub.query_graph()->projection_is_anti_;
            sub.query_graph()->projection_is_anti_ = false;
            graph_->expanded_projections_ = sub.query_graph()->expanded_projections_;
            sub.query_graph()->expanded_projections_ = nullptr;
            /* Update `decorrelated_`-flag. */
            if (sub.is_correlated())
                sub.decorrelated_ = false;
        }

        /* Dissect CNF into filters and nested queries. Decorrelate if possible. */
        /* TODO iterate over clauses in the order filters/nested queries (equi) < nested queries (non-equi) */
        for (auto &clause : cnf_having) {
            auto queries = get_queries(clause);
            if (queries.empty()) {
                aliases_.at("HAVING")->update_filter(graph_->info_->compute(cnf::CNF({clause})));
            } else {
                has_nested_query = true;
                insist(queries.size() == 1, "Multiple nested queries in one clause are not yet supported.");
                auto query = as<const QueryExpr>(*queries.begin());
                if (auto select = cast<SelectStmt>(query->query)) {
                    auto selectClause = as<SelectClause>(select->select);
                    insist(not selectClause->select_all, "* can not yet be used in nested queries.");
                    if (selectClause->select.size() == 1) {
                        /* Replace the alias of the expression by `$res`. */
                        selectClause->select.begin()->second.text = C.pool("$res");
                        /* Create a graph for the nested query. */
                        GraphBuilder builder;
                        builder(*select);
                        auto graph = builder.get();
                        auto q_name = query->alias();
                        auto &q = graph_->add_source(q_name, graph.release());
                        aliases_.emplace(q_name, &q);
                        /* Create a join between the nested query and the HAVING. */
                        auto J = graph_->joins_.emplace_back(new Join(cnf::CNF({clause}), {&q, aliases_.at("HAVING")}));
                        for (auto ds : J->sources())
                            ds->add_join(J);
                        /* Decorrelate the nested query iff it is correlated. */
                        if (q.is_correlated())
                            decorrelate(q);
                    } else {
                        unreachable("invalid variant");
                    }
                } else {
                    unreachable("invalid variant");
                }
            }
        }

        /* Implement nested queries in SELECT after grouping so use a subquery (similar to HAVING) if not already done. */
        if (not nested_queries_select.empty() and not s.having
            and (not graph_->group_by_.empty() or not graph_->aggregates_.empty())) {
            auto sub_graph = graph_.release();
            graph_ = std::make_unique<QueryGraph>();
            auto &sub = graph_->add_source(C.pool("SELECT"), sub_graph);
            aliases_.emplace("SELECT", &sub);
            /* Reset former computation of projection and move it after the SELECT. */
            graph_->projections_ = std::move(sub.query_graph()->projections_);
            sub.query_graph()->projections_.clear();
            graph_->projection_is_anti_ = sub.query_graph()->projection_is_anti_;
            sub.query_graph()->projection_is_anti_ = false;
            graph_->expanded_projections_ = sub.query_graph()->expanded_projections_;
            sub.query_graph()->expanded_projections_ = nullptr;
            /* Update `decorrelated_`-flag. */
            if (sub.is_correlated())
                sub.decorrelated_ = false;
        }
        /* Add nested queries in SELECT. */
        /* TODO iterate over expressions in the order nested queries (equi) < nested queries (non-equi) */
        for (auto &p : nested_queries_select) {
            auto query = p.first;
            if (auto select = cast<SelectStmt>(query->query)) {
                auto selectClause = as<SelectClause>(select->select);
                insist(not selectClause->select_all, "* can not yet be used in nested queries.");
                if (selectClause->select.size() == 1) {
                    /* Replace the alias of the expression by `$res`. */
                    selectClause->select.begin()->second.text = C.pool("$res");
                    /* Create a graph for the nested query. */
                    GraphBuilder builder;
                    builder(*select);
                    auto graph = builder.get();
                    auto q_name = query->alias();
                    auto &q = graph_->add_source(q_name, graph.release());
                    /* Create a cartesian product between the nested query and an arbitrary source iff one exists. */
                    if (not aliases_.empty()) {
                        /* TODO if q is correlated use the referenced table */
                        DataSource *source;
                        try {
                            source = aliases_.at("SELECT");
                        } catch (std::out_of_range) {
                            try {
                                source = aliases_.at("HAVING");
                            } catch (std::out_of_range) {
                                source = aliases_.begin()->second;
                            }
                        }
                        auto J = graph_->joins_.emplace_back(new Join(cnf::CNF(), {&q, source}));
                        q.add_join(J);
                        source->add_join(J);
                    }
                    aliases_.emplace(q_name, &q);
                    if (not graph_->projection_is_anti_ or has_nested_query)
                        graph_->projections_.emplace_back(query, p.second);
                    /* Decorrelate the nested query iff it is correlated. */
                    if (q.is_correlated())
                        decorrelate(q);
                } else {
                    unreachable("invalid variant");
                }
            } else {
                unreachable("invalid variant");
            }
        }

        if (has_nested_query and graph_->projection_is_anti_) {
            /* Replace * by all attributes of original data sources. */
            graph_->projection_is_anti_ = false;
            for (auto e : S->expansion)
                graph_->projections_.emplace_back(e, nullptr);
        }

        /* Add order by. */
        if (s.order_by) {
            auto O = as<OrderByClause>(s.order_by);
            for (auto o : O->order_by)
                graph_->order_by_.emplace_back(o.first, o.second);
        }

        /* Add limit. */
        if (s.limit) {
            auto L = as<LimitClause>(s.limit);
            errno = 0;
            graph_->limit_.limit = strtoull(L->limit.text, nullptr, 10);
            insist(errno == 0);
            if (L->offset) {
                graph_->limit_.offset = strtoull(L->offset.text, nullptr, 10);
                insist(errno == 0);
            }
        }
    }

    void operator()(Const<InsertStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<UpdateStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<DeleteStmt>&) {
        /* TODO: implement */
    }

    void operator()(Const<DSVImportStmt>&) {
        /* TODO: implement */
    }

    private:
    void decorrelate(Query &query) {
        Decorrelation dec(query, graph_.get());
        if(not dec.decorrelate())
            query.decorrelated_ = false; // query is not completely decorrelated
    }
};

/*======================================================================================================================
 * QueryGraph
 *====================================================================================================================*/

QueryGraph::QueryGraph()
    : info_(new GetCorrelationInfo())
{ }

QueryGraph::~QueryGraph()
{
    for (auto src : sources_)
        delete src;
    for (auto j : joins_)
        delete j;
    delete info_;
}

std::unique_ptr<QueryGraph> QueryGraph::Build(const Stmt &stmt)
{
    GraphBuilder builder;
    builder(stmt);
    return builder.get();
}

bool QueryGraph::is_correlated() const {
    if (not info_->empty()) return true;
    for (auto ds : sources_) {
        if (ds->is_correlated())
            return true;
    }
    return false;
}

void QueryGraph::dot(std::ostream &out) const
{
    out << "graph query_graph\n{\n"
        << "    forcelabels=true;\n"
        << "    overlap=false;\n"
        << "    labeljust=\"l\";\n"
        << "    graph [compound=true];\n"
        << "    graph [fontname = \"DejaVu Sans\"];\n"
        << "    node [fontname = \"DejaVu Sans\"];\n"
        << "    edge [fontname = \"DejaVu Sans\"];\n";

    dot_recursive(out);

    out << '}' << std::endl;
}

void QueryGraph::dot_recursive(std::ostream &out) const
{
#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X << std::dec) // convert virtual address to identifier
    for (auto ds : sources()) {
        if (auto q = cast<Query>(ds)) {
            q->query_graph()->dot_recursive(out);
        }
    }

    out << "\n  subgraph cluster_" << this << " {\n";

    for (auto ds : sources_) {
        out << "    " << id(*ds) << " [label=<<B>" << ds->alias() << "</B>";
        if (ds->filter().size())
            out << "<BR/><FONT COLOR=\"0.0 0.0 0.25\" POINT-SIZE=\"10\">"
                << html_escape(to_string(ds->filter()))
                << "</FONT>";
        out << ">,style=filled,fillcolor=\"0.0 0.0 0.8\"];\n";
        if (auto q = cast<Query>(ds))
            out << "  " << id(*ds) << " -- \"cluster_" << q->query_graph() << "\";\n";
    }

    for (auto j : joins_) {
        out << "    " << id(*j) << " [label=<" << html_escape(to_string(j->condition())) << ">,style=filled,fillcolor=\"0.0 0.0 0.95\"];\n";
        for (auto ds : j->sources())
            out << "    " << id(*j) << " -- " << id(*ds) << ";\n";
    }

    out << "    label=<"
        << "<TABLE BORDER=\"0\" CELLPADDING=\"0\" CELLSPACING=\"0\">\n";

    /* Limit */
    if (limit_.limit != 0 or limit_.offset != 0) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>λ</B><FONT POINT-SIZE=\"9\">"
            << limit_.limit << ", " << limit_.offset
            << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    /* Order by */
    if (order_by_.size()) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>ω</B><FONT POINT-SIZE=\"9\">";
        for (auto it = order_by_.begin(), end = order_by_.end(); it != end; ++it) {
            if (it != order_by_.begin()) out << ", ";
            out << html_escape(to_string(*it->first));
            out << ' ' << (it->second ? "ASC" : "DESC");
        }
        out << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    /* Projections */
    if (projection_is_anti() or not projections_.empty()) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>π</B><FONT POINT-SIZE=\"9\">";
        if (projection_is_anti())
            out << "*";
        for (auto it = projections_.begin(), end = projections_.end(); it != end; ++it) {
            if (it != projections_.begin() or projection_is_anti())
                out << ", ";
            out << html_escape(to_string(*it->first));
            if (it->second)
                out << " AS " << html_escape(it->second);
        }
        out << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    /* Group by and aggregates */
    if (not group_by_.empty() or not aggregates_.empty()) {
        out << "             <TR><TD ALIGN=\"LEFT\">\n"
            << "               <B>γ</B><FONT POINT-SIZE=\"9\">";
        for (auto it = group_by_.begin(), end = group_by_.end(); it != end; ++it) {
            if (it != group_by_.begin()) out << ", ";
            out << html_escape(to_string(**it));
        }
        if (not group_by_.empty() and not aggregates_.empty())
            out << ", ";
        for (auto it = aggregates_.begin(), end = aggregates_.end(); it != end; ++it) {
            if (it != aggregates_.begin()) out << ", ";
            out << html_escape(to_string(**it));
        }
        out << "</FONT>\n"
            << "             </TD></TR>\n";
    }

    out << "           </TABLE>\n"
        << "          >;\n"
        << "  }\n";
#undef id
#undef q
}

void QueryGraph::sql(std::ostream &out) const
{
    QueryGraph2SQL t(out);
    t(this);
    out << ';' << std::endl;
}

void QueryGraph::dump(std::ostream &out) const
{
    out << "QueryGraph {\n  sources:";

    /*----- Print sources. -------------------------------------------------------------------------------------------*/
    for (auto src : sources()) {
        out << "\n    ";
        if (auto q = cast<Query>(src)) {
            out << "(...)";
            if (q->alias())
                out << " AS " << q->alias();
        } else {
            auto bt = as<BaseTable>(src);
            out << bt->table().name;
            if (bt->alias() != bt->table().name)
                out << " AS " << src->alias();
        }
        if (not src->filter().empty())
            out << " WHERE " << src->filter();
    }

    /*----- Print joins. ---------------------------------------------------------------------------------------------*/
    if (joins().empty()) {
        out << "\n  no joins";
    } else {
        out << "\n  joins:";
        for (auto j : joins()) {
            out << "\n    {";
            auto &srcs = j->sources();
            for (auto it = srcs.begin(), end = srcs.end(); it != end; ++it) {
                if (it != srcs.begin()) out << ' ';
                out << (*it)->alias();
            }
            out << "}  " << j->condition();
        }
    }

    /*----- Print grouping and aggregation information.  -------------------------------------------------------------*/
    if (group_by().empty() and aggregates().empty()) {
        out << "\n  no grouping";
    } else {
        if (not group_by().empty()) {
            out << "\n  group by: ";
            for (auto it = group_by().begin(), end = group_by().end(); it != end; ++it) {
                if (it != group_by().begin())
                    out << ", ";
                out << **it;
            }
        }
        if (not aggregates().empty()) {
            out << "\n  aggregates: ";
            for (auto it = aggregates().begin(), end = aggregates().end(); it != end; ++it) {
                if (it != aggregates().begin())
                    out << ", ";
                out << **it;
            }
        }
    }

    /*----- Print ordering information. ------------------------------------------------------------------------------*/
    if (order_by().empty()) {
        out << "\n  no order";
    } else {
        out << "\n  order by: ";
        for (auto it = order_by().begin(), end = order_by().end(); it != end; ++it) {
            if (it != order_by().begin())
                out << ", ";
            out << *it->first << ' ' << (it->second ? "ASC" : "DESC");
        }
    }

    /*----- Print projections. ---------------------------------------------------------------------------------------*/
    out << "\n  projections: ";
    for (auto &p : projections()) {
        if (p.second) {
            out << "\n    AS " << p.second;
            ASTDumper P(out, 3);
            P(*p.first);
        } else {
            ASTDumper P(out, 2);
            P(*p.first);
        }
    }

    out << "\n}" << std::endl;
}
void QueryGraph::dump() const { dump(std::cerr); }

void AdjacencyMatrix::dump(std::ostream &out) const { out << *this << std::endl; }
void AdjacencyMatrix::dump() const { dump(std::cerr); }
