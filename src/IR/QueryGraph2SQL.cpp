#include "IR/QueryGraph2SQL.hpp"

#include "mutable/IR/QueryGraph.hpp"


using namespace m;

void QueryGraph2SQL::translate(const QueryGraph *graph)
{
    graph_ = graph;
    after_grouping_ = true;
    out_ << "SELECT ";
    if (graph_->projections().empty()) {
        if (graph_->grouping()) {
            /* Rename grouping keys and aggregates similar to mu*t*able, i.e. their textual representation is used as
             * new attribute name and no table name is specified. */
            bool first = true; // indicates whether the current element is the first one fulfilling the `std::find_if` condition
            for (auto e : graph_->group_by()) {
                auto pred = [=](const std::pair<const Expr*, const char*> &p) { return *p.first == *e; };
                if (std::find_if(graph_->projections().begin(), graph_->projections().end(), pred) == graph_->projections().end()) {
                    if (not first)
                        out_ << ", ";
                    insert_projection(e);
                    first = false;
                }
            }
            for (auto e : graph_->aggregates()) {
                auto pred = [=](const std::pair<const Expr*, const char*> &p) { return *p.first == *e; };
                if (std::find_if(graph_->projections().begin(), graph_->projections().end(), pred) == graph_->projections().end()) {
                    if (not first)
                        out_ << ", ";
                    insert_projection(e);
                    first = false;
                }
            }
        } else {
            out_ << '*';
        }
    } else {
        for (auto it = graph_->projections().begin(); it != graph_->projections().end(); ++it) {
            if (it != graph_->projections().begin())
                out_ << ", ";
            translate_projection(*it);
        }
    }

    after_grouping_ = false;
    if (not graph_->sources().empty()) {
        out_ << " FROM ";
        for (auto it = graph_->sources().begin(); it != graph_->sources().end(); ++it) {
            if (it != graph_->sources().begin())
                out_ << ", ";
            auto ds = *it;
            if (auto base = cast<BaseTable>(ds)) {
                auto name = base->table().name;
                out_ << name;
                if (name != ds->alias()) {
                    M_insist(ds->alias());
                    out_ << " AS " << ds->alias();
                }
            } else if (auto query = cast<Query>(ds)) {
                out_ << '(';
                QueryGraph2SQL trans(out_);
                trans.translate(query->query_graph());
                out_ << ") AS ";
                if (auto alias = ds->alias())
                    out_ << alias;
                else
                    out_ << make_unique_alias(); // query is anonymous -> alias is never used but required by SQL syntax
            } else {
                M_unreachable("invalid variant");
            }
        }
    }

    cnf::CNF where;
    for (auto ds : graph_->sources())
        where = where and ds->filter();
    for (auto j : graph_->joins())
        where = where and j->condition();
    if (not where.empty()) {
        out_ << " WHERE ";
        (*this)(where);
    }

    if (not graph_->group_by().empty()) {
        out_ << " GROUP BY ";
        for (auto it = graph_->group_by().begin(); it != graph_->group_by().end(); ++it) {
            if (it != graph_->group_by().begin())
                out_ << ", ";
            (*this)(**it);
        }
    }

    after_grouping_ = true;
    if (not graph_->order_by().empty()) {
        out_ << " ORDER BY ";
        for (auto it = graph_->order_by().begin(); it != graph_->order_by().end(); ++it) {
            if (it != graph_->order_by().begin())
                out_ << ", ";
            (*this)(*it->first);
            if (it->second)
                out_ << " ASC";
            else
                out_ << " DESC";
        }
    }

    if (auto limit = graph_->limit().limit, offset = graph_->limit().offset; limit != 0 or offset != 0 ) {
        out_ << " LIMIT " << limit;
        if (offset != 0)
            out_ << " OFFSET " << offset;
    }
}

void QueryGraph2SQL::insert_projection(const m::Expr *e)
{
    /* Translate the given `Expr` recursively. Set `after_grouping` because we want to insert a projection for the
     * output of the grouping operator. */
    std::ostringstream expr;
    QueryGraph2SQL expr_trans(expr, graph_, true);
    expr_trans(*e);
    /* Build an alias from the textual representation of the given `Expr` by replacing every occurrence of a special
     * character with `_`. */
    auto alias = to_string(*e);
    alias = replace_all(alias, ".", "_");
    alias = replace_all(alias, ",", "_");
    alias = replace_all(alias, " ", "_");
    alias = replace_all(alias, "(", "_");
    alias = replace_all(alias, ")", "_");
    /* Compare both translations and add an alias iff they differ. */
    if (streq(expr.str().c_str(), alias.c_str()))
        out_ << expr.str();
    else
        out_ << expr.str() << " AS " << alias;
}

void QueryGraph2SQL::translate_projection(const std::pair<const Expr*, const char*> &p)
{
    if (p.second) {
        /* With alias. Translate recursively and add the alias. */
        (*this)(*p.first);
        out_ << " AS ";
        std::string alias(p.second);
        alias = replace_all(alias, "$", "_"); // for `$res` in decorrelated queries
        out_ << alias;
    } else {
        /* Without alias. Insert the given `Expr` and add an alias iff needed. */
        insert_projection(p.first);
    }
}

bool QueryGraph2SQL::references_group_by(Designator::target_type t)
{
    if (std::holds_alternative<const Expr*>(t))
        return contains(graph_->group_by(), std::get<const Expr*>(t));
    else
        return false;
}

void QueryGraph2SQL::operator()(Const<ErrorExpr>&)
{
    M_unreachable("graph must not contain errors");
}

void QueryGraph2SQL::operator()(Const<Designator> &e)
{
    if (after_grouping_ and references_group_by(e.target())) {
        M_insist(not e.has_table_name());
        std::ostringstream expr;
        QueryGraph2SQL expr_trans(expr, graph_, false);
        expr_trans(*std::get<const Expr*>(e.target()));
        out_ << expr.str();
    } else {
        if (e.table_name)
            out_ << e.table_name.text << '.';
        std::string attr(e.attr_name.text);
        attr = replace_all(attr, ".", "_");
        attr = replace_all(attr, ",", "_");
        attr = replace_all(attr, " ", "");
        attr = replace_all(attr, "(", "_");
        attr = replace_all(attr, ")", "_");
        out_ << attr;
    }
}

void QueryGraph2SQL::operator()(Const<Constant> &e)
{
    out_ << e.tok.text;
}

void QueryGraph2SQL::operator()(Const<FnApplicationExpr> &e)
{
    if (after_grouping_) {
        (*this)(*e.fn);
        out_ << '(';
        for (auto it = e.args.cbegin(), end = e.args.cend(); it != end; ++it) {
            if (it != e.args.cbegin())
                out_ << ", ";
            (*this)(**it);
        }
        out_ << ')';
    } else {
        auto expr = to_string(e);
        expr = replace_all(expr, ".", "_");
        expr = replace_all(expr, ",", "_");
        expr = replace_all(expr, " ", "_");
        expr = replace_all(expr, "(", "_");
        expr = replace_all(expr, ")", "_");
        out_ << expr;
    }
}

void QueryGraph2SQL::operator()(Const<UnaryExpr> &e)
{
    out_ << '(' << e.op().text;
    if (e.op() == TK_Not) out_ << ' ';
    (*this)(*e.expr);
    out_ << ')';
}

void QueryGraph2SQL::operator()(Const<BinaryExpr> &e)
{
    out_ << '(';
    (*this)(*e.lhs);
    out_ << ' ' << e.op().text << ' ';
    (*this)(*e.rhs);
    out_ << ')';
}

void QueryGraph2SQL::operator()(Const<QueryExpr> &e)
{
    out_ << e.alias() << "._res";
}

void QueryGraph2SQL::operator()(const cnf::Predicate &pred)
{
    if (pred.negative())
        out_ << "NOT ";
    (*this)(*pred.expr());
}

void QueryGraph2SQL::operator()(const cnf::Clause &clause)
{
    for (auto it = clause.begin(); it != clause.end(); ++it) {
        if (it != clause.begin())
            out_ << " OR ";
        (*this)(*it);
    }
}

void QueryGraph2SQL::operator()(const cnf::CNF &cnf)
{
    if (cnf.empty())
        out_ << "TRUE";
    else if (cnf.size() == 1)
        (*this)(cnf[0]);
    else {
        for (auto it = cnf.begin(); it != cnf.end(); ++it) {
            if (it != cnf.begin())
                out_ << " AND ";
            out_ << '(';
            (*this)(*it);
            out_ << ')';
        }
    }
}
