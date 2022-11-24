#include "IR/QueryGraph2SQL.hpp"

#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/QueryGraph.hpp>


using namespace m;

void QueryGraph2SQL::translate(const QueryGraph &graph)
{
    graph_ = &graph;
    after_grouping_ = true;
    out_ << "SELECT ";
    if (graph_->projections().empty()) {
        out_ << '*';
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
            auto &ds = *it;
            if (auto base = cast<BaseTable>(ds.get())) {
                auto name = base->table().name;
                out_ << name;
                if (name != ds->alias()) {
                    M_insist(ds->alias());
                    out_ << " AS " << ds->alias();
                }
            } else if (auto query = cast<Query>(ds.get())) {
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
    for (auto &ds : graph_->sources())
        where = where and ds->filter();
    for (auto &j : graph_->joins())
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
            (*this)(it->first);
            if (it->second)
                out_ << " AS " << it->second;
        }
    }

    after_grouping_ = true;
    if (not graph_->order_by().empty()) {
        out_ << " ORDER BY ";
        for (auto it = graph_->order_by().begin(); it != graph_->order_by().end(); ++it) {
            if (it != graph_->order_by().begin())
                out_ << ", ";
            (*this)(it->first);
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

void QueryGraph2SQL::insert_projection(const ast::Expr *e)
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

void QueryGraph2SQL::translate_projection(const std::pair<std::reference_wrapper<const ast::Expr>, const char*> p)
{
    if (p.second) {
        /* With alias. Translate recursively and add the alias. */
        (*this)(p.first.get());
        out_ << " AS ";
        std::string alias(p.second);
        alias = replace_all(alias, "$", "_"); // for `$res` in decorrelated queries
        out_ << alias;
    } else {
        /* Without alias. Insert the given `Expr` and add an alias iff needed. */
        insert_projection(&p.first.get());
    }
}

const char * QueryGraph2SQL::make_unique_alias()
{
    static uint64_t id(0);
    std::ostringstream oss;
    oss << "alias_" << id++;
    Catalog &C = Catalog::Get();
    return C.pool(oss.str().c_str());
}

void QueryGraph2SQL::operator()(Const<ast::ErrorExpr>&)
{
    M_unreachable("graph must not contain errors");
}

void QueryGraph2SQL::operator()(Const<ast::Designator> &e)
{
    if (e.table_name)
        out_ << e.table_name.text << '.';
    out_ << e.attr_name.text;
}

void QueryGraph2SQL::operator()(Const<ast::Constant> &e)
{
    out_ << e.tok.text;
}

void QueryGraph2SQL::operator()(Const<ast::FnApplicationExpr> &e)
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

void QueryGraph2SQL::operator()(Const<ast::UnaryExpr> &e)
{
    out_ << '(' << e.op().text;
    if (e.op() == TK_Not) out_ << ' ';
    (*this)(*e.expr);
    out_ << ')';
}

void QueryGraph2SQL::operator()(Const<ast::BinaryExpr> &e)
{
    out_ << '(';
    (*this)(*e.lhs);
    out_ << ' ' << e.op().text << ' ';
    (*this)(*e.rhs);
    out_ << ')';
}

void QueryGraph2SQL::operator()(Const<ast::QueryExpr> &e)
{
    out_ << e.alias() << "._res";
}

void QueryGraph2SQL::operator()(const cnf::Predicate &pred)
{
    if (pred.negative())
        out_ << "NOT ";
    (*this)(*pred);
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
