#include "mutable/parse/AST.hpp"

#include "catalog/Schema.hpp"
#include "parse/ASTDot.hpp"
#include "parse/ASTDumper.hpp"
#include "parse/ASTPrinter.hpp"


using namespace m;


/*======================================================================================================================
 * Destructors
 *====================================================================================================================*/

/*===== Expr =========================================================================================================*/

FnApplicationExpr::~FnApplicationExpr()
{
    delete fn;
    for (auto arg : args)
        delete arg;
}

QueryExpr::~QueryExpr()
{
    delete query;
}

/*===== Clause =======================================================================================================*/

SelectClause::~SelectClause()
{
    for (auto s : select)
        delete s.first;
    for (auto e : expansion)
        delete e;
}

FromClause::~FromClause()
{
    for (auto &f : from) {
        if (Stmt **stmt = std::get_if<Stmt*>(&f.source))
            delete (*stmt);
    }
}

WhereClause::~WhereClause()
{
    delete where;
}

GroupByClause::~GroupByClause()
{
    for (auto e : group_by)
        delete e;
}

HavingClause::~HavingClause()
{
    delete having;
}

OrderByClause::~OrderByClause()
{
    for (auto o : order_by)
        delete o.first;
}

/*===== Stmt =========================================================================================================*/

SelectStmt::~SelectStmt()
{
    delete select;
    delete from;
    delete where;
    delete group_by;
    delete having;
    delete order_by;
    delete limit;
}

InsertStmt::~InsertStmt()
{
    for (tuple_t &v : tuples) {
        for (element_type &e : v)
            delete e.second;
    }
}

UpdateStmt::~UpdateStmt()
{
    for (auto &s : set)
        delete s.second;
    delete where;
}

DeleteStmt::~DeleteStmt()
{
    delete where;
}

/*======================================================================================================================
 * QueryExpr
 *====================================================================================================================*/

bool QueryExpr::is_constant() const
{
    auto stmt = as<const SelectStmt>(query);
    if (stmt->from) return false;
    auto select = as<const SelectClause>(stmt->select);
    for (const auto &s : select->select) {
        if (not s.first->is_constant())
            return false;
    }
    return true;
}

bool QueryExpr::is_correlated() const
{
    /* Correlation is only valid in the where- or having-clause */
    auto stmt = as<const SelectStmt>(query);
    if (stmt->where) {
        auto where = as<const WhereClause>(stmt->where);
        if (where->where->is_correlated()) return true;
    }
    if (stmt->having) {
        auto having = as<const HavingClause>(stmt->having);
        if (having->having->is_correlated()) return true;
    }
    return false;
}

/*======================================================================================================================
 * operator==
 *====================================================================================================================*/

bool ErrorExpr::operator==(const Expr &o) const { return cast<const ErrorExpr>(&o) != nullptr; }

bool Designator::operator==(const Expr &o) const
{
    auto other = cast<const Designator>(&o);
    if (not other) return false;
    if (this->has_explicit_table_name() or other->has_explicit_table_name())
        return this->table_name.text == other->table_name.text and
               this->attr_name.text == other->attr_name.text;
    else
        return this->attr_name.text == other->attr_name.text;
}

bool Constant::operator==(const Expr &o) const
{
    auto other = cast<const Constant>(&o);
    if (not other) return false;
    return this->tok.text == other->tok.text;
}

bool FnApplicationExpr::operator==(const Expr &o) const
{
    auto other = cast<const FnApplicationExpr>(&o);
    if (not other) return false;
    if (*this->fn != *other->fn) return false;
    if (this->args.size() != other->args.size()) return false;
    for (std::size_t i = 0, end = this->args.size(); i != end; ++i) {
        if (*this->args[i] != *other->args[i])
            return false;
    }
    return true;
}

bool UnaryExpr::operator==(const Expr &o) const
{
    auto other = cast<const UnaryExpr>(&o);
    if (not other) return false;
    if (this->op().text != other->op().text) return false;
    return *this->expr == *other->expr;
}

bool BinaryExpr::operator==(const Expr &o) const
{
    auto other = cast<const BinaryExpr>(&o);
    if (not other) return false;
    if (this->op().text != other->op().text) return false;
    return *this->lhs == *other->lhs and *this->rhs == *other->rhs;
}

bool QueryExpr::operator==(const Expr&) const { unreachable("not implemented"); }


/*======================================================================================================================
 * get_required
 *====================================================================================================================*/

struct GetRequired : ConstASTExprVisitor
{
    private:
    Schema schema;

    public:
    GetRequired() { }

    auto & get() { return schema; }

    /* Expr */
    void operator()(Const<Expr> &e) { e.accept(*this); }
    void operator()(Const<ErrorExpr>&) { unreachable("graph must not contain errors"); }

    void operator()(Const<Designator> &e) {
        Schema::Identifier id(e.table_name.text, e.attr_name.text);
        if (not schema.has(id)) // avoid duplicates
            schema.add(id, e.type());
    }

    using ConstASTExprVisitor::operator();

    void operator()(Const<Constant>&) { /* nothing to be done */ }

    void operator()(Const<FnApplicationExpr> &e) {
        //(*this)(*e.fn);
        for (auto arg : e.args)
            (*this)(*arg);
    }

    void operator()(Const<UnaryExpr> &e) { (*this)(*e.expr); }

    void operator()(Const<BinaryExpr> &e) { (*this)(*e.lhs); (*this)(*e.rhs); }

    void operator()(Const<QueryExpr>&) { /* TODO: implement */ }
};

Schema Expr::get_required() const
{
    GetRequired R;
    R(*this);
    return R.get();
}


/*======================================================================================================================
 * Stream printing
 *====================================================================================================================*/

std::ostream & m::operator<<(std::ostream &out, const Expr &e) {
    ASTPrinter p(out);
    p.expand_nested_queries(false);
    p(e);
    return out;
}

std::ostream & m::operator<<(std::ostream &out, const Clause &c) {
    ASTPrinter p(out);
    p.expand_nested_queries(false);
    p(c);
    return out;
}

std::ostream & m::operator<<(std::ostream &out, const Stmt &s) {
    ASTPrinter p(out);
    p.expand_nested_queries(false);
    p(s);
    return out;
}

/*======================================================================================================================
 * AST Dot
 *====================================================================================================================*/

void Expr::dot(std::ostream &out) const
{
    ASTDot dot(out);
    dot(*this);
}

void Clause::dot(std::ostream &out) const
{
    ASTDot dot(out);
    dot(*this);
}

void Stmt::dot(std::ostream &out) const
{
    ASTDot dot(out);
    dot(*this);
}

/*======================================================================================================================
 * AST Dump
 *====================================================================================================================*/

void Expr::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Expr::dump() const { dump(std::cerr); }

void Clause::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Clause::dump() const { dump(std::cerr); }

void Stmt::dump(std::ostream &out) const
{
    ASTDumper dumper(out);
    dumper(*this);
    out << std::endl;
}
void Stmt::dump() const { dump(std::cerr); }
