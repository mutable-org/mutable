#include <mutable/parse/AST.hpp>

#include "parse/ASTDot.hpp"
#include "parse/ASTDumper.hpp"
#include "parse/ASTPrinter.hpp"
#include <mutable/catalog/Schema.hpp>


using namespace m;


/*======================================================================================================================
 * Destructor
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
 * operator==()
 *====================================================================================================================*/

bool ErrorExpr::operator==(const Expr &o) const { return cast<const ErrorExpr>(&o) != nullptr; }

bool Designator::operator==(const Expr &o) const
{
    if (auto other = cast<const Designator>(&o)) {
        if (this->has_explicit_table_name() or other->has_explicit_table_name())
            return this->table_name.text == other->table_name.text and this->attr_name.text == other->attr_name.text;
        else
            return this->attr_name.text == other->attr_name.text;
    }
    return false;
}

bool Constant::operator==(const Expr &o) const
{
    if (auto other = cast<const Constant>(&o))
        return this->tok.text == other->tok.text;
    return false;
}

bool FnApplicationExpr::operator==(const Expr &o) const
{
    if (auto other = cast<const FnApplicationExpr>(&o)) {
        if (*this->fn != *other->fn) return false;
        if (this->args.size() != other->args.size()) return false;
        for (std::size_t i = 0, end = this->args.size(); i != end; ++i) {
            if (*this->args[i] != *other->args[i])
                return false;
        }
        return true;
    }
    return false;
}

bool UnaryExpr::operator==(const Expr &o) const
{
    if (auto other = cast<const UnaryExpr>(&o))
        return this->op().text == other->op().text and *this->expr == *other->expr;
    return false;
}

bool BinaryExpr::operator==(const Expr &o) const
{
    if (auto other = cast<const BinaryExpr>(&o))
        return this->op().text == other->op().text and *this->lhs == *other->lhs and *this->rhs == *other->rhs;
    return false;
}

bool QueryExpr::operator==(const Expr&) const { M_unreachable("not implemented"); }


/*======================================================================================================================
 * get_required()
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
    void operator()(Const<ErrorExpr>&) { M_unreachable("graph must not contain errors"); }

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
 * operator<<()
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
 * dot()
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
 * dump()
 *====================================================================================================================*/

M_LCOV_EXCL_START
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
M_LCOV_EXCL_STOP


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
    /* Correlation is only valid in a WHERE- or HAVING-clause */
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
 * accept()
 *====================================================================================================================*/

/*===== Expressions ==================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(ASTExprVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTExprVisitor &v) const { v(*this); }
M_AST_EXPR_LIST(ACCEPT)
#undef ACCEPT

/*===== Clauses ======================================================================================================*/

#define ACCEPT(CLASS) \
    void CLASS::accept(ASTClauseVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTClauseVisitor &v) const { v(*this); }
M_AST_CLAUSE_LIST(ACCEPT)
#undef ACCEPT

/*===== Constraints ==================================================================================================*/

#define ACCEPT_CONSTRAINT(CLASS) \
    void CLASS::accept(ASTConstraintVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTConstraintVisitor &v) const { v(*this); }
M_AST_CONSTRAINT_LIST(ACCEPT_CONSTRAINT)
#undef ACCEPT

/*===== Statements ===================================================================================================*/

#define ACCEPT_STMT(CLASS) \
    void CLASS::accept(ASTStmtVisitor &v)            { v(*this); } \
    void CLASS::accept(ConstASTStmtVisitor &v) const { v(*this); }
M_AST_STMT_LIST(ACCEPT_STMT)
#undef ACCEPT
