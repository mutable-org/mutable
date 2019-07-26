#include "parse/ASTVisitor.hpp"


namespace db {

struct ASTPrinter : ConstASTVisitor
{
    using ConstASTVisitor::operator();

    public:
    std::ostream &out;
    private:
    unsigned indent;
    bool is_nested = false; // is the statement nested?

    public:
    ASTPrinter(std::ostream &out, unsigned indent = 0) : out(out), indent(indent) { (void)(this->indent); }

    /* Expressions */
    virtual void operator()(Const<ErrorExpr> &e);
    virtual void operator()(Const<Designator> &e);
    virtual void operator()(Const<Constant> &e);
    virtual void operator()(Const<FnApplicationExpr> &e);
    virtual void operator()(Const<UnaryExpr> &e);
    virtual void operator()(Const<BinaryExpr> &e);

    /* Clauses */
    virtual void operator()(Const<ErrorClause> &c);
    virtual void operator()(Const<SelectClause> &c);
    virtual void operator()(Const<FromClause> &c);
    virtual void operator()(Const<WhereClause> &c);
    virtual void operator()(Const<GroupByClause> &c);
    virtual void operator()(Const<HavingClause> &c);
    virtual void operator()(Const<OrderByClause> &c);
    virtual void operator()(Const<LimitClause> &c);

    /* Statements */
    virtual void operator()(Const<ErrorStmt> &s);
    virtual void operator()(Const<EmptyStmt> &s);
    virtual void operator()(Const<CreateDatabaseStmt> &s);
    virtual void operator()(Const<UseDatabaseStmt> &s);
    virtual void operator()(Const<CreateTableStmt> &s);
    virtual void operator()(Const<SelectStmt> &s);
    virtual void operator()(Const<InsertStmt> &s);
    virtual void operator()(Const<UpdateStmt> &s);
    virtual void operator()(Const<DeleteStmt> &s);
};

}
