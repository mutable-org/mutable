#include "parse/ASTVisitor.hpp"


namespace db {

struct ASTPrinter : ConstASTVisitor
{
    using ConstASTVisitor::operator();

    public:
    std::ostream &out;
    private:
    unsigned indent;

    public:
    ASTPrinter(std::ostream &out, unsigned indent = 0) : out(out), indent(indent) { (void)(this->indent); }

    /* Expressions */
    virtual void operator()(Const<ErrorExpr> &e);
    virtual void operator()(Const<Designator> &e);
    virtual void operator()(Const<Constant> &e);
    virtual void operator()(Const<FnApplicationExpr> &e);
    virtual void operator()(Const<UnaryExpr> &e);
    virtual void operator()(Const<BinaryExpr> &e);

    /* Statements */
    virtual void operator()(Const<ErrorStmt> &s);
    virtual void operator()(Const<CreateDatabaseStmt> &s);
    virtual void operator()(Const<UseDatabaseStmt> &s);
    virtual void operator()(Const<CreateTableStmt> &s);
    virtual void operator()(Const<SelectStmt> &s);
    virtual void operator()(Const<InsertStmt> &s);
    virtual void operator()(Const<UpdateStmt> &s);
    virtual void operator()(Const<DeleteStmt> &s);
};

}
