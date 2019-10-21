#include "parse/ASTVisitor.hpp"

#include "util/macro.hpp"


namespace db {

struct ASTDumper : ConstASTVisitor
{
    using ConstASTVisitor::operator();

    public:
    std::ostream &out;
    private:
    int indent_;

    public:
    ASTDumper(std::ostream &out, int indent = 0) : out(out), indent_(indent) { }

    /* Expressions */
    void operator()(Const<ErrorExpr> &e);
    void operator()(Const<Designator> &e);
    void operator()(Const<Constant> &e);
    void operator()(Const<FnApplicationExpr> &e);
    void operator()(Const<UnaryExpr> &e);
    void operator()(Const<BinaryExpr> &e);

    /* Clauses */
    void operator()(Const<ErrorClause> &c);
    void operator()(Const<SelectClause> &c);
    void operator()(Const<FromClause> &c);
    void operator()(Const<WhereClause> &c);
    void operator()(Const<GroupByClause> &c);
    void operator()(Const<HavingClause> &c);
    void operator()(Const<OrderByClause> &c);
    void operator()(Const<LimitClause> &c);

    /* Statements */
    void operator()(Const<ErrorStmt> &s);
    void operator()(Const<EmptyStmt> &s);
    void operator()(Const<CreateDatabaseStmt> &s);
    void operator()(Const<UseDatabaseStmt> &s);
    void operator()(Const<CreateTableStmt> &s);
    void operator()(Const<SelectStmt> &s);
    void operator()(Const<InsertStmt> &s);
    void operator()(Const<UpdateStmt> &s);
    void operator()(Const<DeleteStmt> &s);
    void operator()(Const<DSVImportStmt> &s);

    private:
    std::ostream & indent() const {
        insist(indent_ >= 0, "Indent must not be negative!  Missing increment or superfluous decrement?");
        if (indent_)
            out << '\n' << std::string(2 * indent_ - 2, ' ') << "` ";
        return out;
    }
};

}
