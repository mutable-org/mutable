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

    /* Statements */
    void operator()(Const<ErrorStmt> &s);
    void operator()(Const<CreateTableStmt> &s);
    void operator()(Const<SelectStmt> &s);
    void operator()(Const<InsertStmt> &s);
    void operator()(Const<UpdateStmt> &s);
    void operator()(Const<DeleteStmt> &s);

    private:
    std::ostream & indent() const {
        insist(indent_ >= 0, "Indent must not be negative!  Missing increment or superfluous decrement?");
        if (indent_)
            out << std::string(2 * indent_ - 2, ' ') << "` ";
        return out;
    }
};

}
