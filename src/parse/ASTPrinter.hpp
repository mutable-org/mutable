#include "parse/ASTVisitor.hpp"


namespace db {

/** Pretty-prints the AST in SQL. */
struct ASTPrinter : ConstASTVisitor
{
    public:
    std::ostream &out; ///< the output stream to write to
    private:
    unsigned indent_; ///< the current level of indentation
    bool is_nested_ = false; ///< whether the statement is nested; determines if a final ';' must be printed

    public:
    ASTPrinter(std::ostream &out, unsigned indent = 0) : out(out), indent_(indent) { (void)(this->indent_); }

    using ConstASTVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS>&) override;
    DB_AST_LIST(DECLARE)
#undef DECLARE
};

}
