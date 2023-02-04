#include <mutable/parse/AST.hpp>


namespace m {

namespace ast {

/** Pretty-prints the AST in SQL. */
struct M_EXPORT ASTPrinter : ConstASTVisitor
{
    public:
    std::ostream &out; ///< the output stream to write to
    private:
    unsigned indent_; ///< the current level of indentation
    bool is_nested_ = false; ///< whether the statement is nested; determines whether a final ';' must be printed
    bool expand_nested_queries_ = true; ///< determines whether nested queries are printed

    public:
    ASTPrinter(std::ostream &out, unsigned indent = 0) : out(out), indent_(indent) { (void)(this->indent_); }

    bool expand_nested_queries() { return expand_nested_queries_; }
    void expand_nested_queries(bool expand) { expand_nested_queries_ = expand; }

    using ConstASTVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS>&) override;
    M_AST_LIST(DECLARE)
#undef DECLARE
};

}

}
