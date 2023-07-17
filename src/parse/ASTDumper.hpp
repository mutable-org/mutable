#include <mutable/parse/AST.hpp>

#include <mutable/util/macro.hpp>


namespace m {

namespace ast {

/** Dumps a textual representation of the AST to an output stream. */
struct ASTDumper : ConstASTVisitor
{
    std::ostream &out; ///< the output stream to write to
    private:
    int indent_; ///< the current level of indentation

    public:
    ASTDumper(std::ostream &out, int indent = 0) : out(out), indent_(indent) { }

    using ConstASTVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS>&) override;
    M_AST_LIST(DECLARE)
#undef DECLARE

    private:
    /** Start a new line with proper indentation. */
    std::ostream & indent() const {
        M_insist(indent_ >= 0, "Indent must not be negative!  Missing increment or superfluous decrement?");
        if (indent_)
            out << '\n' << std::string(2 * indent_ - 2, ' ') << "` ";
        return out;
    }

    private:
    void print_type(const Expr &e) const;
};

}

}
