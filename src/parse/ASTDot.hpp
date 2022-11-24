#pragma once

#include <iostream>
#include <mutable/parse/AST.hpp>


namespace m {

namespace ast {

/** Implements printing the AST in dot language. */
struct ASTDot : ConstASTVisitor
{
    static constexpr const char * const GRAPH_TYPE = "graph";
    static constexpr const char * const EDGE = " -- ";

    std::ostream &out; ///< the output stream to print to

    private:
    int indent_; ///< the current indentation for pretty printing

    public:
    /**
     * @param out the output stream to print to
     * @param indent the initial indentation
     */
    ASTDot(std::ostream &out, int indent = 0);
    ~ASTDot();

    using ConstASTVisitor::operator();
#define DECLARE(CLASS) void operator()(Const<CLASS>&) override;
    M_AST_LIST(DECLARE)
#undef DECLARE

    private:
    /** Start a new line with proper indentation. */
    std::ostream & indent() const {
        M_insist(indent_ >= 0, "Indent must not be negative!  Missing increment or superfluous decrement?");
        if (indent_)
            out << '\n' << std::string(2 * indent_, ' ');
        return out;
    }

    /** Emit the given clause `c` in a dot cluster.
     *
     * @param c the clause to emit
     * @param name the internal name of the clause (must contain no white spaces)
     * @param label the human-readable name of this clause (may contain white spaces)
     * @param color the color to use for highlighting this cluster
     */
    void cluster(Const<Clause> &c, const char *name, const char *label, const char *color);
};

}

}
