#pragma once

#include "util/Diagnostic.hpp"
#include <sstream>


/** This class enables direct rendering of dot output (e.g. that of `ASTDot`).  It uses the graphviz library to render
 * the dot output directly to PDF. */
struct DotTool
{
    static constexpr const char *DEFAULT_LAYOUT_ALGORITHM = "dot";

    db::Diagnostic &diag;
    private:
    std::stringstream stream_;

    public:
    DotTool(db::Diagnostic &diag);

    template<typename T>
    friend DotTool & operator<<(DotTool &dot, const T &t) { dot.stream_ << t; return dot; }

    std::ostream & stream() { return stream_; }

    friend std::ostream & operator<<(std::ostream &out, const DotTool &dot) { return out << dot.stream_.rdbuf(); }

    /** Render the graph to the PDF file `path_to_pdf` using the given layouting `algo`rithm.
     * \returns `0` (zero) if rendering to PDF succeeded, non-zero otherwise
     */
    int render_to_pdf(const char *path_to_pdf, const char *algo = DEFAULT_LAYOUT_ALGORITHM);

    /** Present the graph to the user.  Automatically figures out the best way to do so.
     * \param name  the name of the graph, used in the filename
     * \param       interactive whether the program runs interactively
     * \param algo  the layouting algorithm to use
     */
    void show(const char *name, bool interactive, const char *algo = DEFAULT_LAYOUT_ALGORITHM);
};
