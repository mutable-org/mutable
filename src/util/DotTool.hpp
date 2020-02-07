#pragma once

#include "util/Diagnostic.hpp"
#include <sstream>


/** This class enables direct rendering of dot output (e.g. that of `ASTDot`).  It uses the graphviz library to render
 * the dot output directly to PDF. */
struct DotTool
{
    db::Diagnostic &diag;
    private:
    std::stringstream stream_;

    public:
    DotTool(db::Diagnostic &diag);

    template<typename T>
    friend DotTool & operator<<(DotTool &dot, const T &t) { dot.stream_ << t; return dot; }

    std::ostream & stream() { return stream_; }

    friend std::ostream & operator<<(std::ostream &out, const DotTool &dot) { return out << dot.stream_.rdbuf(); }

    void render_to_pdf(const char *path_to_pdf);
    void show(const char *name, bool interactive);
};
