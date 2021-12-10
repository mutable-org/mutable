#include <mutable/util/DotTool.hpp>

#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <cstdio>
#include <fstream>
#include <graphviz/gvc.h>
#include <iostream>
#include <sstream>

#if __linux
#include <dlfcn.h>
#include <unistd.h>
#elif __APPLE__
#include <dlfcn.h>
#include <unistd.h>
#endif


using namespace m;


#define SYMBOLS(X) \
    X(agclose) \
    X(agmemread) \
    X(gvContext) \
    X(gvFreeContext) \
    X(gvFreeLayout) \
    X(gvLayout) \
    X(gvRenderFilename)

#define DECLSYM(SYM) static decltype(SYM) *sym_##SYM;
#define LOADSYM(SYM) { \
    sym_##SYM = (decltype(SYM)*)(dlsym(libgraphviz, #SYM)); \
}
#if __linux
static constexpr const char * LIB_GRAPHVIZ = "libgvc.so";
#elif __APPLE__
static constexpr const char * LIB_GRAPHVIZ = "libgvc.dylib";
#endif
static void *libgraphviz;
static GVC_t *gvc;
SYMBOLS(DECLSYM);

DotTool::DotTool(Diagnostic &diag)
    : diag(diag)
{
    /*----- Test whether the graphviz library is available. ----------------------------------------------------------*/
#if __linux || __APPLE__
    libgraphviz = dlopen(LIB_GRAPHVIZ, RTLD_LAZY|RTLD_NOLOAD);
    if (libgraphviz == nullptr) { // shared object not yet present; must load
        libgraphviz = dlopen(LIB_GRAPHVIZ, RTLD_LAZY|RTLD_NODELETE); // load shared object

        if (libgraphviz) {
            /* Load the required symbols from the shared object. */
            SYMBOLS(LOADSYM);
            gvc = sym_gvContext();
        }
    }
#endif
}

int DotTool::render_to_pdf(const char *path_to_pdf, const char *algo)
{
    /*----- Render the dot graph with graphviz. ----------------------------------------------------------------------*/
    auto dotstr = stream_.str();
    Agraph_t *G = M_notnull(sym_agmemread(dotstr.c_str()));
    sym_gvLayout(gvc, G, algo);
    auto ret = sym_gvRenderFilename(gvc, G, "pdf", path_to_pdf);
    sym_gvFreeLayout(gvc, G);
    sym_agclose(G);
    return ret;
}

void DotTool::show(const char *name, bool interactive, const char *algo)
{
    /* Construct filename. */
    std::ostringstream oss;
    oss << name << '_';
#if __linux || __APPLE__
    oss << getpid();
#endif

    /* Try to render a PDF document. */
    if (libgraphviz) {
        const std::string filename_pdf = oss.str() + ".pdf";
        if (render_to_pdf(filename_pdf.c_str(), algo))
            goto show_dot; // fall back to DOT

        if (interactive) {
#if __linux
            exec("/usr/bin/setsid", { "--fork", "xdg-open", filename_pdf.c_str() });
#elif __APPLE__
            exec("/usr/bin/open", { "-a", "Preview", filename_pdf.c_str() });
#endif
        } else {
            diag.out() << diag.NOTE << "Rendering to '" << filename_pdf << "'.\n" << diag.RESET;
        }
        return;
    }

show_dot:
    /* Fallback: emit graph as a DOT file. */
    const std::string filename_dot = oss.str() + ".dot";
    std::ofstream out(filename_dot);
    if (not out) {
        diag.err() << "Failed to generate '" << filename_dot << "'.\n";
        return;
    }
    out << stream_.rdbuf();
    out.flush();
    diag.out() << diag.NOTE << "Rendering to '" << filename_dot << "'.\n" << diag.RESET;
}
