#include "util/DotTool.hpp"

#include "util/fn.hpp"
#include "util/macro.hpp"
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


using namespace db;


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

void DotTool::render_to_pdf(const char *path_to_pdf, const char *algo)
{
    /*----- Render the dot graph with graphviz. ----------------------------------------------------------------------*/
    auto dotstr = stream_.str();
    Agraph_t *G = notnull(sym_agmemread(dotstr.c_str()));
    sym_gvLayout(gvc, G, algo);
    sym_gvRenderFilename(gvc, G, "pdf", path_to_pdf);
    sym_gvFreeLayout(gvc, G);
    sym_agclose(G);
}

void DotTool::show(const char *name, bool interactive, const char *algo)
{
    std::ostringstream oss;
    oss << name << '_';
#if __linux || __APPLE__
    oss << getpid();
#endif
    if (libgraphviz) {
        oss << ".pdf";
        render_to_pdf(oss.str().c_str(), algo);
        if (interactive) {
#if __linux
            exec("/usr/bin/setsid", { "--fork", "xdg-open", oss.str().c_str() });
#elif __APPLE__
            exec("/usr/bin/open", { "-a", "Preview", oss.str().c_str() });
#endif
        } else {
            diag.out() << diag.NOTE << "Rendering to '" << oss.str() << "'.\n" << diag.RESET;
        }
    } else {
        oss << ".dot";
        std::ofstream out(oss.str());
        if (not out) {
            diag.err() << "Failed to generate '" << oss.str() << "'.\n";
            return;
        }
        out << stream_.rdbuf();
        out.flush();
        if (interactive) diag.out() << diag.NOTE << "Rendering to '" << oss.str() << "'.\n" << diag.RESET;
    }
}
