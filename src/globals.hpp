#pragma once


/** The `options_t` represents options provided as command line argument to the shell. */
struct options_t
{
    /* Help */
    bool show_help;

    /* Shell configuration */
    bool has_color;
    bool show_prompt;
    bool quiet;

    /* Additional outputs */
    bool times;
    bool echo;
    bool ast;
    bool astdot;
    bool graph;
    bool graphdot;
    bool plan;
    bool plandot;
    bool dryrun;
    bool wasm;
};

/** Returns a reference to the global `options_t` instance. */
options_t & get_options();
