#pragma once


/** Singleton class representing options provided as command line argument to the binaries. */
struct Options
{
    /* Help */
    bool show_help;
    bool list_stores;
    bool list_plan_enumerators;

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
    /** If `true`, the results of queries are dropped and not passed back to the user. */
    bool benchmark;

    /* Keyword arguments. */
    const char *store;
    const char *plan_enumerator;

    private:
    Options() = default;

    public:
    /** Return a reference to the single `Options` instance. */
    static Options & Get();
};
