#pragma once


/** Singleton class representing options provided as command line argument to the binaries. */
struct Options
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

    private:
    Options() = default;

    static Options the_options_;

    public:
    /** Return a reference to the single `Options` instance. */
    static Options & Get() { return the_options_; }
};
