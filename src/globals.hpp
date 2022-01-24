#pragma once

#include <cstdint>

namespace m {

/** Singleton class representing options provided as command line argument to the binaries. */
struct Options
{
    enum PlanTableType
    {
        PT_auto,
        PT_SmallOrDense,
        PT_LargeAndSparse,
    };

    /* Help */
    bool show_help;
    bool show_version;
    bool list_stores;
    bool list_plan_enumerators;
    bool list_backends;
    bool list_cardinality_estimators;
    bool show_injected_cardinalities_example;

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
    bool graph2sql;
    bool plan;
    bool plandot;
    bool dryrun;
    bool wasm;

    /** If `true`, the results of queries are dropped and not passed back to the user. */
    bool benchmark;

    /* What plan table type to use. */
    PlanTableType plan_table_type;

    /* Keyword arguments. */
    const char *store;
    const char *plan_enumerator;
    const char *backend;
    const char *cardinality_estimator;
    const char *injected_cardinalities_file;
    const char *output_partial_plans_file;
    const char *pddl;
    int pddl_actions;

    bool train_cost_models;

#if M_WITH_V8
    /* Specify the port for debugging via ChromeDevTools (CDT). */
    int16_t cdt_port = -1;
    int wasm_optimization_level = 2;
    bool wasm_adaptive = false;
#endif

    /* AI planning configuration */
    const char *ai_state;
    const char *ai_heuristic;
    const char *ai_search;

    private:
    Options() = default;

    public:
    /** Return a reference to the single `Options` instance. */
    static Options & Get();
};

}
