#pragma once

#include <cstdint>


namespace m {

/** Singleton class representing options provided as command line argument to the binaries.  Implements Scott Meyer's
 * singleton pattern.  */
struct Options
{
    /** Type of plan tables available for query optimization. */
    enum PlanTableType
    {
        PT_auto,
        PT_SmallOrDense,
        PT_LargeAndSparse,
    };

    /*----- Help -----------------------------------------------------------------------------------------------------*/
    bool show_help;
    bool show_version;
    bool list_stores;
    bool list_plan_enumerators;
    bool list_backends;
    bool list_cardinality_estimators;
    bool show_injected_cardinalities_example;

    /*----- Shell configuration --------------------------------------------------------------------------------------*/
    bool has_color;
    bool show_prompt;
    bool quiet;

    /*----- Additional outputs ---------------------------------------------------------------------------------------*/
    bool times;
    bool echo;
    bool ast;
    bool astdot;
    bool graph;
    bool graphdot;
    bool graph2sql;
    bool plan;
    bool plandot;

    const char *pddl;
    int pddl_actions;

    /** If `true`, do not pass the query to the backend for execution. */
    bool dryrun;

    /** If `true`, the results of queries are dropped and not passed back to the user. */
    bool benchmark;

    /** The type of plan table to use for query optimization. */
    PlanTableType plan_table_type = PT_auto;

    /*----- Database configuration. ----------------------------------------------------------------------------------*/
    const char *store;
    const char *plan_enumerator;
    const char *backend;
    const char *cardinality_estimator;
    const char *injected_cardinalities_file;
    const char *output_partial_plans_file;

    /** If `true`, run the procedure to train cost models for query building blocks at startup. */
    bool train_cost_models;

    ///> Specify the port for debugging via ChromeDevTools (CDT)
    int16_t cdt_port = -1;
    int wasm_optimization_level = 2;
    bool wasm_adaptive = false;

    /* AI planning configuration */
    const char *ai_state;
    const char *ai_heuristic;
    const char *ai_search;

    private:
    Options() = default;
    public:
    Options(const Options&) = delete;
    Options & operator=(const Options&) = delete;

    /** Return a reference to the single `Options` instance. */
    static Options & Get();
};

}
