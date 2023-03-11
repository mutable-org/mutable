#pragma once

#include <mutable/mutable-config.hpp>
#include <cstdint>


namespace m {

/** Singleton class representing options provided as command line argument to the binaries.  Implements Scott Meyer's
 * singleton pattern.  */
struct M_EXPORT Options
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
    bool list_data_layouts;
    bool list_cardinality_estimators;
    bool list_plan_enumerators;
    bool list_backends;
    bool list_cost_functions;

    /*----- Shell configuration --------------------------------------------------------------------------------------*/
    bool has_color;
    bool show_prompt;
    bool quiet;

    /*----- Additional outputs ---------------------------------------------------------------------------------------*/
    bool times;
    bool statistics;
    bool echo;
    bool ast;
    bool astdot;
    bool graph;
    bool graphdot;
    bool graph2sql;
    bool plan;
    bool plandot;
    bool physplan;
    bool physplandot;

    /** If `true`, do not pass the query to the backend for execution. */
    bool dryrun;

    /** If `true`, the results of queries are dropped and not passed back to the user. */
    bool benchmark;

    /** The type of plan table to use for query optimization. */
    PlanTableType plan_table_type = PT_auto;

    /*----- Database configuration. ----------------------------------------------------------------------------------*/
    const char *injected_cardinalities_file;
    const char *output_partial_plans_file;

    /** If `true`, run the procedure to train cost models for query building blocks at startup. */
    bool train_cost_models;

    /** A comma seperated list of libraries that are loaded dynamically. */
    const char *plugins;

    private:
    Options() = default;
    public:
    Options(const Options&) = delete;
    Options & operator=(const Options&) = delete;

    /** Return a reference to the single `Options` instance. */
    static Options & Get();
};

}
