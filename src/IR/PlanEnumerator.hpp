#pragma once

#include "catalog/CostFunction.hpp"
#include "IR/QueryGraph.hpp"
#include "util/macro.hpp"
#include "util/macro.hpp"
#include <cstdint>
#include <unordered_map>
#include <vector>


namespace db {

struct PlanTable;

/** An interface for all plan enumerators. */
struct PlanEnumerator
{
    using Subproblem = QueryGraph::Subproblem;

    enum kind_t {
#define DB_PLAN_ENUMERATOR(NAME, _) PE_ ## NAME,
#include "tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR
    };

    static const std::unordered_map<std::string, kind_t> STR_TO_KIND;

    virtual ~PlanEnumerator() { }

    /** Create a `PlanEnumerator` instance given the kind of plan enumerator. */
    static std::unique_ptr<PlanEnumerator> Create(kind_t kind);
    /** Create a `PlanEnumerator` instance given the name of a plan enumerator. */
    static std::unique_ptr<PlanEnumerator> Create(const char *kind) { return Create(STR_TO_KIND.at(kind)); }

#define DB_PLAN_ENUMERATOR(NAME, _) \
    static std::unique_ptr<PlanEnumerator> Create ## NAME();
#include "tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR

    /** Enumerate subplans and fill plan table. */
    virtual void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const = 0;
};

}
