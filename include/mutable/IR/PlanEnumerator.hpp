#pragma once

#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/util/macro.hpp>
#include <cstdint>
#include <unordered_map>
#include <vector>


namespace m {

/* forward declarations */
struct PlanTable;

/** An interface for all plan enumerators. */
struct PlanEnumerator
{
    using Subproblem = QueryGraph::Subproblem;

    enum kind_t {
#define M_PLAN_ENUMERATOR(NAME, _) PE_ ## NAME,
#include <mutable/tables/PlanEnumerator.tbl>
#undef M_PLAN_ENUMERATOR
    };

    static const std::unordered_map<std::string, kind_t> STR_TO_KIND;

    virtual ~PlanEnumerator() { }

    /** Create a `PlanEnumerator` instance given the kind of plan enumerator. */
    static std::unique_ptr<PlanEnumerator> Create(kind_t kind);
    /** Create a `PlanEnumerator` instance given the name of a plan enumerator. */
    static std::unique_ptr<PlanEnumerator> Create(const char *kind) { return Create(STR_TO_KIND.at(kind)); }

#define M_PLAN_ENUMERATOR(NAME, _) \
    static std::unique_ptr<PlanEnumerator> Create ## NAME();
#include <mutable/tables/PlanEnumerator.tbl>
#undef M_PLAN_ENUMERATOR

    /** Enumerate subplans and fill plan table. */
    virtual void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const = 0;
};

}
