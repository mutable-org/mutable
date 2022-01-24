#pragma once

#include <functional>
#include <iostream>
#include <mutable/util/ADT.hpp>
#include <vector>


namespace m {

/* Forward declarations. */
struct QueryGraph;

/** A *partial plan* is a set of (potentially incomplete) pairwise disjoint plans.  Each plan is uniquely represented by
 * its root, encoded as a `Subproblem`.  We say that a partial plan is a *complete partial plan* if each relations of a
 * query occurs exactly once as leaf in the partial plan. */
struct PartialPlanGenerator
{
    using Subproblem = SmallBitset;
    using partial_plan_type = std::vector<Subproblem>;
    using callback_type = std::function<void(const partial_plan_type&)>;

    /** Given a `PlanTable` with a final plan, enumerate all *complete partial plans* of this final plan and invoke
     * `callback` for each such complete partial plan. */
    template<typename PlanTable>
    void for_each_complete_partial_plan(const PlanTable &PT, callback_type callback);

    template<typename PlanTable>
    void write_partial_plans_JSON(std::ostream &out, const QueryGraph &G, const PlanTable &PT,
                                  std::function<void(callback_type)> for_each_partial_plan);
};

}
