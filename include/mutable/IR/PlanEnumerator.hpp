#pragma once

#include <cstdint>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/macro.hpp>
#include <unordered_map>
#include <vector>


namespace m {

struct enumerate_tag : const_virtual_crtp_helper<enumerate_tag>::
    returns<void>::
    crtp_args<PlanTableSmallOrDense&, PlanTableLargeAndSparse&>::
    args<const QueryGraph&, const CostFunction&> { };

/** An interface for all plan enumerators. */
struct PlanEnumerator : enumerate_tag::base_type
{
    using Subproblem = QueryGraph::Subproblem;
    using enumerate_tag::base_type::operator();

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
    template<typename PlanTable>
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const {
        operator()(enumerate_tag{}, PT, G, CF);
    }
};

template<typename Actual>
struct PlanEnumeratorCRTP : PlanEnumerator
                          , enumerate_tag::derived_type<Actual>
{
    using PlanEnumerator::operator();
};

}
