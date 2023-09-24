#pragma once

#include <cstdint>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/mutable-config.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/macro.hpp>
#include <unordered_map>


namespace m {

struct enumerate_tag : const_virtual_crtp_helper<enumerate_tag>::
    returns<void>::
    crtp_args<PlanTableSmallOrDense&, PlanTableLargeAndSparse&>::
    args<const QueryGraph&, const CostFunction&> { };

/** An interface for all plan enumerators. */
struct M_EXPORT PlanEnumerator : enumerate_tag::base_type
{
    using Subproblem = QueryGraph::Subproblem;
    using enumerate_tag::base_type::operator();

    virtual ~PlanEnumerator() { }

    /** Enumerate subplans and fill plan table. */
    template<typename PlanTable>
    void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const {
        operator()(enumerate_tag{}, PT, G, CF);
    }
};

template<typename Actual>
struct M_EXPORT PlanEnumeratorCRTP : PlanEnumerator
                                   , enumerate_tag::derived_type<Actual>
{
    using PlanEnumerator::operator();
};

/** Computes the join order using connected subgraph complement pairs (CCP). */
struct M_EXPORT DPccp final : PlanEnumeratorCRTP<DPccp>
{
    using base_type = PlanEnumeratorCRTP<DPccp>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const;
};

/** Greedy operator ordering. */
struct M_EXPORT GOO : PlanEnumeratorCRTP<GOO>
{
    using base_type = PlanEnumeratorCRTP<GOO>;
    using base_type::operator();

    struct node
    {
        Subproblem subproblem;
        Subproblem neighbors;

        node() = default;
        node(Subproblem subproblem, Subproblem neighbors)
            : subproblem(subproblem), neighbors(neighbors)
        { }

        /** Checks whether two nodes can be merged. */
        bool can_merge_with(const node &other) const {
            M_insist(bool(this->subproblem & other.neighbors) == bool(other.subproblem & this->neighbors));
            return bool(this->subproblem & other.neighbors);
        }

        /** Merge two nodes. */
        node merge(const node &other) const {
            M_insist(can_merge_with(other));
            const Subproblem S = this->subproblem | other.subproblem;
            return node(S, (this->neighbors | other.neighbors) - S);
        }

        /** Merges `this` and `other`. */
        node operator+(const node &other) const { return merge(other); }
        /** Merges `other` *into* `this` node. */
        node & operator+=(const node &other) { *this = *this + other; return *this; }

        /** Checks whether `this` node node can be merged with `other`. */
        bool operator&(const node &other) const { return can_merge_with(other); }
    };

    template<typename Callback, typename PlanTable>
    void for_each_join(Callback &&callback, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                       const CostFunction&, const CardinalityEstimator &CE, node *begin, node *end) const
    {
        cnf::CNF condition; // TODO use join condition
        while (begin + 1 != end) {
            using std::swap;

            /*----- Find two most promising subproblems to join. -----*/
            node *left = nullptr, *right = nullptr;
            double least_cardinality = std::numeric_limits<double>::infinity();
            for (node *outer = begin; outer != end; ++outer) {
                for (node *inner = std::next(outer); inner != end; ++inner) {
                    if (*outer & *inner) { // can be merged
                        M_insist((outer->subproblem & inner->subproblem).empty());
                        M_insist(M.is_connected(outer->subproblem, inner->subproblem));
                        const Subproblem joined = outer->subproblem | inner->subproblem;
                        if (not PT[joined].model)
                            PT[joined].model = CE.estimate_join(G, *PT[outer->subproblem].model,
                                                                *PT[inner->subproblem].model, condition);
                        const double C_joined = CE.predict_cardinality(*PT[joined].model);
                        if (C_joined < least_cardinality) {
                            least_cardinality = C_joined;
                            left = outer;
                            right = inner;
                        }
                    }
                }
            }

            /*----- Issue callback. -----*/
            M_insist((left->subproblem & right->subproblem).empty());
            M_insist(M.is_connected((left->subproblem & right->subproblem)));
            callback(left->subproblem, right->subproblem);

            /*----- Join the two most promising subproblems found. -----*/
            M_insist(left);
            M_insist(right);
            M_insist(left < right);
            *left += *right; // merge `right` into `left`
            swap(*right, *--end); // erase old `right`
        }
    }

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const;
};

}
