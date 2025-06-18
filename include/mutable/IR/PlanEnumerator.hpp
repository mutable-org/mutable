#pragma once

#include <cstdint>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/CardinalityStorage.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/mutable-config.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/MinCutAGaT.hpp>
#include <unordered_map>

///> mutable namespace
namespace m
{

    ///> plan enumerator namespace
    namespace pe
    {

        struct enumerate_tag : const_virtual_crtp_helper<enumerate_tag>::
                                   returns<void>::
                                       crtp_args<PlanTableSmallOrDense &, PlanTableLargeAndSparse &>::
                                           args<const QueryGraph &, const CostFunction &>
        {
        };

        /** An interface for all plan enumerators. */
        struct M_EXPORT PlanEnumerator : enumerate_tag::base_type
        {
            using Subproblem = QueryGraph::Subproblem;
            using enumerate_tag::base_type::operator();

            virtual ~PlanEnumerator() {}

            /** Enumerate subplans and fill plan table. */
            template <typename PlanTable>
            void operator()(const QueryGraph &G, const CostFunction &CF, PlanTable &PT) const
            {
                operator()(enumerate_tag{}, PT, G, CF);
            }
        };

        template <typename Actual>
        struct M_EXPORT PlanEnumeratorCRTP : PlanEnumerator, enumerate_tag::derived_type<Actual>
        {
            using PlanEnumerator::operator();
        };

        /** Computes the join order using connected subgraph complement pairs (CCP). */
        struct M_EXPORT DPccp final : PlanEnumeratorCRTP<DPccp>
        {
            using base_type = PlanEnumeratorCRTP<DPccp>;
            using base_type::operator();

            template <typename PlanTable>
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
                {
                }

                /** Checks whether two nodes can be merged. */
                bool can_merge_with(const node &other) const
                {
                    M_insist(bool(this->subproblem & other.neighbors) == bool(other.subproblem & this->neighbors));
                    return bool(this->subproblem & other.neighbors);
                }

                /** Merge two nodes. */
                node merge(const node &other) const
                {
                    M_insist(can_merge_with(other));
                    const Subproblem S = this->subproblem | other.subproblem;
                    return node(S, (this->neighbors | other.neighbors) - S);
                }

                /** Merges `this` and `other`. */
                node operator+(const node &other) const { return merge(other); }
                /** Merges `other` *into* `this` node. */
                node &operator+=(const node &other)
                {
                    *this = *this + other;
                    return *this;
                }

                /** Checks whether `this` node node can be merged with `other`. */
                bool operator&(const node &other) const { return can_merge_with(other); }
            };

            /** Enumerate the sequence of joins that yield the smallest subproblem in each step. */
            template <typename Callback, typename PlanTable>
            void for_each_join(Callback &&callback, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                               const CostFunction &, const CardinalityEstimator &CE, node *begin, node *end) const
            {
                cnf::CNF condition; // TODO use join condition
                while (begin + 1 != end)
                {
                    using std::swap;

                    /*----- Find two most promising subproblems to join. -----*/
                    node *left = nullptr, *right = nullptr;
                    double least_cardinality = std::numeric_limits<double>::infinity();
                    for (node *outer = begin; outer != end; ++outer)
                    {
                        for (node *inner = std::next(outer); inner != end; ++inner)
                        {
                            if (*outer & *inner)
                            { // can be merged
                                M_insist((outer->subproblem & inner->subproblem).empty());
                                M_insist(M.is_connected(outer->subproblem, inner->subproblem));
                                const Subproblem joined = outer->subproblem | inner->subproblem;

                                // Search the CardinalityStorage for matching plans
                                bool found = false;
                                double stored_cardinality = -1.0;

                                // Use Catalog to access CardinalityStorage
                                stored_cardinality = CardinalityStorage::Get().lookup_join_cardinality(
                                    outer->subproblem, inner->subproblem, found);

                                // Create data model if needed - use existing code
                                if (not PT[joined].model)
                                    PT[joined].model = CE.estimate_join(G, *PT[outer->subproblem].model,
                                                                        *PT[inner->subproblem].model, condition);

                                // Use stored cardinality if found, otherwise use estimate
                                double C_joined;
                                if (found)
                                {
                                    C_joined = stored_cardinality;
                                    if (CardinalityStorage::Get().debug_output())
                                        std::cout << "Using stored true cardinality: " << C_joined << std::endl;

                                    // Update the model's cardinality to match the stored true cardinality
                                    PT[joined].model->set_cardinality(C_joined);
                                }

                                C_joined = CE.predict_cardinality(*PT[joined].model);
                                if (C_joined < least_cardinality)
                                {
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
                    *left += *right;      // merge `right` into `left`
                    swap(*right, *--end); // erase old `right`
                }
            }
            template <typename PlanTable>
            void compute_plan(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                              const CostFunction &CF, const CardinalityEstimator &CE, node *begin, node *end) const
            {
                /** Starting at the state described by the node array, we greedyly enumerate joins and thereby compute a plan.*/
                for_each_join([&](const Subproblem left, const Subproblem right)
                              {
            static cnf::CNF condition;
            PT.update(G, CE, CF, left, right, condition); }, PT, G, M, CF, CE, begin, end);
            }

            template <typename PlanTable>
            void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const;
        };

        /** Top-down version of greedy operator ordering. */
        struct M_EXPORT TDGOO : PlanEnumeratorCRTP<TDGOO>
        {
            using base_type = PlanEnumeratorCRTP<TDGOO>;
            using base_type::operator();

            /** Enumerate the sequence of graph cuts that yield the smallest subproblems in each step.  Joins are enumerated in
             * bottom-up fashion. */
            template <typename Callback, typename PlanTable>
            void for_each_join(Callback &&callback, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                               const CostFunction &, const CardinalityEstimator &CE, std::vector<Subproblem> subproblems) const
            {
                std::vector<Subproblem> worklist(std::move(subproblems));
                worklist.reserve(G.num_sources());
                std::vector<std::pair<Subproblem, Subproblem>> joins;
                joins.reserve(G.num_sources() - 1);

                while (not worklist.empty())
                {
                    const Subproblem top = worklist.back();
                    worklist.pop_back();
                    if (top.is_singleton())
                        continue; // nothing to be done for singletons

                    double C_min = std::numeric_limits<decltype(C_min)>::infinity();
                    Subproblem min_left, min_right;
                    auto enumerate_ccp = [&](Subproblem left, Subproblem right) -> void
                    {
                        if (not PT[left].model)
                            PT[left].model = CE.estimate_join_all(G, PT, left, cnf::CNF{}); // TODO: use actual condition
                        if (not PT[right].model)
                            PT[right].model = CE.estimate_join_all(G, PT, right, cnf::CNF{}); // TODO: use actual condition
                        const double C = CE.predict_cardinality(*PT[left].model) + CE.predict_cardinality(*PT[right].model);
                        if (C < C_min)
                        {
                            C_min = C;
                            min_left = left;
                            min_right = right;
                        }
                    };
                    MinCutAGaT{}.partition(M, enumerate_ccp, top);

                    /*----- Save joins on a stack to process them later in reverse order (bottom up). -----*/
                    M_insist(not min_left.empty(), "no partition found");
                    M_insist(not min_right.empty(), "no partition found");
                    joins.emplace_back(min_left, min_right); // save join to stack

                    worklist.emplace_back(min_left);
                    worklist.emplace_back(min_right);
                }

                /*----- Issue callback for joins in bottom-up fashion. -----*/
                for (auto it = joins.crbegin(); it != joins.crend(); ++it)
                    callback(it->first, it->second);
            }

            template <typename PlanTable>
            void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const;
        };

        /** Customizable greedy operator ordering. */
        struct M_EXPORT CustomGOO : PlanEnumeratorCRTP<CustomGOO>
        {
            using base_type = PlanEnumeratorCRTP<CustomGOO>;
            using base_type::operator();

            struct node
            {
                Subproblem subproblem;
                Subproblem neighbors;

                node() = default;
                node(Subproblem subproblem, Subproblem neighbors)
                    : subproblem(subproblem), neighbors(neighbors)
                {
                }

                /** Checks whether two nodes can be merged. */
                bool can_merge_with(const node &other) const
                {
                    M_insist(bool(this->subproblem & other.neighbors) == bool(other.subproblem & this->neighbors));
                    return bool(this->subproblem & other.neighbors);
                }

                /** Merge two nodes. */
                node merge(const node &other) const
                {
                    M_insist(can_merge_with(other));
                    const Subproblem S = this->subproblem | other.subproblem;
                    return node(S, (this->neighbors | other.neighbors) - S);
                }

                /** Merges `this` and `other`. */
                node operator+(const node &other) const { return merge(other); }
                /** Merges `other` *into* `this` node. */
                node &operator+=(const node &other)
                {
                    *this = *this + other;
                    return *this;
                }

                /** Checks whether `this` node node can be merged with `other`. */
                bool operator&(const node &other) const { return can_merge_with(other); }
            };

            /** Enumerate the sequence of joins that yield the smallest subproblem in each step. */
            template <typename Callback, typename PlanTable>
            void for_each_join(Callback &&callback, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                               const CostFunction &, const CardinalityEstimator &CE, node *begin, node *end) const
            {
                cnf::CNF condition; // TODO use join condition
                while (begin + 1 != end)
                {
                    using std::swap;

                    /*----- Find two most promising subproblems to join. -----*/
                    node *left = nullptr, *right = nullptr;
                    double least_cardinality = std::numeric_limits<double>::infinity();
                    for (node *outer = begin; outer != end; ++outer)
                    {
                        for (node *inner = std::next(outer); inner != end; ++inner)
                        {
                            if (*outer & *inner)
                            { // can be merged
                                M_insist((outer->subproblem & inner->subproblem).empty());
                                M_insist(M.is_connected(outer->subproblem, inner->subproblem));
                                const Subproblem joined = outer->subproblem | inner->subproblem;

                                // Search the CardinalityStorage for matching plans
                                bool found = false;
                                double stored_cardinality = -1.0;

                                // Use Catalog to access CardinalityStorage
                                stored_cardinality = CardinalityStorage::Get().lookup_join_cardinality(
                                    outer->subproblem, inner->subproblem, found);

                                // Create data model if needed - use existing code
                                if (not PT[joined].model)
                                    PT[joined].model = CE.estimate_join(G, *PT[outer->subproblem].model,
                                                                        *PT[inner->subproblem].model, condition);

                                // Use stored cardinality if found, otherwise use estimate
                                double C_joined;
                                if (found)
                                {
                                    C_joined = stored_cardinality;
                                    if (CardinalityStorage::Get().debug_output())
                                        std::cout << "Using stored true cardinality: " << C_joined << std::endl;

                                    // Update the model's cardinality to match the stored true cardinality
                                    PT[joined].model->size = C_joined;
                                }
                                else
                                {
                                    C_joined = CE.predict_cardinality(*PT[joined].model);
                                }
                                if (C_joined < least_cardinality)
                                {
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
                    *left += *right;      // merge `right` into `left`
                    swap(*right, *--end); // erase old `right`
                }
            }

            template <typename PlanTable>
            void compute_plan(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                              const CostFunction &CF, const CardinalityEstimator &CE, node *begin, node *end) const
            {
                for_each_join([&](const Subproblem left, const Subproblem right)
                              {
            static cnf::CNF condition;
            PT.update(G, CE, CF, left, right, condition); }, PT, G, M, CF, CE, begin, end);
            }

            template <typename PlanTable>
            void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const;
        };
    }

}
