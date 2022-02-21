#include <mutable/IR/PlanEnumerator.hpp>

#include "util/ADT.hpp"
#include <algorithm>
#include <boost/container/allocator.hpp>
#include <boost/container/node_allocator.hpp>
#include <cstring>
#include <globals.hpp>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/HeuristicSearch.hpp>
#include <mutable/util/list_allocator.hpp>
#include <mutable/util/malloc_allocator.hpp>
#include <mutable/util/Timer.hpp>
#include <queue>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <x86intrin.h>


using namespace m;


#define OUT std::cerr


const std::unordered_map<std::string, PlanEnumerator::kind_t> PlanEnumerator::STR_TO_KIND = {
#define M_PLAN_ENUMERATOR(NAME, _) { #NAME,  PlanEnumerator::PE_ ## NAME },
#include <mutable/tables/PlanEnumerator.tbl>
#undef M_PLAN_ENUMERATOR
};

std::unique_ptr<PlanEnumerator> PlanEnumerator::Create(PlanEnumerator::kind_t kind) {
    switch(kind) {
#define M_PLAN_ENUMERATOR(NAME, _) case PE_ ## NAME: return Create ## NAME();
#include <mutable/tables/PlanEnumerator.tbl>
#undef M_PLAN_ENUMERATOR
    }
}


/*======================================================================================================================
 * DPsize
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming. */
struct DPsize final : PlanEnumeratorCRTP<DPsize>
{
    using base_type = PlanEnumeratorCRTP<DPsize>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        std::size_t n = sources.size();
        AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        /* Process all subplans of size greater than one. */
        for (std::size_t s = 2; s <= n; ++s) {
            for (std::size_t s1 = 1; s1 < s; ++s1) {
                std::size_t s2 = s - s1;
                /* Check for all combinations of subsets if they are valid joins and if so, forward the combination to the
                 * plan table. */
                for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                    if (not PT.has_plan(*S1)) continue; // subproblem S1 not connected -> skip
                    for (auto S2 = GospersHack::enumerate_all(s2, n); S2; ++S2) { // enumerate all subsets of size `s - s1`
                        if (not PT.has_plan(*S2)) continue; // subproblem S2 not connected -> skip
                        if (*S1 & *S2) continue; // subproblems not disjoint -> skip
                        if (not M.is_connected(*S1, *S2)) continue; // subproblems not connected -> skip
                        cnf::CNF condition; // TODO use join condition
                        auto cost = CF.calculate_join_cost(G, PT, CE, *S1, *S2, condition);
                        PT.update(G, CE, *S1, *S2, cost);
                    }
                }
            }
        }
    }
};


/*======================================================================================================================
 * DPsizeOpt
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming.  In addition to `DPsize`, applies the following
 * optimizations.  First, do not enumerate symmetric subproblems.  Second, in case both subproblems are of equal size,
 * consider only subproblems succeeding the first subproblem. */
struct DPsizeOpt final : PlanEnumeratorCRTP<DPsizeOpt>
{
    using base_type = PlanEnumeratorCRTP<DPsizeOpt>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        std::size_t n = sources.size();
        AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        /* Process all subplans of size greater than one. */
        for (std::size_t s = 2; s <= n; ++s) {
            std::size_t m = s / 2; // division with rounding down
            for (std::size_t s1 = 1; s1 <= m; ++s1) {
                std::size_t s2 = s - s1;
                /* Check for all combinations of subsets if they are valid joins and if so, forward the combination to the
                 * plan table. */
                if (s1 == s2) { // exploit commutativity of join: if A⋈ B is possible, so is B⋈ A
                    for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                        if (not PT.has_plan(*S1)) continue; // subproblem not connected -> skip
                        GospersHack S2 = GospersHack::enumerate_from(*S1, n);
                        for (++S2; S2; ++S2) { // enumerate only the subsets following S1
                            if (not PT.has_plan(*S2)) continue; // subproblem not connected -> skip
                            if (*S1 & *S2) continue; // subproblems not disjoint -> skip
                            if (not M.is_connected(*S1, *S2)) continue; // subproblems not connected -> skip
                            /* Exploit commutativity of join. */
                            cnf::CNF condition; // TODO use join condition
                            auto cost = CF.calculate_join_cost(G, PT, CE, *S1, *S2, condition);
                            PT.update(G, CE, *S1, *S2, cost);
                            cost = CF.calculate_join_cost(G, PT, CE, *S2, *S1, condition);
                            PT.update(G, CE, *S2, *S1, cost);
                        }
                    }
                } else {
                    for (auto S1 = GospersHack::enumerate_all(s1, n); S1; ++S1) { // enumerate all subsets of size `s1`
                        if (not PT.has_plan(*S1)) continue; // subproblem not connected -> skip
                        for (auto S2 = GospersHack::enumerate_all(s2, n); S2; ++S2) { // enumerate all subsets of size `s2`
                            if (not PT.has_plan(*S2)) continue; // subproblem not connected -> skip
                            if (*S1 & *S2) continue; // subproblems not disjoint -> skip
                            if (not M.is_connected(*S1, *S2)) continue; // subproblems not connected -> skip
                            /* Exploit commutativity of join. */
                            cnf::CNF condition; // TODO use join condition
                            auto cost = CF.calculate_join_cost(G, PT, CE, *S1, *S2, condition);
                            PT.update(G, CE, *S1, *S2, cost);
                            cost = CF.calculate_join_cost(G, PT, CE, *S1, *S2, condition);
                            PT.update(G, CE, *S2, *S1, cost);
                        }
                    }
                }
            }
        }
    }
};


/*======================================================================================================================
 * DPsizeSub
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming. */
struct DPsizeSub final : PlanEnumeratorCRTP<DPsizeSub>
{
    using base_type = PlanEnumeratorCRTP<DPsizeSub>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        std::size_t n = sources.size();
        AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        /* Process all subplans of size greater than one. */
        for (std::size_t s = 2; s <= n; ++s) {
            for (auto S = GospersHack::enumerate_all(s, n); S; ++S) { // enumerate all subsets of size `s`
                if (not M.is_connected(*S)) continue; // not connected -> skip
                for (Subproblem O(least_subset(*S)); O != *S; O = Subproblem(next_subset(O, *S))) {
                    Subproblem Comp = *S - O;
                    M_insist(M.is_connected(O, Comp), "implied by S inducing a connected subgraph");
                    if (not PT.has_plan(O)) continue; // not connected -> skip
                    if (not PT.has_plan(Comp)) continue; // not connected -> skip
                    cnf::CNF condition; // TODO use join condition
                    auto cost = CF.calculate_join_cost(G, PT, CE, O, Comp, condition);
                    PT.update(G, CE, O, Comp, cost);
                }
            }
        }
    }
};


/*======================================================================================================================
 * DPsub
 *====================================================================================================================*/

/** Computes the join order using subset-based dynamic programming. */
struct DPsub final : PlanEnumeratorCRTP<DPsub>
{
    using base_type = PlanEnumeratorCRTP<DPsub>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        const std::size_t n = sources.size();
        const AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
            Subproblem S(i);
            if (S.size() == 1) continue; // no non-empty and strict subset of S -> skip
            if (not M.is_connected(S)) continue; // not connected -> skip
            for (Subproblem S1(least_subset(S)); S1 != S; S1 = Subproblem(next_subset(S1, S))) {
                Subproblem S2 = S - S1;
                M_insist(M.is_connected(S1, S2), "implied by S inducing a connected subgraph");
                if (not PT.has_plan(S1)) continue; // not connected -> skip
                if (not PT.has_plan(S2)) continue; // not connected -> skip
                cnf::CNF condition; // TODO use join condition
                auto cost = CF.calculate_join_cost(G, PT, CE, S1, S2, condition);
                PT.update(G, CE, S1, S2, cost);
            }
        }
    }
};


/*======================================================================================================================
 * DPsubOpt
 *====================================================================================================================*/

/** Computes the join order using subset-based dynamic programming.  In comparison to `DPsub`, do not enumerate
 * symmetric subproblems. */
struct DPsubOpt final : PlanEnumeratorCRTP<DPsubOpt>
{
    using base_type = PlanEnumeratorCRTP<DPsubOpt>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        const std::size_t n = sources.size();
        const AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
            Subproblem S(i);
            if (S.size() == 1) continue; // no non-empty and strict subset of S -> skip
            if (not M.is_connected(S)) continue;
            /* Compute break condition to avoid enumerating symmetric subproblems. */
            uint64_t offset = S.capacity() - __builtin_clzl(uint64_t(S));
            M_insist(offset != 0, "invalid subproblem offset");
            Subproblem limit(1UL << (offset - 1));
            for (Subproblem S1(least_subset(S)); S1 != limit; S1 = Subproblem(next_subset(S1, S))) {
                Subproblem S2 = S - S1; // = S \ S1;
                M_insist(M.is_connected(S1, S2), "implied by S inducing a connected subgraph");
                if (not PT.has_plan(S1)) continue; // not connected -> skip
                if (not PT.has_plan(S2)) continue; // not connected -> skip
                /* Exploit commutativity of join. */
                cnf::CNF condition; // TODO use join condition
                auto cost = CF.calculate_join_cost(G, PT, CE, S1, S2, condition);
                PT.update(G, CE, S1, S2, cost);
                cost = CF.calculate_join_cost(G, PT, CE, S1, S2, condition);
                PT.update(G, CE, S2, S1, cost);
            }
        }
    }
};


/*======================================================================================================================
 * DPccp
 *====================================================================================================================*/

/** Computes the join order using connected subgraph complement pairs (CCP). */
struct DPccp final : PlanEnumeratorCRTP<DPccp>
{
    using base_type = PlanEnumeratorCRTP<DPccp>;
    using base_type::operator();

    /** For each connected subgraph (csg) `S1` of `G`, enumerate all complement connected subgraphs,
     * i.e.\ all csgs of `G - S1` that are connected to `S1`. */
    template<typename PlanTable>
    void enumerate_cmp(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF, PlanTable &PT,
                       const CardinalityEstimator &CE, Subproblem S1) const
    {
        SmallBitset min = least_subset(S1); // node in `S1` with the lowest ID
        Subproblem Bmin((uint64_t(min) << 1UL) - 1UL); // all nodes 'smaller' than `min`
        Subproblem X(Bmin | S1); // exclude `S1` and all nodes with a lower ID than the smallest node in `S1`

        Subproblem N = M.neighbors(S1) - X;
        if (not N) return; // empty neighborhood

        /* Process subgraphs in breadth-first order.  The queue contains pairs of connected subgraphs and the corresponding
         * set of nodes to exclude. */
        std::queue<std::pair<Subproblem, Subproblem>> Q;

        for (std::size_t i = G.sources().size(); i != 0; --i) {
            Subproblem vi(1UL << (i - 1));
            if (not (vi & N)) continue; // `vi` is in neighborhood of `S1`
            /* Compute exclude set. */
            Subproblem excluded((1UL << i) - 1); // all nodes 'smaller' than the current node `vi`
            Subproblem excluded_N = excluded & N; // only exclude nodes in the neighborhood of `S1`
            Q.emplace(std::make_pair(vi, X | excluded_N));
            while (not Q.empty()) {
                auto [S, X] = Q.front();
                Q.pop();
                /* Update `PlanTable` with connected subgraph complement pair (S1, S). */
                cnf::CNF condition; // TODO use join condition
                auto cost = CF.calculate_join_cost(G, PT, CE, S1, S, condition);
                PT.update(G, CE, S1, S, cost);
                cost = CF.calculate_join_cost(G, PT, CE, S, S1, condition);
                PT.update(G, CE, S, S1, cost);

                Subproblem N = M.neighbors(S) - X;
                /* Iterate over all subsets `sub` in `N` */
                for (Subproblem sub(least_subset(N)); bool(sub); sub = Subproblem(next_subset(sub, N))) {
                    /* Connected subgraph `S` expanded by `sub` constitutes a new connected subgraph.  For each of those
                     * connected subgraphs, do not consider the neighborhood `N` in addition to the existing set of excluded
                     * nodes. */
                    Q.emplace(std::make_pair(S | sub, X | N));
                }
            }
        }
    }

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        const std::size_t n = sources.size();
        const AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        /* Process subgraphs in breadth-first order.  The queue contains pairs of connected subgraphs and the corresponding
         * set of nodes to exclude. */
        std::queue<std::pair<Subproblem, Subproblem>> Q;

        for (std::size_t i = n; i != 0; --i) {
            /* For a given single node subgraph, the node itself and all nodes with a lower ID are excluded. */
            Q.emplace(std::make_pair(Subproblem(1UL << (i - 1)), Subproblem ((1UL << i) - 1)));
            while (not Q.empty()) {
                auto [S, X] = Q.front();
                Q.pop();
                enumerate_cmp(G, M, CF, PT, CE, S);

                Subproblem N = M.neighbors(S) - X;
                /* Iterate over all subsets `sub` in `N` */
                for (Subproblem sub(least_subset(N)); bool(sub); sub = Subproblem(next_subset(sub, N)))
                    /* Connected subgraph `S` expanded by `sub` constitutes a new connected subgraph.  For each of those
                     * connected subgraphs, do not consider the neighborhood `N` in addition to the existing set of excluded
                     * nodes. */
                    Q.emplace(std::make_pair(S | sub, X | N));
            }
        }
    }
};


/*======================================================================================================================
 * IK/KBZ
 *====================================================================================================================*/

/** Implements join ordering using the IK/KBZ algorithm.
 *
 * See Toshihide (I)baraki and Tiko (K)ameda. "On the optimal nesting order for computing n-relational joins." and Ravi
 * (K)rishnamurthy, Haran (B)oral, and Carlo (Z)aniolo. "Optimization of Nonrecursive Queries." */
struct IKKBZ final : PlanEnumeratorCRTP<IKKBZ>
{
    using base_type = PlanEnumeratorCRTP<IKKBZ>;
    using base_type::operator();

    template<typename PlanTable>
    std::vector<std::size_t>
    linearize(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
              const CardinalityEstimator &CE) const
    {
        /* Computes the selectivity between two relations, identified by indices `u` and `v`, respectively. */
        auto selectivity = [&](std::size_t u, std::size_t v) {
            const SmallBitset U(1UL << u);
            const SmallBitset V(1UL << v);
            auto &model_u = *PT[U].model;
            auto &model_v = *PT[V].model;
            auto &joined = PT[U|V];
            if (not joined.model) {
                cnf::CNF condition; // TODO use join condition
                joined.model = CE.estimate_join(G, model_u, model_v, condition);
            }
            const double cardinality_joined = double(CE.predict_cardinality(*joined.model));
            return cardinality_joined / double(CE.predict_cardinality(model_u)) / double(CE.predict_cardinality(model_v));
        };

        /* Compute MST w.r.t. selectivity of join edges.  See Ravi Krishnamurthy, Haran Boral, and Carlo Zaniolo.
         * "Optimization of Nonrecursive Queries." */
        const AdjacencyMatrix MST = M.minimum_spanning_forest(selectivity);

        std::vector<std::size_t> linearization;
        linearization.reserve(G.num_sources());
        std::vector<std::size_t> best_linearization;
        best_linearization.reserve(G.num_sources());
        double least_cost = std::numeric_limits<decltype(least_cost)>::infinity();

        /* The *rank* of a relation is defined as the factor by which the result set grows when joining with this
         * relation.  The rank is defined for a *child* relation and its *parent* relation w.r.t. the MST.
         * "Intuitively, the rank measures the increase in the intermediate result per unit differential cost of doing
         * the join." */
        auto rank = [&](std::size_t parent_id, std::size_t child_id) -> double {
            const SmallBitset parent(1UL << parent_id);
            const SmallBitset child(1UL << child_id);
            M_insist(MST.is_connected(parent, child), "relations must be joinable");
            M_insist(MST.neighbors(parent)[child_id]);

            const auto &model_parent = *PT[parent].model;
            const auto &model_child = *PT[child].model;
            if (not PT[parent|child].model) {
                cnf::CNF condition; // TODO use join condition
                PT[parent|child].model = CE.estimate_join(G, model_parent, model_child, condition);
            }
            const double cardinality_joined = CE.predict_cardinality(*PT[parent|child].model);
            const double cardinality_parent  = CE.predict_cardinality(model_parent);
            const double cardinality_child  = CE.predict_cardinality(model_child);
            auto g = [](double cardinality) -> double { return 1 * cardinality; }; // TODO adapt to cost function
            return (cardinality_joined - cardinality_parent) / g(cardinality_child);
        };

        struct ranked_relation
        {
            private:
            std::size_t id_;
            double rank_;

            public:
            ranked_relation(std::size_t id, double rank) : id_(id), rank_(rank) { }

            std::size_t id() const { return id_; }
            double rank() const { return rank_; }

            bool operator<(const ranked_relation &other) const { return this->rank() < other.rank(); }
            bool operator>(const ranked_relation &other) const { return this->rank() > other.rank(); }
        };

        /*---- Consider each relation as root and linearize the query tree. -----*/
        for (std::size_t root_id = 0; root_id != G.num_sources(); ++root_id) {
            Subproblem root(1UL << root_id);

            linearization.clear();
            linearization.emplace_back(root_id);

            /* Initialize MIN heap with successors of root of query tree. */
            std::priority_queue<ranked_relation, std::vector<ranked_relation>, std::greater<ranked_relation>> Q;
            for (std::size_t n : MST.neighbors(root))
                Q.emplace(n, rank(root_id, n));

            Subproblem joined = root;
            while (not Q.empty()) {
                ranked_relation ranked_relation = Q.top();
                Q.pop();

                const Subproblem R(1UL << ranked_relation.id());
                M_insist((joined & R).empty());
                M_insist(MST.is_connected(joined, R));
                linearization.emplace_back(ranked_relation.id());

                /*----- Join next relation with already joined relations. -----*/
                cnf::CNF condition; // TODO use join condition
                const double join_cost = CF.calculate_join_cost(G, PT, CE, R, joined, condition);
                PT.update(G, CE, joined, R, join_cost);
                joined |= R; // add R to the joined relations

                /*----- Add all children of `R` to the priority queue. -----*/
                const Subproblem N = MST.neighbors(R) - joined;
                for (std::size_t n : N)
                    Q.emplace(n, rank(ranked_relation.id(), n));
            }

            /* Get the cost of the final plan. */
            const double cost = PT[joined].cost;

            /*----- Clear plan table. -----*/
            Subproblem runner = root;
            M_insist(linearization[0] == root_id);
            for (std::size_t i = 1; i != linearization.size(); ++i) {
                const std::size_t R = linearization[i];
                M_insist(not runner[R]);
                runner[R] = true;
                M_insist(not runner.singleton());
                M_insist(bool(PT[runner].model), "must have computed a model during linearization");
                M_insist(bool(PT[runner].left), "must have a left subplan");
                M_insist(bool(PT[runner].right), "must have a right subplan");
                PT[runner] = PlanTableEntry();
            }

            if (cost < least_cost) {
                using std::swap;
                swap(best_linearization, linearization);
                least_cost = cost;
            }
        }

        return best_linearization;
    }

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        /* Linearize the vertices. */
        const std::vector<std::size_t> linearization = linearize(PT, G, M, CF, CE);
        M_insist(linearization.size() == G.num_sources());

        /*----- Reconstruct the right-deep plan from the linearization. -----*/
        Subproblem right(1UL << linearization[0]);
        for (std::size_t i = 1; i < G.num_sources(); ++i) {
            const Subproblem left(1UL << linearization[i]);
            cnf::CNF condition; // TODO use join condition
            const double cost = CF.calculate_join_cost(G, PT, CE, left, right, condition);
            PT.update(G, CE, left, right, cost);
            right = left | right;
        }
    }
};


/*======================================================================================================================
 * LinearizedDP
 *====================================================================================================================*/

struct LinearizedDP final : PlanEnumeratorCRTP<LinearizedDP>
{
    using base_type = PlanEnumeratorCRTP<LinearizedDP>;
    using base_type::operator();

    struct Sequence
    {
        private:
        const std::vector<std::size_t> &linearization_;
        ///> the index in the linearization of the first element of this sequence
        std::size_t begin_;
        ///> the index in the linearization one past the last element of this sequence
        std::size_t end_;
        ///> the subproblem formed by this sequence
        Subproblem S_;

        public:
        Sequence(const std::vector<std::size_t> &linearization, std::size_t begin, std::size_t end)
            : linearization_(linearization), begin_(begin), end_(end)
        {
            S_ = compute_subproblem_from_sequence(begin_, end_);
        }

        std::size_t length() const { return end_ - begin_; }
        Subproblem subproblem() const { return S_; }
        /** Returns the index in linearization of the first element of this sequence. */
        std::size_t first_index() const { return begin_; }
        /** Returns the index in linearization of the last element of this sequence. */
        std::size_t last_index() const { return end_ - 1; }
        std::size_t end() const { return end_; }

        std::size_t first() const { return linearization_[first_index()]; }
        std::size_t last() const { return linearization_[last_index()]; }

        bool is_at_front() const { return begin_ == 0; }
        bool is_at_back() const { return end_ == linearization_.size(); }

        Subproblem compute_subproblem_from_sequence(std::size_t begin, std::size_t end) {
            M_insist(begin < end);
            M_insist(end <= linearization_.size());
            Subproblem S;
            while (begin != end)
                S[linearization_[begin++]] = true;
            return S;
        }

        void extend_at_front() {
            M_insist(begin_ > 0);
            --begin_;
            S_[first()] = true;
        }

        void extend_at_back() {
            M_insist(end_ < linearization_.size());
            ++end_;
            S_[last()] = true;
        }

        void shrink_at_front() {
            M_insist(begin_ < end_);
            S_[first()] = false;
            ++begin_;
        }

        void shrink_at_back() {
            M_insist(begin_ < end_);
            S_[last()] = false;
            --end_;
        }

        friend std::ostream & operator<<(std::ostream &out, const Sequence &seq) {
            out << seq.first_index() << '-' << seq.last_index() << ": [";
            for (std::size_t i = seq.first_index(); i <= seq.last_index(); ++i) {
                if (i != seq.first_index()) out << ", ";
                out << seq.linearization_[i];
            }
            return out << ']';
        }

        void dump(std::ostream &out) const { out << *this << std::endl; }
        void dump() const { dump(std::cerr); }
    };

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        if (G.num_sources() <= 1) return;

        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();
        const AdjacencyMatrix M(G);
        const std::vector<std::size_t> linearization = IKKBZ{}.linearize(PT, G, M, CF, CE);
        const std::size_t num_relations = G.num_sources();

        Sequence seq(linearization, 0, 2);
        bool moves_right = true;
        for (;;) {
            if (M.is_connected(seq.subproblem())){
                /*----- Consider all dissections of `seq` into two sequences. -----*/
                for (std::size_t mid = seq.first_index() + 1; mid <= seq.last_index(); ++mid) {
                    const Subproblem left = seq.compute_subproblem_from_sequence(seq.first_index(), mid);
                    const Subproblem right = seq.subproblem() - left;

                    const bool is_left_connected = PT.has_plan(left);
                    const bool is_right_connected = PT.has_plan(right);
                    if (is_left_connected and is_right_connected) {
                        cnf::CNF condition; // TODO use join condition
                        const double cost = CF.calculate_join_cost(G, PT, CE, left, right, condition);
                        PT.update(G, CE, left, right, cost);
                    }
                }
            }

            if (seq.length() == num_relations)
                break;

            /*----- Move and grow the sequence. -----*/
            if (moves_right) {
                if (seq.is_at_back()) [[unlikely]] {
                    moves_right = false;
                    seq.extend_at_front();
                } else {
                    seq.extend_at_back();
                    seq.shrink_at_front();
                }
            } else {
                if (seq.is_at_front()) [[unlikely]] {
                    moves_right = true;
                    seq.extend_at_back();
                } else {
                    seq.extend_at_front();
                    seq.shrink_at_back();
                }
            }
        }
    }
};


/*======================================================================================================================
 * TDbasic
 *====================================================================================================================*/

struct TDbasic final : PlanEnumeratorCRTP<TDbasic>
{
    using base_type = PlanEnumeratorCRTP<TDbasic>;
    using base_type::operator();

    template<typename PlanTable>
    void PlanGen(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF, const CardinalityEstimator &CE,
                 PlanTable &PT, Subproblem S) const
    {
        if (not PT.has_plan(S)) {
            /* Naive Partitioning */
            /* Iterate over all non-empty and strict subsets in `S`. */
            for (Subproblem sub(least_subset(S)); sub != S; sub = Subproblem(next_subset(sub, S))) {
                Subproblem complement = S - sub;
                /* Check for valid connected subgraph complement pair. */
                if (uint64_t(least_subset(sub)) < uint64_t(least_subset(complement)) and
                    M.is_connected(sub) and M.is_connected(complement))
                {
                    /* Process `sub` and `complement` recursively. */
                    PlanGen(G, M, CF, CE, PT, sub);
                    PlanGen(G, M, CF, CE, PT, complement);

                    /* Update `PlanTable`. */
                    cnf::CNF condition; // TODO use join condition
                    auto cost = CF.calculate_join_cost(G, PT, CE, sub, complement, condition);
                    PT.update(G, CE, sub, complement, cost);
                    cost = CF.calculate_join_cost(G, PT, CE, complement, sub, condition);
                    PT.update(G, CE, complement, sub, cost);
                }
            }
        }
    }

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        std::size_t n = sources.size();
        AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        PlanGen(G, M, CF, CE, PT, Subproblem((1UL << n) - 1));
    }
};


/*======================================================================================================================
 * TDMinCutAGaT
 *====================================================================================================================*/

struct TDMinCutAGaT final : PlanEnumeratorCRTP<TDMinCutAGaT>
{
    using base_type = PlanEnumeratorCRTP<TDMinCutAGaT>;
    using base_type::operator();

    struct queue_entry
    {
        Subproblem C;
        Subproblem X;
        Subproblem T;

        queue_entry(Subproblem C, Subproblem X, Subproblem T) : C(C), X(X), T(T) { }
    };

    template<typename PlanTable>
    void MinCutAGaT(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                    const CardinalityEstimator &CE, PlanTable &PT,
                    Subproblem S, Subproblem C, Subproblem X, Subproblem T) const
    {
        if (PT.has_plan(S)) return;

        std::queue<queue_entry> queue;
        queue.push(queue_entry(C, X, T));

        while (not queue.empty()) {
            auto e = queue.front();
            queue.pop();

            Subproblem T_tmp;
            Subproblem X_tmp;
            Subproblem N_T = (M.neighbors(e.T) & S) - e.C; // sufficient to check if neighbors of T are connected
            if (M.is_connected(N_T)) {
                /* ccp (C, S - C) found, process `C` and `S - C` recursively. */
                Subproblem cmpl = S - e.C;
                MinCutAGaT(G, M, CF, CE, PT,
                           e.C, Subproblem(least_subset(e.C)), Subproblem(0), Subproblem(least_subset(e.C)));
                MinCutAGaT(G, M, CF, CE, PT,
                           cmpl, Subproblem(least_subset(cmpl)), Subproblem(0), Subproblem(least_subset(cmpl)));

                /* Update `PlanTable`. */
                cnf::CNF condition; // TODO use join condition
                auto cost = CF.calculate_join_cost(G, PT, CE, e.C, cmpl, condition);
                PT.update(G, CE, e.C, cmpl, cost);
                cost = CF.calculate_join_cost(G, PT, CE, cmpl, e.C, condition);
                PT.update(G, CE, cmpl, e.C, cost);

                T_tmp = Subproblem(0);
            } else T_tmp = e.C;

            if (e.C.size() + 1 >= S.size()) continue;

            X_tmp = e.X;
            Subproblem N_C = M.neighbors(e.C);

            for (auto i : (N_C - e.X)) {
                Subproblem v(1UL << i);
                queue.push(queue_entry(e.C | v, X_tmp, T_tmp | v));
                X_tmp = X_tmp | v;
            }
        }
    }

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        std::size_t n = sources.size();
        AdjacencyMatrix M(G);
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        MinCutAGaT(G, M, CF, CE, PT, Subproblem((1UL << n) - 1), Subproblem(1), Subproblem(0), Subproblem(1));
    }
};


/*======================================================================================================================
 * Heuristic Search
 *====================================================================================================================*/

/** A "less than" comparator for `Subproblem`s. */
inline bool subproblem_lt(Subproblem left, Subproblem right) { return uint64_t(left) < uint64_t(right); };


/*----------------------------------------------------------------------------------------------------------------------
 * States
 *--------------------------------------------------------------------------------------------------------------------*/

namespace search_states {

// #define WITH_STATE_COUNTERS
#if !defined(NDEBUG) && !defined(WITH_STATE_COUNTERS)
#define WITH_STATE_COUNTERS
#endif

/** A state in the search space.
 *
 * A state consists of the accumulated costs of actions to reach this state from the from initial state and a desciption
 * of the actual problem within the state.  The problem is described as the sorted list of `Subproblem`s yet to be
 * joined.
 */
template<typename Actual>
struct Base : crtp<Actual, Base>
{
    using crtp<Actual, Base>::actual;
    using size_type = std::size_t;

    /*----- State counters -------------------------------------------------------------------------------------------*/
#ifdef WITH_STATE_COUNTERS
    struct state_counters_t
    {
        unsigned num_states_generated;
        unsigned num_states_expanded;
        unsigned num_states_constructed;
        unsigned num_states_disposed;

        state_counters_t()
            : num_states_generated(0)
            , num_states_expanded(0)
            , num_states_constructed(0)
            , num_states_disposed(0)
        { }
    };

    private:
    static state_counters_t state_counters_;

    public:
    static void RESET_STATE_COUNTERS() { state_counters_ = state_counters_t(); }
    static unsigned NUM_STATES_GENERATED() { return state_counters_.num_states_generated; }
    static unsigned NUM_STATES_EXPANDED() { return state_counters_.num_states_expanded; }
    static unsigned NUM_STATES_CONSTRUCTED() { return state_counters_.num_states_constructed; }
    static unsigned NUM_STATES_DISPOSED() { return state_counters_.num_states_disposed; }

    static void INCREMENT_NUM_STATES_GENERATED() { state_counters_.num_states_generated += 1; }
    static void INCREMENT_NUM_STATES_EXPANDED() { state_counters_.num_states_expanded += 1; }
    static void INCREMENT_NUM_STATES_CONSTRUCTED() { state_counters_.num_states_constructed += 1; }
    static void INCREMENT_NUM_STATES_DISPOSED() { state_counters_.num_states_disposed += 1; }

    static state_counters_t STATE_COUNTERS() { return state_counters_; }
    static state_counters_t STATE_COUNTERS(state_counters_t new_counters) {
        state_counters_t old_counters = state_counters_;
        state_counters_ = new_counters;
        return old_counters;
    }
#else
    static void RESET_STATE_COUNTERS() { /* nothing to be done */ };
    static unsigned NUM_STATES_GENERATED() { return 0; }
    static unsigned NUM_STATES_EXPANDED() { return 0; }
    static unsigned NUM_STATES_CONSTRUCTED() { return 0; }
    static unsigned NUM_STATES_DISPOSED() { return 0; }

    static void INCREMENT_NUM_STATES_GENERATED() { /* nothing to be done */ }
    static void INCREMENT_NUM_STATES_EXPANDED() { /* nothing to be done */ }
    static void INCREMENT_NUM_STATES_CONSTRUCTED() { /* nothing to be done */ }
    static void INCREMENT_NUM_STATES_DISPOSED() { /* nothing to be done */ }
#endif

    /*----- Getters --------------------------------------------------------------------------------------------------*/

    /** Returns `true` iff this is a goal state. */
    template<typename PlanTable>
    bool is_goal(const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                 const CardinalityEstimator &CE) const
    {
        return actual().is_goal(PT, G, M, CF, CE);
    }

    /** Returns the cost to reach this state from the initial state. */
    double g() const { return actual().g(); }

    /*----- Setters --------------------------------------------------------------------------------------------------*/

    /** Reduces the *g* value of the state. */
    double decrease_g(double new_g) const { return actual().decrease_g(new_g); }

    /*----- Comparison -----------------------------------------------------------------------------------------------*/

    bool operator==(const Base &other) const { return actual().operator==(other.actual()); }
    bool operator!=(const Base &other) const { return actual().operator!=(other.actual()); }
    bool operator<(const Base &other) const { return actual().operator<(other.actual()); }

    /** Returns `true` iff `this` and `other` have the exact same `Subproblem`s. */
    /** Calls `callback` on every state reachable from this state by a single actions. */
    template<typename Callback, typename PlanTable>
    void for_each_successor(Callback &&callback, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                            const CostFunction &CF, const CardinalityEstimator &CE) const {
        actual().for_each_successor(std::forward<Callback>(callback), PT, G, M, CF, CE);
    }

    /*----- Iteration ------------------------------------------------------------------------------------------------*/

    template<typename Callback>
    void for_each_subproblem(Callback &&callback, const QueryGraph &G) const {
        actual().template for_each_subproblem<Callback>(std::forward<Callback>(callback), G);
    }

    /*----- Debugging ------------------------------------------------------------------------------------------------*/
M_LCOV_EXCL_START
    void dump(std::ostream &out) const { out << actual() << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
};

#ifdef WITH_STATE_COUNTERS
template<typename Actual>
typename Base<Actual>::state_counters_t
Base<Actual>::state_counters_;
#endif

struct SubproblemsBottomUp : Base<SubproblemsBottomUp>
{
    using base_type = Base<SubproblemsBottomUp>;
    using allocator_type = boost::container::node_allocator<Subproblem>;
    using size_type = typename base_type::size_type;
    using iterator = Subproblem*;
    using const_iterator = const Subproblem*;

    private:
    ///> class-wide allocator, used by all instances
    static allocator_type allocator_;

    public:
    ///> returns a reference to the class-wide allocator
    static allocator_type & get_allocator() { return allocator_; }

    ///> the cost to reach this state from the initial state
    mutable double g_;
    ///> number of subproblems in this state
    size_type size_ = 0;
    ///> array of subproblems
    Subproblem *subproblems_ = nullptr;

    /*----- The Big Four and a Half, copy & swap idiom ---------------------------------------------------------------*/
    public:
    friend void swap(SubproblemsBottomUp &first, SubproblemsBottomUp &second) {
        using std::swap;
        swap(first.g_,           second.g_);
        swap(first.size_,        second.size_);
        swap(first.subproblems_, second.subproblems_);
    }

    SubproblemsBottomUp() = default;

    /** Creates a state with actual costs `g` and given `subproblems`. */
    SubproblemsBottomUp(double g, size_type size, Subproblem *subproblems)
        : g_(g)
        , size_(size)
        , subproblems_(subproblems)
    {
        M_insist(size == 0 or subproblems_);
        M_insist(std::is_sorted(begin(), end(), subproblem_lt));
        base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
    }

    /** Creates an initial state with the given `subproblems`. */
    SubproblemsBottomUp(size_type size, Subproblem *subproblems)
        : SubproblemsBottomUp(0, size, subproblems)
    { }

    /** Creates a state with actual costs `g` and subproblems in range `[begin; end)`. */
    template<typename It>
    SubproblemsBottomUp(double g, It begin, It end)
        : g_(g)
        , size_(std::distance(begin, end))
        , subproblems_(allocator_.allocate(size_))
    {
        M_insist(begin != end);
        std::copy(begin, end, this->begin());
        M_insist(std::is_sorted(this->begin(), this->end(), subproblem_lt));
        base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
    }

    /** Copy c'tor. */
    explicit SubproblemsBottomUp(const SubproblemsBottomUp &other)
        : g_(other.g_)
        , size_(other.size_)
        , subproblems_(allocator_.allocate(size_))
    {
        base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
        M_insist(subproblems_);
        std::copy(other.begin(), other.end(), this->begin());
    }

    /** Move c'tor. */
    SubproblemsBottomUp(SubproblemsBottomUp &&other) : SubproblemsBottomUp() { swap(*this, other); }
    /** Assignment. */
    SubproblemsBottomUp & operator=(SubproblemsBottomUp other) { swap(*this, other); return *this; }

    /** D'tor. */
    ~SubproblemsBottomUp() {
        if (subproblems_) base_type::INCREMENT_NUM_STATES_DISPOSED();
        allocator_.deallocate(subproblems_, size_);
    }

    /*----- Factory methods. -----------------------------------------------------------------------------------------*/

    static SubproblemsBottomUp CreateInitial(const QueryGraph &G, const AdjacencyMatrix&) {
        const size_type size = G.sources().size();
        auto subproblems = allocator_.allocate(size);
        for (auto ds : G.sources())
            new (&subproblems[ds->id()]) Subproblem(1UL << ds->id());
        M_insist(std::is_sorted(subproblems, subproblems + size, subproblem_lt));
        return SubproblemsBottomUp(size, subproblems);
    }

    /*----- Getters --------------------------------------------------------------------------------------------------*/

    template<typename PlanTable>
    bool is_goal(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                 const CardinalityEstimator&) const
    {
        return size() <= 1;
    }
    double g() const { return g_; }
    void decrease_g(double new_g) const { M_insist(new_g <= g_); g_ = new_g; }
    size_type size() const { return size_; }
    Subproblem operator[](std::size_t idx) const { M_insist(idx < size_); return subproblems_[idx]; }

    /*----- Iteration ------------------------------------------------------------------------------------------------*/

    iterator begin() { return subproblems_; };
    iterator end() { return begin() + size(); }
    const_iterator begin() const { return subproblems_; };
    const_iterator end() const { return begin() + size(); }
    const_iterator cbegin() const { return begin(); };
    const_iterator cend() const { return end(); }

    template<typename Callback>
    void for_each_subproblem(Callback &&callback, const QueryGraph&) const {
        for (Subproblem S : *this)
            callback(S);
    }

    /*----- Comparison -----------------------------------------------------------------------------------------------*/

    /** Returns `true` iff `this` and `other` have the exact same `Subproblem`s. */
    bool operator==(const SubproblemsBottomUp &other) const {
        if (this->size() != other.size()) return false;
        M_insist(this->size() == other.size());
        auto this_it = this->cbegin();
        auto other_it = other.cbegin();
        for (; this_it != this->cend(); ++this_it, ++other_it) {
            if (*this_it != *other_it)
                return false;
        }
        return true;
    }

    bool operator!=(const SubproblemsBottomUp &other) const { return not operator==(other); }

    bool operator<(const SubproblemsBottomUp &other) const {
        return std::lexicographical_compare(cbegin(), cend(), other.cbegin(), other.cend(), subproblem_lt);
    }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const SubproblemsBottomUp &S) {
        out << "g = " << S.g() << ", [";
        for (auto it = S.cbegin(); it != S.cend(); ++it) {
            if (it != S.cbegin()) out << ", ";
            it->print_fixed_length(out, 15);
        }
        return out << ']';
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
};

SubproblemsBottomUp::allocator_type SubproblemsBottomUp::allocator_;

}

namespace std {

template<>
struct hash<search_states::SubproblemsBottomUp>
{
    uint64_t operator()(const search_states::SubproblemsBottomUp &state) const {
        /* Rolling hash with multiplier taken from [1] where the moduli is 2^64.
         * [1] http://www.ams.org/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf */
        uint64_t hash = 0;
        for (Subproblem s : state) {
            hash = hash ^ uint64_t(s);
            hash = hash * 1181783497276652981UL + 4292484099903637661UL;
        }
        return hash;
    }
};

}

namespace search_states {

struct EdgesBottomUp : Base<EdgesBottomUp>
{
    using base_type = Base<EdgesBottomUp>;
    using allocator_type = boost::container::node_allocator<unsigned>;
    using size_type = typename base_type::size_type;
    using iterator = unsigned*;
    using const_iterator = const unsigned*;

    private:
    ///> class-wide allocator, used by all instances
    static allocator_type allocator_;

    public:
    ///> returns a reference to the class-wide allocator
    static allocator_type & get_allocator() { return allocator_; }

    private:
    ///> number of joins necessary to reach goal
    size_type num_joins_to_goal_ = 0;
    ///> the cost to reach this state from the initial state
    mutable double g_;
    ///> number of joins performed to reach this state
    size_type num_joins_ = 0;
    ///> array of IDs of joins performed
    unsigned *joins_ = nullptr;

    /*----- The Big Four and a Half, copy & swap idiom ---------------------------------------------------------------*/
    public:
    friend void swap(EdgesBottomUp &first, EdgesBottomUp &second) {
        using std::swap;
        swap(first.g_,                 second.g_);
        swap(first.num_joins_,         second.num_joins_);
        swap(first.num_joins_to_goal_, second.num_joins_to_goal_);
        swap(first.joins_,             second.joins_);
    }

    EdgesBottomUp() = default;

    /** Creates a state with actual costs `g` and given `subproblems`. */
    EdgesBottomUp(size_type num_joins_to_goal, double g, size_type num_joins,
                             unsigned *joins)
        : num_joins_to_goal_(num_joins_to_goal)
        , g_(g)
        , num_joins_(num_joins)
        , joins_(joins)
    {
        M_insist(num_joins_ == 0 or bool(joins_));
        M_insist(std::is_sorted(cbegin(), cend()), "joins must be sorted by index");
#ifdef WITH_STATE_COUNTERS
        if (num_joins_)
            base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
#endif
    }

    /** Creates an initial state with the given `subproblems`. */
    EdgesBottomUp(size_type num_joins_to_goal, size_type num_joins, unsigned *joins)
        : EdgesBottomUp(num_joins_to_goal, 0, num_joins, joins)
    { }

    /** Creates a state with actual costs `g` and subproblems in range `[begin; end)`. */
    template<typename It>
    EdgesBottomUp(size_type num_joins_to_goal, double g, It begin, It end)
        : num_joins_to_goal_(num_joins_to_goal)
        , g_(g)
        , num_joins_(std::distance(begin, end))
        , joins_(allocator_.allocate(num_joins_))
    {
        M_insist(begin != end);
        std::copy(begin, end, this->begin());
        M_insist(std::is_sorted(cbegin(), cend()), "joins must be sorted by index");
#ifdef WITH_STATE_COUNTERS
        if (num_joins_)
            base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
#endif
    }

    /** Copy c'tor. */
    explicit EdgesBottomUp(const EdgesBottomUp &other)
        : num_joins_to_goal_(other.num_joins_to_goal_)
        , g_(other.g_)
        , num_joins_(other.num_joins_)
        , joins_(allocator_.allocate(num_joins_))
    {
        M_insist(bool(joins_));
        M_insist(std::is_sorted(cbegin(), cend()), "joins must be sorted by index");
        std::copy(other.begin(), other.end(), this->begin());
#ifdef WITH_STATE_COUNTERS
        if (num_joins_)
            base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
#endif
    }

    /** Move c'tor. */
    EdgesBottomUp(EdgesBottomUp &&other) : EdgesBottomUp()
    { swap(*this, other); }
    /** Assignment. */
    EdgesBottomUp & operator=(EdgesBottomUp other) { swap(*this, other); return *this; }

    /** D'tor. */
    ~EdgesBottomUp() {
#ifdef WITH_STATE_COUNTERS
        if (num_joins_) {
            M_insist(bool(joins_));
            base_type::INCREMENT_NUM_STATES_DISPOSED();
        }
#endif
        allocator_.deallocate(joins_, num_joins_);
    }

    /*----- Factory methods. -----------------------------------------------------------------------------------------*/

    static EdgesBottomUp CreateInitial(const QueryGraph &G, const AdjacencyMatrix&) {
        return EdgesBottomUp(G.num_sources() - 1, 0, nullptr);
    }

    /*----- Getters --------------------------------------------------------------------------------------------------*/

    template<typename PlanTable>
    bool is_goal(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                 const CardinalityEstimator&) const
    {
        return num_joins() == num_joins_to_goal_;
    }
    double g() const { return g_; }
    void decrease_g(double new_g) const { M_insist(new_g <= g_); g_ = new_g; }
    size_type num_joins() const { return num_joins_; }
    size_type num_subproblems(const QueryGraph &G) const {
        M_insist(num_joins() < G.num_sources());
        return G.num_sources() - num_joins();
    }
    size_type num_joins_to_goal() const { return num_joins_to_goal_; }
    unsigned operator[](std::size_t idx) const { M_insist(idx < num_joins()); return joins_[idx]; }

    /*----- Iteration ------------------------------------------------------------------------------------------------*/

    iterator begin() { return joins_; };
    iterator end() { return begin() + num_joins(); }
    const_iterator begin() const { return joins_; };
    const_iterator end() const { return begin() + num_joins(); }
    const_iterator cbegin() const { return begin(); };
    const_iterator cend() const { return end(); }

    template<typename Callback>
    void for_each_subproblem(Callback &&callback, const QueryGraph &G) const {
        Subproblem subproblems[G.num_sources()];
        uint8_t datasource_to_subproblem[G.num_sources()];
        compute_datasource_to_subproblem_index(G, subproblems, datasource_to_subproblem);

        for (Subproblem *it = subproblems, *end = subproblems + G.num_sources(); it != end; ++it) {
            if (not it->empty())
                callback(*it);
        }
    }

    /*----- Comparison -----------------------------------------------------------------------------------------------*/

    /** Returns `true` iff `this` and `other` have the exact same joins. */
    bool operator==(const EdgesBottomUp &other) const {
        if (this->num_joins() != other.num_joins()) return false;
        M_insist(this->num_joins() == other.num_joins());
        auto this_it = this->cbegin();
        auto other_it = other.cbegin();
        for (; this_it != this->cend(); ++this_it, ++other_it) {
            if (*this_it != *other_it)
                return false;
        }
        return true;
    }

    bool operator!=(const EdgesBottomUp &other) const { return not operator==(other); }

    /*----- Edge calculations ----------------------------------------------------------------------------------------*/

    /** Computes the `Subproblem`s produced by the `Join`s of this state in `subproblems`.  Simultaniously, buils a
     * reverse index that maps from `DataSource` ID to `Subproblem` in `datasource_to_subproblem`.  */
    void compute_datasource_to_subproblem_index(const QueryGraph &G, Subproblem *subproblems,
                                                uint8_t *datasource_to_subproblem) const
    {
        /*----- Initialize datasource_to_subproblem reverse index. -----*/
        for (std::size_t idx = 0; idx != G.num_sources(); ++idx) {
            new (&datasource_to_subproblem[idx]) int8_t(idx);
            new (&subproblems[idx]) Subproblem(1UL << idx);
        }

        /*----- Update index with joins performed so far. -----*/
        for (unsigned join_idx : *this) {
            const Join *join = G.joins()[join_idx];
            const auto &sources = join->sources();
            const unsigned left_idx  = datasource_to_subproblem[sources[0]->id()];
            const unsigned right_idx = datasource_to_subproblem[sources[1]->id()];
            subproblems[left_idx] |= subproblems[right_idx];
            for (auto id : subproblems[right_idx])
                datasource_to_subproblem[id] = left_idx;
            subproblems[right_idx] = Subproblem();
        }

#ifndef NDEBUG
        for (std::size_t idx = 0; idx != G.num_sources(); ++idx) {
            Subproblem S = subproblems[datasource_to_subproblem[idx]];
            M_insist(not S.empty(), "reverse index must never point to an empty subproblem");
        }
        Subproblem All((1UL << G.num_sources()) - 1UL);
        Subproblem combined;
        unsigned num_subproblems_nonempty = 0;
        for (std::size_t idx = 0; idx != G.num_sources(); ++idx) {
            Subproblem S = subproblems[idx];
            if (S.empty()) continue;
            ++num_subproblems_nonempty;
            M_insist((S & combined).empty(), "current subproblem must be disjoint from all others");
            combined |= S;
        }
        M_insist(num_subproblems(G) == num_subproblems_nonempty);
        M_insist(All == combined, "must not loose a relation");
#endif
    }


    /*----- Debugging ------------------------------------------------------------------------------------------------*/
M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const EdgesBottomUp &S) {
        out << "g = " << S.g() << ", [";
        for (auto it = S.cbegin(); it != S.cend(); ++it) {
            if (it != S.cbegin()) out << ", ";
            out << *it;
        }
        return out << ']';
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
};

EdgesBottomUp::allocator_type EdgesBottomUp::allocator_;

}

namespace std {

template<>
struct hash<search_states::EdgesBottomUp>
{
    uint64_t operator()(const search_states::EdgesBottomUp &state) const {
        /* Rolling hash with multiplier taken from [1] where the moduli is 2^64.
         * [1] http://www.ams.org/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf */
        uint64_t hash = 0;
        for (unsigned join_idx : state) {
            hash = hash ^ join_idx;
            hash = hash * 1181783497276652981UL + 4292484099903637661UL;
        }
        return hash;
    }
};

}


/*----------------------------------------------------------------------------------------------------------------------
 * Expansions
 *--------------------------------------------------------------------------------------------------------------------*/

namespace expansions {

using namespace search_states;

struct ExpandBottomUpComplete
{
    template<typename Callback, typename PlanTable>
    void operator()(const SubproblemsBottomUp &state, Callback &&callback, PlanTable &PT,
                    const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                    const CardinalityEstimator &CE) const
    {
        state.INCREMENT_NUM_STATES_EXPANDED();

        /* Enumerate all potential join pairs and check whether they are connected. */
        for (auto outer_it = state.cbegin(), outer_end = std::prev(state.cend()); outer_it != outer_end; ++outer_it)
        {
            const auto neighbors = M.neighbors(*outer_it);
            for (auto inner_it = std::next(outer_it); inner_it != state.cend(); ++inner_it) {
                M_insist(uint64_t(*inner_it) > uint64_t(*outer_it), "subproblems must be sorted");
                M_insist((*outer_it & *inner_it).empty(), "subproblems must not overlap");
                if (neighbors & *inner_it) { // inner and outer are joinable.
                    /* Compute joined subproblem. */
                    const Subproblem joined = *outer_it | *inner_it;

                    /* Compute new subproblems after join */
                    auto subproblems = state.get_allocator().allocate(state.size() - 1);
                    Subproblem *ptr = subproblems;
                    for (auto it = state.cbegin(); it != state.cend(); ++it) {
                        if (it == outer_it) continue; // skip outer
                        else if (it == inner_it) new (ptr++) Subproblem(joined); // replace inner
                        else new (ptr++) Subproblem(*it);
                    }
                    M_insist(std::is_sorted(subproblems, subproblems + state.size() - 1, subproblem_lt));

                    /* Compute total cost. */
                    cnf::CNF condition; // TODO use join condition
                    const double total_cost = CF.calculate_join_cost(G, PT, CE, *outer_it, *inner_it, condition);
                    PT.update(G, CE, *outer_it, *inner_it, total_cost);

                    /* Compute action cost. */
                    const double action_cost = total_cost - (PT[*outer_it].cost + PT[*inner_it].cost);

                    /* Create new search state. */
                    SubproblemsBottomUp S(state.g() + action_cost, state.size() - 1, std::move(subproblems));
                    state.INCREMENT_NUM_STATES_GENERATED();
                    callback(std::move(S));
                }
            }
        }
    }

    template<typename Callback, typename PlanTable>
    void operator()(const EdgesBottomUp &state, Callback &&callback, PlanTable &PT,
                    const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                    const CardinalityEstimator &CE) const
    {
        state.INCREMENT_NUM_STATES_EXPANDED();
        M_insist(std::is_sorted(state.cbegin(), state.cend()), "joins must be sorted by index");

        /*----- Compute datasource to subproblem reverse index. -----*/
        Subproblem subproblems[G.num_sources()];
        uint8_t datasource_to_subproblem[G.num_sources()];
        state.compute_datasource_to_subproblem_index(G, subproblems, datasource_to_subproblem);

        /*----- Initialize join matrix. -----*/
        const unsigned size_of_join_matrix = G.num_sources() * (G.num_sources() - 1) / 2;
        ///> stores for each combination of subproblems whether they were joint yet
        bool join_matrix[size_of_join_matrix];
        std::fill_n(join_matrix, size_of_join_matrix, false);

        auto joined = [&G, size_of_join_matrix](bool *matrix, unsigned row, unsigned col) -> bool& {
            const unsigned x = std::min(row, col);
            const unsigned y = std::max(row, col);
            M_insist(y < G.num_sources());
            M_insist(x < y);
            M_insist(y * (y - 1) / 2 + x < size_of_join_matrix);
            return matrix[ y * (y - 1) / 2 + x ];
        };

        /*----- Enumerate all joins that can still be performed. -----*/
        unsigned *joins = static_cast<unsigned*>(alloca(sizeof(unsigned[state.num_joins() + 1])));
        std::copy(state.cbegin(), state.cend(), joins + 1);
        std::size_t j = 0;
        joins[j] = 0;
        M_insist(std::is_sorted(joins, joins + state.num_joins() + 1));

        /*---- Find the first gap in the joins. -----*/
        while (j < state.num_joins() and joins[j] == joins[j+1]) {
            ++j;
            ++joins[j];
        }
        M_insist(j == state.num_joins() or joins[j] < joins[j+1]);
        M_insist(std::is_sorted(joins, joins + state.num_joins() + 1));

        for (;;) {
            M_insist(j <= state.num_joins(), "j out of bounds");
            M_insist(j == state.num_joins() or joins[j] < joins[j+1], "invalid position of j");

            const Join *join = G.joins()[joins[j]];
            const auto &sources = join->sources();

            /*----- Check whether the join is subsumed by joins in `state`. -----*/
            const unsigned left  = datasource_to_subproblem[sources[0]->id()];
            const unsigned right = datasource_to_subproblem[sources[1]->id()];
            if (left == right) // the data sources joined already belong to the same subproblem
                goto next;

            /*----- Check whether the join is subsumed by a previously considered join. -----*/
            if (bool &was_joined_before = joined(join_matrix, left, right); was_joined_before)
                goto next;
            else
                was_joined_before = true; // this will be joined now

            /*----- This is a feasible join.  Compute the successor state. -----*/
            {
                M_insist(std::is_sorted(joins, joins + state.num_joins() + 1));
                M_insist(not subproblems[left].empty());
                M_insist(not subproblems[right].empty());

                /*----- Compute total cost. -----*/
                cnf::CNF condition; // TODO use join condition
                const double total_cost = CF.calculate_join_cost(G, PT, CE, subproblems[left], subproblems[right], condition);
                PT.update(G, CE, subproblems[left], subproblems[right], total_cost);

                /* Compute action cost. */
                const double action_cost = total_cost - (PT[subproblems[left]].cost + PT[subproblems[right]].cost);

                EdgesBottomUp S(state.num_joins_to_goal(), state.g() + action_cost, joins,
                                           joins + state.num_joins() + 1);
                state.INCREMENT_NUM_STATES_GENERATED();
                callback(std::move(S));
            }

next:
            /*----- Advance to next join. -----*/
            ++joins[j];
            while (j < state.num_joins() and joins[j] == joins[j+1]) {
                ++j;
                ++joins[j];
            }
            if (joins[j] == G.num_joins())
                break;
        };
    }
};

}


/*----------------------------------------------------------------------------------------------------------------------
 * Heuristics
 *--------------------------------------------------------------------------------------------------------------------*/

namespace heuristics {

using namespace search_states;
using namespace expansions;

/** This heuristic implements a perfect oracle, always returning the exact distance to the nearest goal state. */
template<typename PlanTable, typename State>
struct perfect_oracle
{
    using state_type = State;

    perfect_oracle(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction &CF,
                   const CardinalityEstimator&)
    {
        DPccp{}(G, CF, PT); // pre-compute costs
    }

    double operator()(const state_type &state, const PlanTable &PT, const QueryGraph&, const AdjacencyMatrix&,
                      const CostFunction&, const CardinalityEstimator&) const
    {
        // TODO calculate distance based on pre-filled PlanTable
        (void) state;
        (void) PT;
        return 0;
    }
};

/** This heuristic estimates the distance from a state to the nearest goal state as the sum of the sizes of all
 * `Subproblem`s yet to be joined.
 * This heuristic is admissible, yet dramatically underestimates the actual distance to a goal state.
 */
template<typename PlanTable, typename State>
struct sum
{
    using state_type = State;

    sum(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        double distance = 0;
        if (not state.is_goal(PT, G, M, CF, CE)) {
            state.for_each_subproblem([&](Subproblem S) {
                distance += CE.predict_cardinality(*PT[S].model);
            }, G);
        }
        return distance;
    }
};

template<typename PlanTable, typename State>
struct scaled_sum;


template<typename PlanTable>
struct scaled_sum<PlanTable, SubproblemsBottomUp>
{
    using state_type = SubproblemsBottomUp;

    scaled_sum(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
               const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        double distance = 0;
        double cardinalities[G.num_sources()];
        std::size_t num_sources = 0;

        if (not state.is_goal(PT, G, M, CF, CE)) {
            state.for_each_subproblem([&](Subproblem S) {
                cardinalities[num_sources++] = CE.predict_cardinality(*PT[S].model);
            }, G);
            std::sort(cardinalities, cardinalities + num_sources, std::greater<double>());
            for (std::size_t i = 0; i != num_sources - 1; ++i)
                distance += (i+1) * cardinalities[i];
            distance += (num_sources-1) * cardinalities[num_sources - 1];
        }

        return distance;
    }
};

/** This heuristic estimates the distance from a state to the nearest goal state as the product of the sizes of all
 * `Subproblem`s yet to be joined.
 * This heuristic is not admissible and dramatically overestimates the actual distance to a goal state. Serves as a
 * starting point for development of other heuristics that try to estimate the cost by making the margin of error for
 * the overestimation smaller.
 */
template<typename PlanTable, typename State>
struct product
{
    using state_type = State;

    product(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, const PlanTable &PT, const QueryGraph&, const AdjacencyMatrix&,
                      const CostFunction&, const CardinalityEstimator &CE) const
    {
        if (state.size() > 1) {
            double distance = 1;
            for (auto s : state)
                distance *= CE.predict_cardinality(*PT[s].model);
            return distance;
        }
        return 0;
    }
};

template<typename PlanTable, typename State>
struct bottomup_lookahead_cheapest
{
    using state_type = State;

    bottomup_lookahead_cheapest(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                                const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        if (state.is_goal(PT, G, M, CF, CE)) return 0;

        double distance = 0;
        for (auto s : state)
            distance += CE.predict_cardinality(*PT[s].model);

        /* If there are only 2 subproblems left, the result of joining the two is the goal; no lookahead needed. */
        if (state.size() == 2)
            return distance;

        /* Look for least additional costs. */
        double min_additional_costs = std::numeric_limits<decltype(min_additional_costs)>::infinity();
        Subproblem min_subproblem_left;
        Subproblem min_subproblem_right;
        for (auto outer_it = state.cbegin(); outer_it != state.cend(); ++outer_it) {
            const auto neighbors = M.neighbors(*outer_it);
            for (auto inner_it = std::next(outer_it); inner_it != state.cend(); ++inner_it) {
                M_insist(uint64_t(*inner_it) > uint64_t(*outer_it), "subproblems must be sorted");
                M_insist((*outer_it & *inner_it).empty(), "subproblems must not overlap");
                if (neighbors & *inner_it) { // inner and outer are joinable.
                    const Subproblem joined = *outer_it | *inner_it;
                    if (not PT[joined].model) {
                        PT[joined].model = CE.estimate_join(G, *PT[*outer_it].model, *PT[*inner_it].model,
                                                            /* TODO */ cnf::CNF{});
                    }
                    cnf::CNF condition; // TODO use join condition
                    const double total_cost = CF.calculate_join_cost(G, PT, CE, *outer_it, *inner_it, condition);
                    const double action_cost = total_cost - (PT[*outer_it].cost + PT[*inner_it].cost);
                    ///> XXX: Sum of different units: cost and cardinality
                    const double additional_costs = action_cost + CE.predict_cardinality(*PT[joined].model);
                    if (additional_costs < min_additional_costs) {
                        min_subproblem_left = *outer_it;
                        min_subproblem_right = *inner_it;
                        min_additional_costs = additional_costs;
                    }
                }
            }
        }
        distance += min_additional_costs;
        distance -= CE.predict_cardinality(*PT[min_subproblem_left].model);
        distance -= CE.predict_cardinality(*PT[min_subproblem_right].model);
        return distance;
    }
};

/** This heuristic estimates the distance to the goal by calculating certain checkpoints on the path from the goal state
 * to the current state and searching for plans between those checkpoints.
 * Each checkpoint is a `Subproblem` containing a certain number of relations that has the minimal size for that number
 * of relations, while still being reachable from the current state and still being able to reach the next checkpoint.
 * The last checkpoint is the goal state, while the first search is from the current state to the first checkpoint.
 * The heuristic value then is the sum of the results of the executed searches.
 * This heuristic is not admissible and depends highly on the strategy for chosing checkpoints.
 */
template<typename PlanTable, typename State>
struct checkpoints
{ };

/*----- Specialization for SubproblemsBottomUp ------------------------------------------------------------*/
template<typename PlanTable>
struct checkpoints<PlanTable, SubproblemsBottomUp>
{
    using state_type = SubproblemsBottomUp;

    ///> the distance between two checkpoints, i.e. the difference in the number of relations contained in two
    ///> consecutive checkpoints
    static constexpr unsigned CHECKPOINT_DISTANCE = 3;

    /** This heuristic estimates the distance from a state to the nearest goal state as the sum of the sizes of all
     * `Subproblem`s yet to be joined.
     * This heuristic is admissible, yet dramatically underestimates the actual distance to a goal state.  */
    using internal_hsum = sum<PlanTable, state_type>;

    /** Represents one specific search done in the checkpoints heurisitc with its `initial_state` and `goal`.  Used to
     * determine which searches have already been conducted and thus need not be conducted again but can be loaded from
     * cache instead.  */
    struct SearchSection
    {
        private:
        state_type initial_state_;
        Subproblem goal_;

        public:
        SearchSection(state_type state, Subproblem goal)
            : initial_state_(std::move(state))
            , goal_(goal)
        { }

        const state_type & initial_state() const { return initial_state_; }
        Subproblem goal() const { return goal_; }

        bool operator==(const SearchSection &other) const {
            return this->goal() == other.goal() and this->initial_state() == other.initial_state();
        }
        bool operator<(const SearchSection &other) const {
            return this->goal() <  other.goal() or
                  (this->goal() == other.goal() and this->initial_state() < other.initial_state());
        }
    };

    struct SearchSectionHash
    {
        uint64_t operator()(const SearchSection &section) const {
            using std::hash;
            return hash<state_type>{}(section.initial_state()) * 0x100000001b3UL ^ uint64_t(section.goal());
        }
    };


    /** This map caches the estimated costs for searches already performed. */
    std::unordered_map<SearchSection, double, SearchSectionHash> cached_searches_;

    ai::AStar<
        state_type,
        internal_hsum,
        ExpandBottomUpComplete,
        /*----- context ----- */
        PlanTable&,
        const QueryGraph&,
        const AdjacencyMatrix&,
        const CostFunction&,
        const CardinalityEstimator&
    > S_;

    std::vector<std::pair<SmallBitset, SmallBitset>> worklist_;

    public:
    checkpoints(PlanTable&, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction&,
                const CardinalityEstimator&)
    {
        worklist_.reserve(G.sources().size());
    }

    checkpoints(const checkpoints&) = delete;
    checkpoints(checkpoints&&) = default;

    ~checkpoints() { }

    checkpoints & operator=(checkpoints&&) = default;

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE)
    {
        if (state.is_goal(PT, G, M, CF, CE)) return 0;

        /*----- Compute checkpoints for this `state`. ----------------------------------------------------------------*/
        std::vector<Subproblem> checkpoints;
        checkpoints.reserve(state.size() / CHECKPOINT_DISTANCE);
        if (state.size() > CHECKPOINT_DISTANCE) {
            std::vector<Subproblem> subproblems(state.begin(), state.end());

            /*----- Compute checkpoints on the path from `state` to goal. --------------------------------------------*/
            do {
                /* Compute `AdjacencyMatrix` of subproblems. */
                AdjacencyMatrix Msub(subproblems.size());
                for (std::size_t i = 0; i != subproblems.size() - 1U; ++i) {
                    for (std::size_t j = i + 1; j != subproblems.size(); ++j) {
                        const bool c = M.is_connected(subproblems[i], subproblems[j]);
                        Msub(i, j) = Msub(j, i) = c;
                    }
                }

                /* Enumerate all feasible compositions of `CHECKPOINT_DISTANCE` many subproblems. */
                Subproblem next_checkpoint;
                std::size_t size_of_next_checkpoint = -1UL;
#ifndef NDEBUG
                std::unordered_set<uint64_t> duplicate_check;
#endif

                worklist_.clear();
                for (std::size_t i = 0; i != subproblems.size(); ++i) {
                    M_insist(worklist_.empty());
                    const SmallBitset I(1UL << i);
                    worklist_.emplace_back(I, I.singleton_to_lo_mask());

                    while (not worklist_.empty()) {
                        auto [S, X] = worklist_.back();
                        worklist_.pop_back();

                        auto add_to_worklist = [&, this](const SmallBitset S, const SmallBitset X) {
                            worklist_.emplace_back(S, X);
#ifndef NDEBUG
                            M_insist(duplicate_check.insert(uint64_t(S)).second, "must not generate duplicates");
#endif
                        };

                        if (S.size() == CHECKPOINT_DISTANCE) { // checkpoint found
                            /* Compute actual checkpoint. */
                            Subproblem checkpoint;
                            for (auto idx : S)
                                checkpoint |= subproblems[idx];
                            M_insist(M.is_connected(checkpoint));

                            /* Calculate model, if necessary. */
                            if (not PT[checkpoint].model) [[unlikely]]
                                PT[checkpoint].model = CE.estimate_join_all(G, PT, checkpoint, cnf::CNF());
                            const std::size_t est_size = CE.predict_cardinality(*PT[checkpoint].model);
                            if (est_size < size_of_next_checkpoint) {
                                size_of_next_checkpoint = est_size;
                                next_checkpoint = checkpoint;
                            }
                        } else { // needs further subproblems
                            M_insist(S.size() >= 1U);
                            M_insist(S.size() < CHECKPOINT_DISTANCE);
                            const SmallBitset N = Msub.neighbors(S) - X;
                            const SmallBitset Xn = X | N;
                            const unsigned K = std::min<unsigned>(CHECKPOINT_DISTANCE - S.size(), N.size());

                            /*----- Handle singleton subsets. -----*/
                            for (auto it = N.begin(); it != N.end(); ++it) {
                                const SmallBitset sub = it.as_set();
                                M_insist((S & sub).empty());
                                M_insist(sub.is_subset(N));
                                add_to_worklist(S | sub, Xn);
                            }

                            /*----- Handle remaining subsets. -----*/
                            for (unsigned k = 2; k <= K; ++k) {
                                for (auto sub = SubsetEnumerator(N, k); sub; ++sub) {
                                    M_insist((S & *sub).empty());
                                    M_insist((*sub).is_subset(N));
                                    add_to_worklist(S | *sub, Xn);
                                }
                            }
                        }
                    }
                }

                M_insist(not next_checkpoint.empty(), "if there are more subproblems than CHECKPOINT_DISTANCE, "
                                                      "we *must* find at least one checkpoint");

                checkpoints.emplace_back(next_checkpoint);
                auto it = subproblems.begin();
                while (it != subproblems.end()) {
                    if (it->is_subset(next_checkpoint))
                        it = subproblems.erase(it);
                    else
                        ++it;
                }
                subproblems.insert(
                    std::upper_bound(subproblems.begin(), subproblems.end(), next_checkpoint, subproblem_lt),
                    next_checkpoint
                );
            } while (subproblems.size() > CHECKPOINT_DISTANCE);

        }
        /* The final *checkpoint* is the goal state. */
        checkpoints.emplace_back(Subproblem((1UL << G.sources().size()) - 1UL)); // goal state

        /*----- Perform searches towards checkpoints.  ---------------------------------------------------------------*/
        ///> the heuristic value determined by connecting checkpoints
        double h_checkpoints = 0;
        {
            ///> the subproblems in the current state
            std::vector<Subproblem> current_subproblems_unpruned(state.cbegin(), state.cend());
            ///> the subproblems relevant for reaching the next checkpoint
            std::vector<Subproblem> subproblems_current_checkpoint;
            subproblems_current_checkpoint.reserve(state.size());
            ///> the available subproblems for the next checkpoint (must be pruned again)
            std::vector<Subproblem> subproblems_later_checkpoints;
            subproblems_later_checkpoints.reserve(state.size());

            /* Process checkpoints. */
            for (auto it = checkpoints.begin(); it != checkpoints.end(); ++it) {
                const Subproblem checkpoint = *it;
                subproblems_later_checkpoints.clear();
                subproblems_current_checkpoint.clear();

                /* Split available subproblems in those for current checkpoint and later checkpoints. */
                for (auto S : current_subproblems_unpruned) {
                    // Only add subproblems to next initial state, if they are not needed for the checkpoint
                    if (S.is_subset(checkpoint))
                        subproblems_current_checkpoint.emplace_back(S);
                    else
                        subproblems_later_checkpoints.emplace_back(S);
                }
                M_insist(std::is_sorted(subproblems_current_checkpoint.begin(), subproblems_current_checkpoint.end(),
                                      subproblem_lt));
                M_insist(std::is_sorted(subproblems_later_checkpoints.begin(), subproblems_later_checkpoints.end(),
                                      subproblem_lt));

                /* The current checkpoints becomes a subproblem for reaching later checkpoints. */
                const auto pos = std::upper_bound(subproblems_later_checkpoints.cbegin(),
                                                  subproblems_later_checkpoints.cend(),
                                                  checkpoint,
                                                  subproblem_lt);
                subproblems_later_checkpoints.insert(pos, checkpoint);

                M_insist(std::is_sorted(subproblems_current_checkpoint.begin(), subproblems_current_checkpoint.end(),
                                      subproblem_lt));
                M_insist(std::is_sorted(subproblems_later_checkpoints.begin(), subproblems_later_checkpoints.end(),
                                      subproblem_lt));

                /* Construct initial state for local search to next checkpoint. */
                state_type initial_state(0, subproblems_current_checkpoint.begin(),
                                            subproblems_current_checkpoint.end());
                SearchSection cache_candidate(std::move(initial_state), checkpoint);

                if (auto it = cached_searches_.find(cache_candidate); it != cached_searches_.end()) {
                    h_checkpoints += it->second;
                } else {
                    internal_hsum h(PT, G, M, CF, CE);
                    const double cost = S_.search(state_type(cache_candidate.initial_state()), // clone
                                                  h, ExpandBottomUpComplete{}, PT, G, M, CF, CE);
                    S_.clear();
                    h_checkpoints += cost;
                    cached_searches_.emplace_hint(it, std::move(cache_candidate), cost);
                }

#ifdef WITH_GREEDY_BUSHY
                if (h_checkpoints >= h_greedy_bushy)
                    return h_greedy_bushy; // abort search
#endif

                using std::swap;
                swap(current_subproblems_unpruned, subproblems_later_checkpoints);
            }
        }

#ifdef WITH_GREEDY_BUSHY
        M_insist(h_checkpoints < h_greedy_bushy);
#endif
        return h_checkpoints;
    }
};

}

template<typename State, typename Heuristic, typename Expand, typename... Context>
using AStar = ai::AStar<State, Heuristic, Expand, Context...>;
template<typename State, typename Heuristic, typename Expand, typename... Context>
using wAStar = ai::wAStar<std::ratio<2, 1>>::type<State, Heuristic, Expand, Context...>;
template<typename State, typename Heuristic, typename Expand, typename... Context>
using lazyAStar = ai::lazy_AStar<State, Heuristic, Expand, Context...>;
template<typename State, typename Heuristic, typename Expand, typename... Context>
using beam_search = ai::beam_search<2>::type<State, Heuristic, Expand, Context...>;
template<typename State, typename Heuristic, typename Expand, typename... Context>
using dynamic_beam_search = ai::beam_search<-1U>::type<State, Heuristic, Expand, Context...>;
template<typename State, typename Heuristic, typename Expand, typename... Context>
using lazy_beam_search = ai::lazy_beam_search<2>::type<State, Heuristic, Expand, Context...>;
template<typename State, typename Heuristic, typename Expand, typename... Context>
using lazy_dynamic_beam_search = ai::lazy_beam_search<-1U>::type<State, Heuristic, Expand, Context...>;
template<typename State, typename Heuristic, typename Expand, typename... Context>
using monotone_beam_search = ai::monotone_beam_search<2>::type<State, Heuristic, Expand, Context...>;
template<typename State, typename Heuristic, typename Expand, typename... Context>
using monotone_dynamic_beam_search = ai::monotone_beam_search<-1U>::type<State, Heuristic, Expand, Context...>;

template<
    typename PlanTable,
    typename State,
    typename Heuristic,
    template<typename, typename, typename, typename...> typename Search
>
void run_heuristic_search(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                          const CardinalityEstimator &CE)
{
    State::RESET_STATE_COUNTERS();
    State initial_state = State::CreateInitial(G, M);
    try {
        Heuristic h = Heuristic(PT, G, M, CF, CE);
        ai::search<
            State,
            Heuristic,
            expansions::ExpandBottomUpComplete,
            Search,
            /*----- context -----*/
            PlanTable&,
            const QueryGraph&,
            const AdjacencyMatrix&,
            const CostFunction&,
            const CardinalityEstimator&
        >(std::move(initial_state), h, expansions::ExpandBottomUpComplete{}, PT, G, M, CF, CE);
    } catch (std::logic_error err) {
        std::cerr << "search did not reach a goal state, fall back to DPccp" << std::endl;
        DPccp dpccp;
        dpccp(G, CF, PT);
    }
#ifdef WITH_STATE_COUNTERS
    std::cerr <<   "States generated: " << State::NUM_STATES_GENERATED()
              << "\nStates expanded: " << State::NUM_STATES_EXPANDED()
              << "\nStates constructed: " << State::NUM_STATES_CONSTRUCTED()
              << "\nStates disposed: " << State::NUM_STATES_DISPOSED()
              << std::endl;
#endif
}

/** Computes the join order using heuristic search */
struct HeuristicSearch final : PlanEnumeratorCRTP<HeuristicSearch>
{
    using base_type = PlanEnumeratorCRTP<HeuristicSearch>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        Catalog &C = Catalog::Get();
        auto &CE = C.get_database_in_use().cardinality_estimator();
        AdjacencyMatrix M(G);

#define IS_HEURISTIC_SEARCH_CONFIG(STATE, HEURISTIC, SEARCH) \
        (streq(Options::Get().ai_state, #STATE) and streq(Options::Get().ai_heuristic, #HEURISTIC) and \
         streq(Options::Get().ai_search, #SEARCH))
#define EMIT_HEURISTIC_SEARCH_CONFIG(STATE, HEURISTIC, SEARCH) \
        if (IS_HEURISTIC_SEARCH_CONFIG(STATE, HEURISTIC, SEARCH)) \
        { \
            run_heuristic_search<\
                PlanTable, \
                search_states:: STATE,\
                heuristics:: HEURISTIC <PlanTable, search_states:: STATE>,\
                SEARCH>\
            (PT, G, M, CF, CE); \
        }

             EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  sum,                            AStar                           )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  sum,                            lazyAStar                       )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  sum,                            beam_search                     )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  sum,                            dynamic_beam_search             )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  sum,                            monotone_beam_search            )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  sum,                            monotone_dynamic_beam_search    )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  scaled_sum,                     AStar                           )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  scaled_sum,                     monotone_beam_search            )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  scaled_sum,                     monotone_dynamic_beam_search    )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  product,                        AStar                           )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  bottomup_lookahead_cheapest,    AStar                           )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  checkpoints,                    AStar                           )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  checkpoints,                    lazyAStar                       )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  checkpoints,                    beam_search                     )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  checkpoints,                    dynamic_beam_search             )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  checkpoints,                    lazy_beam_search                )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  checkpoints,                    lazy_dynamic_beam_search        )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  checkpoints,                    monotone_beam_search            )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  checkpoints,                    monotone_dynamic_beam_search    )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  perfect_oracle,                 AStar                           )
        else EMIT_HEURISTIC_SEARCH_CONFIG(SubproblemsBottomUp,  perfect_oracle,                 beam_search                     )
        else EMIT_HEURISTIC_SEARCH_CONFIG(EdgesBottomUp,        sum,                            AStar                           )
        else EMIT_HEURISTIC_SEARCH_CONFIG(EdgesBottomUp,        sum,                            beam_search                     )
        else EMIT_HEURISTIC_SEARCH_CONFIG(EdgesBottomUp,        sum,                            monotone_beam_search            )
        else EMIT_HEURISTIC_SEARCH_CONFIG(EdgesBottomUp,        sum,                            monotone_dynamic_beam_search    )
        else { throw std::invalid_argument("illegal search configuration"); }
#undef EMIT_HEURISTIC_SEARCH_CONFIG

#if 0
        {
            const auto ai_cost = PT.get_final().cost;
            DPccp dpccp;
            dpccp(G, CF, PT);
            const auto dp_cost = PT.get_final().cost;

            std::cerr << "AI: " << ai_cost << ", DP: " << dp_cost << ", Δ " << double(ai_cost) / dp_cost << 'x'
                      << std::endl;
        }
#endif
    }
};

#define M_PLAN_ENUMERATOR(NAME, _) \
    std::unique_ptr<PlanEnumerator> PlanEnumerator::Create ## NAME() { \
        return std::make_unique<NAME>(); \
    }
#include <mutable/tables/PlanEnumerator.tbl>
#undef M_PLAN_ENUMERATOR
