#include <mutable/IR/PlanEnumerator.hpp>

#include <algorithm>
#include <cstring>
#include <execution>
#include <functional>
#include <iostream>
#include <iterator>
#include <memory>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/util/ADT.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/list_allocator.hpp>
#include <mutable/util/malloc_allocator.hpp>
#include <mutable/util/MinCutAGaT.hpp>
#include <queue>
#include <set>
#include <type_traits>
#ifdef __BMI2__
#include <x86intrin.h>
#endif


using namespace m;


/*======================================================================================================================
 * PEall
 *====================================================================================================================*/

/** Computes the join order by enumerating *all* join orders, including Cartesian products. */
struct PEall final : PlanEnumeratorCRTP<PEall>
{
    using base_type = PlanEnumeratorCRTP<PEall>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        const std::size_t n = sources.size();
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
            Subproblem S(i);
            if (S.size() == 1) continue; // skip
            /* Compute break condition to avoid enumerating symmetric subproblems. */
            uint64_t offset = S.capacity() - __builtin_clzl(uint64_t(S));
            M_insist(offset != 0, "invalid subproblem offset");
            Subproblem limit(1UL << (offset - 1UL));
            for (Subproblem S1(least_subset(S)); S1 != limit; S1 = Subproblem(next_subset(S1, S))) {
                Subproblem S2 = S - S1; // = S \ S1;
                M_insist(PT.has_plan(S1), "must have found the optimal plan for S1");
                M_insist(PT.has_plan(S2), "must have found the optimal plan for S2");
                /* Exploit commutativity of join. */
                cnf::CNF condition; // TODO use join condition
                PT.update(G, CE, CF, S1, S2, condition);
            }
        }
    }
};


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
        const AdjacencyMatrix &M = G.adjacency_matrix();
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
                        PT.update(G, CE, CF, *S1, *S2, condition);
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
        const AdjacencyMatrix &M = G.adjacency_matrix();
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
                            PT.update(G, CE, CF, *S1, *S2, condition);
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
                            PT.update(G, CE, CF, *S1, *S2, condition);
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
        const AdjacencyMatrix &M = G.adjacency_matrix();
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
                    PT.update(G, CE, CF, O, Comp, condition);
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
        const AdjacencyMatrix &M = G.adjacency_matrix();
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
                PT.update(G, CE, CF, S1, S2, condition);
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
        const AdjacencyMatrix &M = G.adjacency_matrix();
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
                PT.update(G, CE, CF, S1, S2, condition);
            }
        }
    }
};


/*======================================================================================================================
 * DPccp
 *====================================================================================================================*/

template<typename PlanTable>
void DPccp::operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const
{
    const AdjacencyMatrix &M = G.adjacency_matrix();
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();
    const Subproblem All((1UL << G.num_sources()) - 1UL);
    cnf::CNF condition; // TODO use join condition

    auto handle_CSG_pair = [&](const Subproblem left, const Subproblem right) {
        PT.update(G, CE, CF, left, right, condition);
    };

    M.for_each_CSG_pair_undirected(All, handle_CSG_pair);
}


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
                PT.update(G, CE, CF, joined, R, condition);
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
        const AdjacencyMatrix &M = G.adjacency_matrix();
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        /* Linearize the vertices. */
        const std::vector<std::size_t> linearization = linearize(PT, G, M, CF, CE);
        M_insist(linearization.size() == G.num_sources());

        /*----- Reconstruct the right-deep plan from the linearization. -----*/
        Subproblem right(1UL << linearization[0]);
        for (std::size_t i = 1; i < G.num_sources(); ++i) {
            const Subproblem left(1UL << linearization[i]);
            cnf::CNF condition; // TODO use join condition
            PT.update(G, CE, CF, left, right, condition);
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
        const AdjacencyMatrix &M = G.adjacency_matrix();
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
                        PT.update(G, CE, CF, left, right, condition);
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
                    PT.update(G, CE, CF, sub, complement, condition);
                }
            }
        }
    }

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        auto &sources = G.sources();
        std::size_t n = sources.size();
        const AdjacencyMatrix &M = G.adjacency_matrix();
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

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
        const AdjacencyMatrix &M = G.adjacency_matrix();
        auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

        auto handle_ccp = [&](const Subproblem first, const Subproblem second) -> void {
            auto recurse = [&](const Subproblem first, const Subproblem second, auto recurse) -> void {
                auto handle_ccp = std::bind(recurse, std::placeholders::_1, std::placeholders::_2, recurse);
                /*----- Solve recursively. -----*/
                if (not PT.has_plan(first))
                    MinCutAGaT{}.partition(M, handle_ccp, first);
                M_insist(PT.has_plan(first));
                if (not PT.has_plan(second))
                    MinCutAGaT{}.partition(M, handle_ccp, second);
                M_insist(PT.has_plan(second));

                /*----- Update `PlanTable`. -----*/
                cnf::CNF condition; // TODO use join condition
                PT.update(G, CE, CF, first, second, condition);
            };
            recurse(first, second, recurse);
        };

        if (G.num_sources() > 1) {
            const Subproblem All((1UL << G.num_sources()) - 1UL);
            MinCutAGaT{}.partition(M, handle_ccp, All);
        }
    }
};

/*======================================================================================================================
 * GOO
 *====================================================================================================================*/

template<typename PlanTable>
void m::GOO::operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const {
    const AdjacencyMatrix &M = G.adjacency_matrix();
    auto &CE = Catalog::Get().get_database_in_use().cardinality_estimator();

    /*----- Initialize subproblems and their neighbors. -----*/
    node nodes[G.num_sources()];
    for (std::size_t i = 0; i != G.num_sources(); ++i) {
        Subproblem S(1UL << i);
        Subproblem N = M.neighbors(S);
        nodes[i] = node(S, N);
    }

    /*----- Greedyly enumerate joins, thereby computing a plan. -----*/
    for_each_join([&](const Subproblem left, const Subproblem right){
        static cnf::CNF condition;
        PT.update(G, CE, CF, left, right, condition);
    }, PT, G, M, CF, CE, nodes, nodes + G.num_sources());
}

__attribute__((constructor(202)))
static void register_plan_enumerators()
{
    Catalog &C = Catalog::Get();
#define REGISTER(NAME, DESCRIPTION) \
    C.register_plan_enumerator(#NAME, std::make_unique<NAME>(), DESCRIPTION)
    REGISTER(DPccp,        "enumerates connected subgraph complement pairs"); // register DPccp first to be default
    REGISTER(DPsize,       "size-based subproblem enumeration");
    REGISTER(DPsizeOpt,    "optimized DPsize: does not enumerate symmetric subproblems");
    REGISTER(DPsizeSub,    "DPsize with enumeration of subset complement pairs");
    REGISTER(DPsub,        "subset-based subproblem enumeration");
    REGISTER(DPsubOpt,     "optimized DPsub: does not enumerate symmetric subproblems");
    REGISTER(GOO,          "Greedy Operator Ordering");
    REGISTER(IKKBZ,        "greedy algorithm by IK/KBZ, ordering joins by rank");
    REGISTER(LinearizedDP, "DP with search space linearization based on IK/KBZ");
    REGISTER(TDbasic,      "basic top-down join enumeration using generate-and-test partitioning");
    REGISTER(TDMinCutAGaT, "top-down join enumeration using minimal graph cuts and advanced generate-and-test partitioning");
    REGISTER(PEall,        "enumerates ALL join orders, inclding Cartesian products");
#undef REGISTER
}
