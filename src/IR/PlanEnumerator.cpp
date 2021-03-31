#include "mutable/IR/PlanEnumerator.hpp"

#include "util/ADT.hpp"
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/util/fn.hpp>
#include <queue>
#include <globals.hpp>


using namespace m;


const std::unordered_map<std::string, PlanEnumerator::kind_t> PlanEnumerator::STR_TO_KIND = {
#define DB_PLAN_ENUMERATOR(NAME, _) { #NAME,  PlanEnumerator::PE_ ## NAME },
#include "mutable/tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR
};

std::unique_ptr<PlanEnumerator> PlanEnumerator::Create(PlanEnumerator::kind_t kind) {
    switch(kind) {
#define DB_PLAN_ENUMERATOR(NAME, _) case PE_ ## NAME: return Create ## NAME();
#include "mutable/tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR
    }
}

/*======================================================================================================================
 * DPsize
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming. */
struct DPsize final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const override;
};

void DPsize::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

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
                    PT.update(cf, *S1, *S2, OperatorKind::JoinOperator);
                }
            }
        }
    }
}

/*======================================================================================================================
 * DPsizeOpt
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming.  In addition to `DPsize`, applies the following
 * optimizations.  First, do not enumerate symmetric subproblems.  Second, in case both subproblems are of equal size,
 * consider only subproblems succeeding the first subproblem. */
struct DPsizeOpt final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const override;
};

void DPsizeOpt::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

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
                        PT.update(cf, *S1, *S2, OperatorKind::JoinOperator);
                        PT.update(cf, *S2, *S1, OperatorKind::JoinOperator);
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
                        PT.update(cf, *S1, *S2, OperatorKind::JoinOperator);
                        PT.update(cf, *S2, *S1, OperatorKind::JoinOperator);
                    }
                }
            }
        }
    }
}

/*======================================================================================================================
 * DPsizeSub
 *====================================================================================================================*/

/** Computes the join order using size-based dynamic programming. */
struct DPsizeSub final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const override;
};

void DPsizeSub::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

    /* Process all subplans of size greater than one. */
    for (std::size_t s = 2; s <= n; ++s) {
        for (auto S = GospersHack::enumerate_all(s, n); S; ++S) { // enumerate all subsets of size `s`
            if (not M.is_connected(*S)) continue; // not connected -> skip
            for (Subproblem O(least_subset(*S)); O != *S; O = Subproblem(next_subset(O, *S))) {
                Subproblem Comp = *S - O;
                insist(M.is_connected(O, Comp), "implied by S inducing a connected subgraph");
                if (not PT.has_plan(O)) continue; // not connected -> skip
                if (not PT.has_plan(Comp)) continue; // not connected -> skip
                PT.update(cf, O, Comp, OperatorKind::JoinOperator);
            }
        }
    }
}

/*======================================================================================================================
 * DPsub
 *====================================================================================================================*/

/** Computes the join order using subset-based dynamic programming. */
struct DPsub final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const override;
};

void DPsub::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    const std::size_t n = sources.size();
    const AdjacencyMatrix M(G);

    for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
        Subproblem S(i);
        if (S.size() == 1) continue; // no non-empty and strict subset of S -> skip
        if (not M.is_connected(S)) continue; // not connected -> skip
        for (Subproblem S1(least_subset(S)); S1 != S; S1 = Subproblem(next_subset(S1, S))) {
            Subproblem S2 = S - S1;
            insist(M.is_connected(S1, S2), "implied by S inducing a connected subgraph");
            if (not PT.has_plan(S1)) continue; // not connected -> skip
            if (not PT.has_plan(S2)) continue; // not connected -> skip
            PT.update(cf, S1, S2, OperatorKind::JoinOperator);
        }
    }
}

/*======================================================================================================================
 * DPsubOpt
 *====================================================================================================================*/

/** Computes the join order using subset-based dynamic programming.  In comparison to `DPsub`, do not enumerate
 * symmetric subproblems. */
struct DPsubOpt final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const override;
};

void DPsubOpt::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    const std::size_t n = sources.size();
    const AdjacencyMatrix M(G);

    for (std::size_t i = 1, end = 1UL << n; i < end; ++i) {
        Subproblem S(i);
        if (S.size() == 1) continue; // no non-empty and strict subset of S -> skip
        if (not M.is_connected(S)) continue;
        /* Compute break condition to avoid enumerating symmetric subproblems. */
        uint64_t offset = S.capacity() - __builtin_clzl(uint64_t(S));
        insist(offset != 0, "invalid subproblem offset");
        Subproblem limit(1UL << (offset - 1));
        for (Subproblem S1(least_subset(S)); S1 != limit; S1 = Subproblem(next_subset(S1, S))) {
            Subproblem S2 = S - S1; // = S \ S1;
            insist(M.is_connected(S1, S2), "implied by S inducing a connected subgraph");
            if (not PT.has_plan(S1)) continue; // not connected -> skip
            if (not PT.has_plan(S2)) continue; // not connected -> skip
            /* Exploit commutativity of join. */
            PT.update(cf, S1, S2, OperatorKind::JoinOperator);
            PT.update(cf, S2, S1, OperatorKind::JoinOperator);
        }
    }
}

/*======================================================================================================================
 * DPccp
 *====================================================================================================================*/

/** Computes the join order using connected subgraph complement pairs (CCP). */
struct DPccp final : PlanEnumerator
{
    /** For each connected subgraph (csg) `S1` of `G`, enumerate all complement connected subgraphs,
     * i.e.\ all csgs of `G - S1` that are connected to `S1`. */
    void enumerate_cmp(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &cf, PlanTable &PT,
                       Subproblem S1) const;
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const override;
};

void DPccp::enumerate_cmp(const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &cf, PlanTable &PT,
                          Subproblem S1) const
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
            PT.update(cf, S1, S, OperatorKind::JoinOperator);
            PT.update(cf, S, S1, OperatorKind::JoinOperator);

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

void DPccp::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    const std::size_t n = sources.size();
    const AdjacencyMatrix M(G);

    /* Process subgraphs in breadth-first order.  The queue contains pairs of connected subgraphs and the corresponding
     * set of nodes to exclude. */
    std::queue<std::pair<Subproblem, Subproblem>> Q;

    for (std::size_t i = n; i != 0; --i) {
        /* For a given single node subgraph, the node itself and all nodes with a lower ID are excluded. */
        Q.emplace(std::make_pair(Subproblem(1UL << (i - 1)), Subproblem ((1UL << i) - 1)));
        while (not Q.empty()) {
            auto [S, X] = Q.front();
            Q.pop();
            enumerate_cmp(G, M, cf, PT, S);

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

/*======================================================================================================================
 * TDbasic
 *====================================================================================================================*/

struct TDbasic final : PlanEnumerator
{
    void PlanGen(const QueryGraph &G, const CostFunction &cf, PlanTable &PT, const AdjacencyMatrix &M, Subproblem S)
        const;
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &Pt) const override;
};

void TDbasic::PlanGen(const QueryGraph &G, const CostFunction &cf, PlanTable &PT, const AdjacencyMatrix &M,
                      Subproblem S) const
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
                PlanGen(G, cf, PT, M, sub);
                PlanGen(G, cf, PT, M, complement);

                /* Update `PlanTable`. */
                PT.update(cf, sub, complement, OperatorKind::JoinOperator);
                PT.update(cf, complement, sub, OperatorKind::JoinOperator);
            }
        }
    }
}

void TDbasic::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

    PlanGen(G, cf, PT, M, Subproblem((1UL << n) - 1));
}

/*======================================================================================================================
 * TDMinCutAGaT
 *====================================================================================================================*/

struct TDMinCutAGaT final : PlanEnumerator
{
    struct queue_entry
    {
        Subproblem C;
        Subproblem X;
        Subproblem T;

        queue_entry(Subproblem C, Subproblem X, Subproblem T) : C(C), X(X), T(T) { }
    };
    void MinCutAGaT(const QueryGraph &G, const CostFunction &cf, PlanTable &PT, const AdjacencyMatrix &M, Subproblem S,
                    Subproblem C, Subproblem X, Subproblem T) const;
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const override;
};

void TDMinCutAGaT::MinCutAGaT(const QueryGraph &G, const CostFunction &cf, PlanTable &PT, const AdjacencyMatrix &M,
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
            MinCutAGaT(G, cf, PT, M, e.C, Subproblem(least_subset(e.C)), Subproblem(0), Subproblem(least_subset(e.C)));
            MinCutAGaT(G, cf, PT, M, cmpl, Subproblem(least_subset(cmpl)), Subproblem(0),
                       Subproblem(least_subset(cmpl)));

            /* Update `PlanTable`. */
            PT.update(cf, e.C, cmpl, OperatorKind::JoinOperator);
            PT.update(cf, cmpl, e.C, OperatorKind::JoinOperator);

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

void TDMinCutAGaT::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    auto &sources = G.sources();
    std::size_t n = sources.size();
    AdjacencyMatrix M(G);

    MinCutAGaT(G, cf, PT, M, Subproblem((1UL << n) - 1), Subproblem(1), Subproblem(0), Subproblem(1));
}


/*======================================================================================================================
 * AIPlanning
 *====================================================================================================================*/

/** Computes the join order using an AI Planning approach */
struct AIPlanning final : PlanEnumerator
{
    void operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const override;
};

void AIPlanning::operator()(const QueryGraph &G, const CostFunction &cf, PlanTable &PT) const
{
    DPccp *dummyPE = new DPccp();
    DPccp dummy = *dummyPE;
    dummy(G, cf, PT);
    delete dummyPE;
}

#define DB_PLAN_ENUMERATOR(NAME, _) \
    std::unique_ptr<PlanEnumerator> PlanEnumerator::Create ## NAME() { \
        return std::make_unique<NAME>(); \
    }
#include "mutable/tables/PlanEnumerator.tbl"
#undef DB_PLAN_ENUMERATOR
