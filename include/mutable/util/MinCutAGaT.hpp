#pragma once

#include <mutable/util/AdjacencyMatrix.hpp>
#include <mutable/util/ADT.hpp>
#include <mutable/util/macro.hpp>
#include <tuple>
#include <vector>


namespace m {

struct MinCutAGaT
{
    template<typename Callback>
    void min_cut_advanced_generate_and_test(const AdjacencyMatrix &M, Callback &&callback, const Subproblem S,
                                            const Subproblem C, const Subproblem X, const Subproblem T) const
    {
        M_insist(not S.empty());
        M_insist(not S.singleton());
        M_insist(M.is_connected(S));
        M_insist(T.is_subset(C));
        M_insist(C.is_subset(S));
        M_insist((C & X).empty());

        using queue_entry = std::tuple<Subproblem, Subproblem, Subproblem>;
        std::vector<queue_entry> worklist;
        worklist.emplace_back(C, X, T);

        while (not worklist.empty()) {
            auto [C, X, T] = worklist.back();
            worklist.pop_back();

            M_insist(C.is_subset(S));
            M_insist(M.is_connected(C));
            M_insist(T.is_subset(C));

            /*----- IsConnectedImp() check. -----*/
            const Subproblem N_T = (M.neighbors(T) & S) - C; // sufficient to check if neighbors of T are connected
            bool is_connected;
            if (N_T.size() <= 1) {
                is_connected = true; // trivial
            } else {
                const Subproblem n = N_T.begin().as_set(); // single, random vertex from the neighborhood of T
                const Subproblem reachable = M.reachable(n, S - C); // compute vertices reachable from n in S - C
                is_connected = N_T.is_subset(reachable); // if reachable vertices contain N_T, then S - C is connected
            }

            Subproblem T_tmp;
            if (is_connected) { // found ccp (C, S\C)
                M_insist(M.is_connected(S - C));
                M_insist(M.is_connected(C, S - C));
                callback(C, S - C);
            } else {
                M_insist(not M.is_connected(S - C) or not M.is_connected(C, S - C));
                T_tmp = C;
            }

            if (C.size() + 1 >= S.size()) continue;

            Subproblem X_tmp = X;
            const Subproblem N_C = (M.neighbors(C) & S) - X;
            for (auto it = N_C.begin(); it != N_C.end(); ++it) {
                const Subproblem v = it.as_set();
                worklist.emplace_back(C | v, X_tmp, T_tmp | v);
                X_tmp = X_tmp | v;
            }
        }
    }

    template<typename Callback>
    void partition(const AdjacencyMatrix &M, Callback &&callback, const Subproblem S) const {
        M_insist(not S.empty());
        M_insist(not S.singleton());
        const Subproblem C = S.begin().as_set();
        M_insist(not C.empty());
        min_cut_advanced_generate_and_test(
            /* Matrix=   */ M,
            /* Callback= */ std::forward<Callback>(callback),
            /* S=        */ S,
            /* C=        */ C,
            /* X=        */ Subproblem(),
            /* T=        */ C
        );
    }
};

}
