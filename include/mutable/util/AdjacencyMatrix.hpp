#pragma once

#include <array>
#include <mutable/util/ADT.hpp>


namespace m {

/** An adjacency matrix for a given query graph. Represents the join graph. */
struct M_EXPORT AdjacencyMatrix
{
    /** A proxy to access single entries in the `AdjacencyMatrix`. */
    template<bool C>
    struct Proxy
    {
        friend struct AdjacencyMatrix;

        static constexpr bool Is_Const = C;

        private:
        using reference_type = std::conditional_t<Is_Const, const AdjacencyMatrix&, AdjacencyMatrix&>;

        reference_type M_;
        std::size_t i_;
        std::size_t j_;

        Proxy(reference_type M, std::size_t i, std::size_t j) : M_(M) , i_(i), j_(j) {
            M_insist(i < M_.num_vertices_);
            M_insist(j < M_.num_vertices_);
        }
        Proxy(Proxy&&) = default;

        public:
        operator bool() const { return M_.m_[i_][j_]; }

        template<bool C_ = Is_Const>
        std::enable_if_t<not C_, Proxy&>
        operator=(bool val) { M_.m_[i_][j_] = val; return *this; }

        Proxy & operator=(const Proxy<false> &other) {
            static_assert(not C);
            return operator=(bool(other));
        }

        Proxy & operator=(const Proxy<true> &other) {
            static_assert(not C);
            return operator=(bool(other));
        }

        Proxy & operator=(Proxy<false> &&other) {
            static_assert(not C);
            return operator=(bool(other));
        }
        Proxy & operator=(Proxy<true> &&other) {
            static_assert(not C);
            return operator=(bool(other));
        }
    };

    private:
    std::array<SmallBitset, SmallBitset::CAPACITY> m_; ///< matrix entries
    std::size_t num_vertices_ = 0; ///< number of sources of the `QueryGraph` represented by this matrix

    public:
    AdjacencyMatrix() { }
    AdjacencyMatrix(std::size_t num_vertices) : num_vertices_(num_vertices) { }

    explicit AdjacencyMatrix(const AdjacencyMatrix&) = default;
    AdjacencyMatrix(AdjacencyMatrix&&) = default;

    AdjacencyMatrix & operator=(AdjacencyMatrix&&) = default;

    /** Returns the bit at position `(i, j)`.  Both `i` and `j` must be in bounds. */
    Proxy<true> operator()(std::size_t i, std::size_t j) const { return Proxy<true>(*this, i, j); }

    /** Returns the bit at position `(i, j)`.  Both `i` and `j` must be in bounds. */
    Proxy<false> operator()(std::size_t i, std::size_t j) { return Proxy<false>(*this, i, j); }

    Proxy<true> at(std::size_t i, std::size_t j) const {
        if (i >= num_vertices_ or j >= num_vertices_)
            throw out_of_range("index is out of bounds");
        return operator()(i, j);
    }

    Proxy<false> at(std::size_t i, std::size_t j) {
        if (i >= num_vertices_ or j >= num_vertices_)
            throw out_of_range("index is out of bounds");
        return operator()(i, j);
    }

    /** Computes the set of nodes reachable from `src`, i.e.\ the set of nodes reachable from any node in `src`. */
    SmallBitset reachable(SmallBitset src) const {
        SmallBitset R_old(0);
        SmallBitset R_new(src);
        for (;;) {
            auto R = R_new - R_old;
            if (R.empty()) goto exit;
            R_old = R_new;
            for (auto x : R)
                R_new |= m_[x]; // add all nodes reachable from node `x` to the set of reachable nodes
        }
exit:
        return R_new;
    }

    /** Computes the set of nodes in `S` reachable from `src`, i.e.\ the set of nodes in `S` reachable from any node in
     * `src`. */
    SmallBitset reachable(SmallBitset src, SmallBitset S) const {
        SmallBitset R_old(0);
        SmallBitset R_new(src & S);
        for (;;) {
            auto R = R_new - R_old;
            if (R.empty()) goto exit;
            R_old = R_new;
            for (auto x : R)
                R_new |= m_[x] & S; // add all nodes in `S` reachable from node `x` to the set of reachable nodes
        }
exit:
        return R_new;
    }

    /** Computes the neighbors of `S`.  Nodes from `S` are not part of the neighborhood. */
    SmallBitset neighbors(SmallBitset S) const {
        SmallBitset neighbors;
        for (auto it : S)
            neighbors |= m_[it];
        return neighbors - S;
    }

    /** Returns `true` iff the subproblem `S` is connected.  `S` is connected iff any node in `S` can reach all other
     * nodes of `S` using only nodes in `S`.  */
    bool is_connected(SmallBitset S) const { return reachable(S.begin().as_set(), S) == S; }

    /** Returns `true` iff there is at least one edge (join) between `left` and `right`. */
    bool is_connected(SmallBitset left, SmallBitset right) const {
        SmallBitset neighbors;
        /* Compute the neighbors of `right`. */
        for (auto it : right)
            neighbors = neighbors | m_[it];
        /* Intersect `left` with the neighbors of `right`.  If the result is non-empty, `left` and `right` are
         * immediately connected by a join. */
        return not (left & neighbors).empty();
    }

    /** Computes the *transitive closure* of this adjacency matrix.  That is, compute for each pair of vertices *(i, j)*
     * whether *j* can be reached from *i* by any finite path.  Treats edges as directed and hence does not exploit
     * symmetry.  */
    AdjacencyMatrix transitive_closure_directed() const;

    /** Computes the *transitive closure* of this adjacency matrix.  That is, compute for each pair of vertices *(i, j)*
     * whether *j* can be reached from *i* by any finite path.  Expects that this `AdjacencyMatrix` is symmetric, i.e.
     * that the original graph is undirected.  */
    AdjacencyMatrix transitive_closure_undirected() const;

    /** Enumerate all *connected subgraphs* (CSGs) of the graph induced by vertex super set `super`, starting only from
     * nodes in `source`. */
    void for_each_CSG_undirected(SmallBitset super, SmallBitset source, std::function<void(SmallBitset)> callback) const
    {
        source = source & super; // restrict sources to vertices in `super`

        std::deque<std::pair<SmallBitset, SmallBitset>> Q;
        for (auto it = source.begin(); it != source.end(); ++it) {
            const SmallBitset I = it.as_set();
            M_insist(I.singleton());
            Q.emplace_back(I, source & ~I.mask_to_lo()); // exclude larger sources

            while (not Q.empty()) {
                auto [S, X_old] = Q.front();
                Q.pop_front();

                callback(S);

                const SmallBitset N = (neighbors(S) & super) - X_old;
                const SmallBitset X_new = X_old | N;
                for (SmallBitset n = least_subset(N); bool(n); n = next_subset(n, N)) // enumerate 2^{neighbors of S}
                    Q.emplace_back(S | n, X_new);
            }
        }
    }

    /** Enumerate all *connected subgraphs* (CSGs) of the graph induced by vertex super set `super`.  Requires that this
     * matrix is symmetric. */
    void for_each_CSG_undirected(SmallBitset super, std::function<void(SmallBitset)> callback) const {
        for_each_CSG_undirected(super, super, std::move(callback));
    }

    /** Enumerate all pairs of *connected subgraphs* (CSGs) that are connected by at least one edge.  Requires that this
     * matrix is symmetric. */
    void for_each_CSG_pair_undirected(SmallBitset super, std::function<void(SmallBitset, SmallBitset)> callback) const {
        auto callback_CSG = [this, super, &callback](SmallBitset first) {
            auto callback_partial = [&callback, first](SmallBitset second) {
                callback(first, second);
            };
            const SmallBitset N_first = neighbors(first) & super;
            const SmallBitset super_second = (super - first) & first.hi().mask_to_lo();
            for_each_CSG_undirected(super_second, N_first, callback_partial);
        };
        for_each_CSG_undirected(super, callback_CSG);
    }

    /** Computes the minimum spanning forest for this graph.  Expects the graph to be undirected, meaning that the
     * `AdjacencyMatrix` must be symmetric. */
    AdjacencyMatrix minimum_spanning_forest(std::function<double(std::size_t, std::size_t)> weight) const;

    /** Computes an `AdjacencyMatrix` with all edges directed away from `root`.  Requires that the graph is a tree,
     * i.e.\ connected and acyclic. */
    AdjacencyMatrix tree_directed_away_from(SmallBitset root);

    /** Compares two `AdjacencyMatrix`s element-wise. */
    bool operator==(const AdjacencyMatrix &other) const {
        if (this->num_vertices_ != other.num_vertices_) return false;
        for (std::size_t i = 0; i != num_vertices_; ++i) {
            if (this->m_[i] != other.m_[i])
                return false;
        }
        return true;
    }
    bool operator!=(const AdjacencyMatrix &other) const { return not operator==(other); }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const AdjacencyMatrix &M) {
        M.print_fixed_length(out, M.num_vertices_);
        return out;
    }

    friend std::string to_string(const AdjacencyMatrix &M) {
        std::ostringstream oss;
        oss << M;
        return oss.str();
    }

    void print_fixed_length(std::ostream &out, unsigned length) const {
        M_insist(length <= SmallBitset::CAPACITY);
        out << "Adjacency Matrix";
        for (unsigned i = 0; i != length; ++i) {
            out << '\n';
            m_[i].print_fixed_length(out, length);
        }
    }

    void dump(std::ostream &out) const;
    void dump() const;
M_LCOV_EXCL_STOP
};

}
