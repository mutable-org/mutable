#pragma once

#include <array>
#include <mutable/util/ADT.hpp>
#include <unordered_set>


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
        requires (not C_)
        Proxy & operator=(bool val) { M_.m_[i_][j_] = val; return *this; }

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

    /** Returns the bitset stored at position 'i'. */
    SmallBitset &operator[](std::size_t i) { return m_[i]; }

    /** Returns the number of sources represented by the matrix **/
    std::size_t size() const {
        return num_vertices_;
    }

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

    /** Returns `true` iff the represented join graph is cyclic.
     * An `AdjacencyMatrix` is cyclic iff the number of joins is equal to or higher than the number of data sources. */
    bool is_cyclic() const {
        /* Compute number of edges. */
        std::size_t num_joins = 0;
        for (std::size_t i = 0; i < num_vertices_ - 1; ++i)
            for (std::size_t j = i + 1; j < num_vertices_; ++j)
                num_joins += m_[i][j];
        return num_joins >= num_vertices_;
    }

    /** Computes the node with the highest degree, i.e. the node with the most edges, given the nodes in `S`. */
    std::size_t highest_degree_node(SmallBitset S) const {
        if (S.is_singleton())
            return (uint64_t) S;
        std::size_t highest_degree_id = *S.begin();
        for (auto it = ++S.begin(); it != S.end(); ++it)
            if (m_[*it].size() > m_[highest_degree_id].size())
                highest_degree_id = *it;
        return highest_degree_id;
    }

    /** Returns `true` iff the subproblem `S` is connected.
     * `S` is connected iff any node in `S` can reach all other nodes of `S` using only nodes in `S`.
     *  Assumes `AdjacencyMatrix` is symmetric, i.e. edges are undirected.  */
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

    /** Returns `true` if the graph would still be connected after removing the nodes specified in `removed` */
    bool is_connected_after_removal(SmallBitset removed, SmallBitset block) const {

        auto visited(removed);
        auto rec = [&](SmallBitset node, auto&& rec) -> void {
            visited |= node;
            auto allowed_neighbors = (neighbors(node) & block) - visited;
            for (size_t neighbor_id : allowed_neighbors) {
                auto neighbor = SmallBitset::Singleton(neighbor_id);
                if (not (visited & neighbor).empty()) continue;
                rec(neighbor, rec);
            }
        };
        /* Take any node from the remaining nodes */
        SmallBitset root = block - removed;
        if (root.empty()) {
            return true;
        }
        rec(root.hi() , rec);

        /* At the end, all nodes should be visited to be connected still. */
        return visited == block;
    }

    /** Finds all 2-vertex cuts within the block (biconnected component) of graph and stores them as Smallbitsets in the vector passed as argument.
     * Further, only nodes which are not already contained in `already_used` are considered to avoid joining more than
     * two relations together during the greedy phase at the beginning.
     * Note that this implementation is "inefficient" as of now, as it runs in O(n^2) compared to the existing, linear approaches
     * However, given the sizes of nodes that are typically considered during join enumerations, this is not a huge problem. */
    void find_two_vertex_cuts(std::vector<SmallBitset> &vertex_cuts, SmallBitset block, SmallBitset &already_used) {
        std::unordered_set<SmallBitset, SmallBitsetHash> visited_pairs;
        SmallBitset visited(already_used);
        auto rec = [this, &block, &visited_pairs, &vertex_cuts, &visited](SmallBitset node, auto&& rec) -> void {
            for (std::size_t neighbor_id : (block & neighbors(node)) - visited) {
                auto neighbor = SmallBitset::Singleton(neighbor_id);
                auto pair = node | neighbor;
                if (visited_pairs.contains(pair)) continue;
                if (not is_connected_after_removal(pair, block)) vertex_cuts.emplace_back(pair);
                visited_pairs.emplace(pair);
                rec(neighbor, rec);
            }
            visited |= node;
        };
        /* Take any node from the remaining nodes */
        SmallBitset root = block - already_used;
        if (root.empty()) {
            return;
        }
        rec(root.hi() , rec);
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
            M_insist(I.is_singleton());
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

    /** Compute the blocks (biconnected components) and cut vertices using the method described in */
    void compute_blocks_and_cut_vertices(std::vector<SmallBitset> &blocks, SmallBitset &cut_vertices, std::size_t min_size = 2) {

        M_insist(min_size >= 2);
        std::unordered_set<SmallBitset, SmallBitsetHash> visited;
        std::unordered_map<SmallBitset, std::size_t, SmallBitsetHash> depth;
        std::unordered_map<SmallBitset, std::size_t, SmallBitsetHash> low;
        std::unordered_map<SmallBitset, SmallBitset, SmallBitsetHash> parent;

        auto rec = [&min_size, &visited, &depth, &low, &parent, &blocks, &cut_vertices, this](SmallBitset current, std::size_t current_depth, auto&& rec) -> SmallBitset {
            visited.emplace(current);
            depth.emplace(current, current_depth);
            low.emplace(current, current_depth);
            SmallBitset current_block = current;
            std::size_t child_count = 0;

            /* Check all neighbors */
            for (auto neighbor_id: neighbors(current)) {
                SmallBitset neighbor = SmallBitset::Singleton(neighbor_id);

                /* Neighbor was not explored yet */
                if (not visited.contains(neighbor)) {
                    parent.emplace(neighbor, current);
                    SmallBitset child_block = rec(neighbor, current_depth + 1, rec);

                    if (child_block.size() >= (min_size - 1)) {
                        child_count += 1;
                    }

                    /* if the lowest connected depth of the neighbor is at least as high as the current depth, we found a component */
                    if (low[neighbor] >= depth[current]) {
                        child_block |= current;

                        /* Only add blocks that have a size of at least 2. */
                        if (child_block.size() >= min_size) {
                            blocks.emplace_back(child_block);
                            if (current != SmallBitset(1) or child_count > 1) cut_vertices |= current;
                        }
                    } else {
                        /* Add the block returned by the neighbor call to the current block. */
                        current_block |= child_block;
                    }
                    low[current] = std::min(low[current], low[neighbor]);
                } else if (neighbor != parent[current]) {
                    low[current] = std::min(low[current], depth[neighbor]);
                }
            }

            return current_block;
        };

        rec(SmallBitset(1), 0, rec);
    }

    /** Computes the minimum spanning forest for this graph.  Expects the graph to be undirected, meaning that the
     * `AdjacencyMatrix` must be symmetric. */
    AdjacencyMatrix minimum_spanning_forest(std::function<double(std::size_t, std::size_t)> weight) const;

    /** Computes an `AdjacencyMatrix` with all edges directed away from `root`.  Requires that the graph is a tree,
     * i.e.\ connected and acyclic. */
    AdjacencyMatrix tree_directed_away_from(SmallBitset root);

    /** Computes an `AdjacencyMatrix` that has the two nodes at index `i` and `j` merged, i.e. all edges previously
     * pointing to either node `i` or `j` now point to the merged node. The size of the adjacency matrix is reduced by
     * one. */
    AdjacencyMatrix merge_nodes(const std::size_t i, const std::size_t j) const;

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
