#include <mutable/util/AdjacencyMatrix.hpp>

#include <queue>


using namespace m;


/*======================================================================================================================
 * AdjacencyMatrix
 *====================================================================================================================*/

AdjacencyMatrix AdjacencyMatrix::transitive_closure_directed() const
{
    AdjacencyMatrix closure(*this); // copy

    bool changed;
    do {
        changed = false;
        for (std::size_t i = 0; i != num_vertices_; ++i) {
            for (std::size_t j = 0; j != num_vertices_; ++j) {
                if (not closure(i, j)) {
                    const SmallBitset row = closure.m_[i];
                    const SmallBitset col_mask = SmallBitset::Singleton(j);
                    for (std::size_t k : row) {
                        M_insist(row[k]);
                        if (closure.m_[k] & col_mask) {
                            closure(i, j) = true;
                            changed = true;
                            break;
                        }
                    }
                }
            }
        }
    } while (changed);

    return closure;
}

AdjacencyMatrix AdjacencyMatrix::transitive_closure_undirected() const
{
    AdjacencyMatrix closure(*this); // copy

    bool changed;
    do {
        changed = false;
        for (std::size_t i = 0; i != num_vertices_; ++i) {
            for (std::size_t j = i; j != num_vertices_; ++j) { // exploit symmetry
                M_insist(closure(i, j) == closure(j, i), "not symmetric");
                const bool before = closure(i, j);
                const SmallBitset row = closure.m_[i];
                const SmallBitset col = closure.m_[j]; // exploit symmetry
                const bool dot = not (row & col).empty(); // compute connected-ness as "dot product"
                closure(i, j) = closure(j, i) = dot;
                changed = changed or before != dot;
            }
        }
    } while (changed);

    return closure;
}

AdjacencyMatrix AdjacencyMatrix::minimum_spanning_forest(std::function<double(std::size_t, std::size_t)> weight) const
{
    AdjacencyMatrix MSF(num_vertices_);

    if (num_vertices_ == 0)
        return MSF;

    struct weighted_edge
    {
        std::size_t source, sink;
        double weight;

        weighted_edge() = default;
        weighted_edge(std::size_t source, std::size_t sink, double weight)
            : source(source), sink(sink), weight(weight)
        { }

        bool operator<(const weighted_edge &other) const { return this->weight < other.weight; }
        bool operator>(const weighted_edge &other) const { return this->weight > other.weight; }
    };
    std::priority_queue<weighted_edge, std::vector<weighted_edge>, std::greater<weighted_edge>> Q;

    /* Run Prim's algorithm for each remaining vertex to compute a MSF. */
    SmallBitset vertices_remaining = SmallBitset::All(num_vertices_);

    while (vertices_remaining) {
        SmallBitset next_vertex = vertices_remaining.begin().as_set();
        vertices_remaining = vertices_remaining - next_vertex;

        /* Prim's algorithm for finding a MST. */
        while (next_vertex) {
            /* Explore edges of `next_vertex`. */
            M_insist(next_vertex.is_singleton());
            const SmallBitset N = neighbors(next_vertex) & vertices_remaining;
            const std::size_t u = *next_vertex.begin();
            for (std::size_t v : N)
                Q.emplace(u, v, weight(u, v));

            /* Search for the cheapest edge not within the MSF. */
            weighted_edge E;
            do {
                if (Q.empty())
                    goto MST_done;
                E = Q.top();
                Q.pop();
            } while (not vertices_remaining[E.sink]);

            /* Add edge to MSF. */
            M_insist(vertices_remaining[E.sink], "sink must not yet be in the MSF");
            vertices_remaining[E.sink] = false;
            MSF(E.source, E.sink) = MSF(E.sink, E.source) = true; // add undirected edge to MSF
            next_vertex = SmallBitset::Singleton(E.sink);
        }
MST_done: /* MST is complete */;
    }
    return MSF;
}

AdjacencyMatrix AdjacencyMatrix::tree_directed_away_from(SmallBitset root)
{
    M_insist(root.is_singleton());
    AdjacencyMatrix directed_tree(num_vertices_);
    SmallBitset V = root;
    SmallBitset X;
    while (not V.empty()) {
        X |= V;
        for (auto v : V)
            directed_tree.m_[v] = this->m_[v] - X;
        V = directed_tree.neighbors(V);
    }

    return directed_tree;
}

AdjacencyMatrix AdjacencyMatrix::merge_nodes(const std::size_t i, const std::size_t j) const
{
    M_insist(num_vertices_ > 1);
    M_insist(i < j);
    M_insist(i < num_vertices_);
    M_insist(j < num_vertices_);

    AdjacencyMatrix m_merged(num_vertices_ - 1);

    std::size_t row_new_idx = 0;
    for (std::size_t row_idx = 0; row_idx < num_vertices_; ++row_idx) {
        SmallBitset S = m_[row_idx];
        //std::cout << "Current row: ";
        //S.print_fixed_length(std::cout, num_vertices_);
        //std::cout << std::endl;

        if (row_idx == i) {
            // merge both nodes ``vertically''
            S |= m_[j];
            // set both `i` and `j` bit to zero to avoid self-edge in horizontal merge later
            S.at(i) = S.at(j) = false;
            //std::cout << "\tmerge vertically: ";
            //S.print_fixed_length(std::cout, num_vertices_);
            //std::cout << std::endl;
        } else if (row_idx == j) {
            // skip this row
            continue;
        }
        // merge both nodes ``horizontally''
        /* 1. set bit at pos `i` to S[i] | S[j]
         *      - isolate bit as pos `j` from `S` (mask_j)
         *      - shift this bit by (j - i) to the right
         *      - compute S | mask_j
         */
        SmallBitset mask_j = S & SmallBitset::Singleton(j);
        mask_j = mask_j >> (j - i);
        //std::cout << "\tmask_j: ";
        //mask_j.print_fixed_length(std::cout, num_vertices_);
        //std::cout << std::endl;
        S |= mask_j;
        //std::cout << "\tS after mask_j: ";
        //S.print_fixed_length(std::cout, num_vertices_);
        //std::cout << std::endl;
        /* 2. shift all bits left from `j` one position to the right
         *      - isolate all bits left from the bit at pos `j`
         *      - shift those bits one to the right (mask)
         *      - clear (set to 0) all bits left from the bit at pos `j` in `S` (including `j`)
         *      - compute S & mask
         */
        SmallBitset mask(~0 << (j + 1));
        //std::cout << "\tmask: ";
        //mask.print_fixed_length(std::cout, num_vertices_);
        //std::cout << std::endl;
        SmallBitset left = S & mask;
        left = left >> 1;
        //std::cout << "\tleft: ";
        //left.print_fixed_length(std::cout, num_vertices_);
        //std::cout << std::endl;


        mask = SmallBitset((1UL << j) - 1);
        //std::cout << "\tmask: ";
        //mask.print_fixed_length(std::cout, num_vertices_);
        //std::cout << std::endl;
        S &= mask;
        //std::cout << "\tS after mask: ";
        //S.print_fixed_length(std::cout, num_vertices_);
        //std::cout << std::endl;

        S |= left;
        //std::cout << "\tS after left: ";
        //S.print_fixed_length(std::cout, num_vertices_);
        //std::cout << std::endl;

        /* Set `merged_row` = `S`. */
        m_merged.m_[row_new_idx] = std::move(S);

        ++row_new_idx;
    }
    return m_merged;
}

M_LCOV_EXCL_START
void AdjacencyMatrix::dump(std::ostream &out) const { out << *this << std::endl; }
void AdjacencyMatrix::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
