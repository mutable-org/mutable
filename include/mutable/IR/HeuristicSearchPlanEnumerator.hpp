#pragma once

#include <algorithm>
#include <array>
#include <boost/container/allocator.hpp>
#include <boost/container/node_allocator.hpp>
#include <boost/heap/binomial_heap.hpp>
#include <boost/heap/fibonacci_heap.hpp>
#include <boost/heap/pairing_heap.hpp>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <limits>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/IR/PlanEnumerator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/util/ADT.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/HeuristicSearch.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/MinCutAGaT.hpp>
#include <numeric>
#include <ratio>
#include <type_traits>
#include <utility>
#include <vector>


///> mutable namespace
namespace m {

///> plan enumerator namespace
namespace pe {

///> heuristic search namespace
namespace hs {

using Subproblem = SmallBitset;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

namespace {

///> order two subproblems lexicographically
inline bool subproblem_lt(Subproblem left, Subproblem right) { return uint64_t(left) < uint64_t(right); };

}

}
}
}


namespace m::pe::hs {

/*======================================================================================================================
 * Heuristic Search States
 *====================================================================================================================*/

///> heuristic search states namespace
namespace search_states {

// #define COUNTERS
#if !defined(NDEBUG) && !defined(COUNTERS)
#define COUNTERS
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
#ifdef COUNTERS
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

    const Actual * parent() const { return actual().parent(); }

    template<typename PlanTable>
    bool is_bottom(const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                   const CardinalityEstimator &CE) const
    {
        return actual().is_bottom(PT, G, M, CF, CE);
    }

    template<typename PlanTable>
    bool is_top(const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                const CardinalityEstimator &CE) const
    {
        return actual().is_top(PT, G, M, CF, CE);
    }

    /** Returns the cost to reach this state from the initial state. */
    double g() const { return actual().g(); }

    /*----- Setters --------------------------------------------------------------------------------------------------*/

    /** Reduces the *g* value of the state. */
    double decrease_g(const Actual *new_parent, double new_g) const { return actual().decrease_g(new_parent, new_g); }

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

#ifdef COUNTERS
template<typename Actual>
typename Base<Actual>::state_counters_t
Base<Actual>::state_counters_;
#endif

struct SubproblemsArray : Base<SubproblemsArray>
{
    using base_type = Base<SubproblemsArray>;
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

    private:
    mutable const SubproblemsArray *parent_ = nullptr;
    ///> the cost to reach this state from the initial state
    mutable double g_;
    ///> number of subproblems in this state
    size_type size_ = 0;
    ///> marked subproblem, used to avoid redundant paths
    Subproblem marked_;
    ///> array of subproblems
    Subproblem *subproblems_ = nullptr;

    /*----- The Big Four and a Half, copy & swap idiom ---------------------------------------------------------------*/
    public:
    friend void swap(SubproblemsArray &first, SubproblemsArray &second) {
        using std::swap;
        swap(first.parent_,      second.parent_);
        swap(first.g_,           second.g_);
        swap(first.size_,        second.size_);
        swap(first.marked_,      second.marked_);
        swap(first.subproblems_, second.subproblems_);
    }

    SubproblemsArray() = default;

    /** Creates a state with cost `g` and given `subproblems`. */
    template<typename PlanTable>
    SubproblemsArray(const PlanTable&, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction&,
                     const CardinalityEstimator&, const SubproblemsArray *parent, double g, size_type size,
                     Subproblem marked, Subproblem *subproblems)
        : parent_(parent)
        , g_(g)
        , size_(size)
        , marked_(marked)
        , subproblems_(subproblems)
    {
        M_insist(parent_ == nullptr or parent_->g() <= this->g(), "cannot have less cost than parent");
        M_insist(size == 0 or subproblems_);
        M_insist(size <= G.num_sources());
        M_insist(std::is_sorted(begin(), end(), subproblem_lt));
        base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
    }

    /** Copy c'tor. */
    SubproblemsArray(const SubproblemsArray&) = delete;

    template<typename PlanTable>
    SubproblemsArray(const SubproblemsArray &other, const PlanTable&, const QueryGraph&, const AdjacencyMatrix&,
                     const CostFunction&, const CardinalityEstimator&)
        : g_(other.g_)
        , size_(other.size_)
        , marked_(other.marked_)
        , subproblems_(allocator_.allocate(size_))
    {
        base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
        M_insist(subproblems_);
        std::copy(other.begin(), other.end(), this->begin());
    }

    /** Move c'tor. */
    SubproblemsArray(SubproblemsArray &&other) : SubproblemsArray() { swap(*this, other); }
    /** Assignment. */
    SubproblemsArray & operator=(SubproblemsArray other) { swap(*this, other); return *this; }

    /** D'tor. */
    ~SubproblemsArray() {
        if (subproblems_) base_type::INCREMENT_NUM_STATES_DISPOSED();
        allocator_.deallocate(subproblems_, size_);
    }

    /*----- Factory methods ------------------------------------------------------------------------------------------*/

    template<typename PlanTable>
    static SubproblemsArray Bottom(const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                                   const CostFunction &CF, const CardinalityEstimator &CE)
    {
        auto subproblems = allocator_.allocate(G.num_sources());
        for (uint64_t i = 0; i != G.num_sources(); ++i)
            subproblems[i] = Subproblem::Singleton(i);
        return SubproblemsArray(
            /* Context=     */ PT, G, M, CF, CE,
            /* parent=      */ nullptr,
            /* cost=        */ 0.,
            /* size=        */ G.num_sources(),
            /* marked=      */ Subproblem(1),
            /* subproblems= */ subproblems
        );
    }

    template<typename PlanTable>
    static SubproblemsArray Top(const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                                const CostFunction &CF, const CardinalityEstimator &CE)
    {
        const Subproblem All = Subproblem::All(G.num_sources());
        auto subproblems = allocator_.allocate(1);
        subproblems[0] = All;
        return SubproblemsArray(
            /* Context=     */ PT, G, M, CF, CE,
            /* parent=      */ nullptr,
            /* cost=        */ 0.,
            /* size=        */ 1,
            /* marked=      */ All,
            /* subproblems= */ subproblems
        );
    }

    /*----- Getters --------------------------------------------------------------------------------------------------*/

    const SubproblemsArray * parent() const { return parent_; }

    template<typename PlanTable>
    bool is_bottom(const PlanTable&, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction&,
                const CardinalityEstimator&) const
    {
        return size() == G.num_sources();
    }

    template<typename PlanTable>
    bool is_top(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                const CardinalityEstimator&) const
    {
        return size() <= 1;
    }

    double g() const { return g_; }
    Subproblem marked() const { return marked_; }
    Subproblem mark(const Subproblem new_marked) { const Subproblem old = marked_; marked_ = new_marked; return old; }
    void decrease_g(const SubproblemsArray *new_parent, double new_g) const {
        M_insist(new_parent != this);
        M_insist(new_parent->g() <= new_g);
        M_insist(new_g <= this->g());
        parent_ = new_parent;
        g_ = new_g;
    }
    size_type size() const { return size_; }
    Subproblem operator[](std::size_t idx) const { M_insist(idx < size_); return subproblems_[idx]; }

    template<typename PlanTable>
    static unsigned num_partitions(const PlanTable&, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction&,
                                   const CardinalityEstimator&)
    {
        return G.num_sources();
    }

    template<typename PlanTable>
    unsigned partition_id(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                          const CardinalityEstimator&) const
    {
        M_insist(size() > 0);
        return size() - 1;
    }

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
    bool operator==(const SubproblemsArray &other) const {
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

    bool operator!=(const SubproblemsArray &other) const { return not operator==(other); }

    bool operator<(const SubproblemsArray &other) const {
        return std::lexicographical_compare(cbegin(), cend(), other.cbegin(), other.cend(), subproblem_lt);
    }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const SubproblemsArray &S) {
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


struct SubproblemTableBottomUp : Base<SubproblemTableBottomUp>
{
    using base_type = Base<SubproblemTableBottomUp>;
    using allocator_type = boost::container::node_allocator<Subproblem>;
    using size_type = typename base_type::size_type;

    private:
    ///> class-wide allocator, used by all instances
    static allocator_type allocator_;

    public:
    ///> returns a reference to the class-wide allocator
    static allocator_type & get_allocator() { return allocator_; }

    private:
    ///> the cost to reach this state from the initial state
    mutable double g_;
    ///> number of subproblems in this state
    size_type size_ = 0;
    ///> the last formed subproblem
    Subproblem marker_;
    ///> array of subproblems
    Subproblem *table_ = nullptr;

    template<bool IsConst>
    struct the_iterator
    {
        static constexpr bool is_const = IsConst;

        using reference = std::conditional_t<is_const, const Subproblem&, Subproblem&>;
        using pointer = std::conditional_t<is_const, const Subproblem*, Subproblem*>;

        private:
        Subproblem *table_;
        Subproblem X_;
        size_type size_;
        size_type idx_;
        size_type unique_;

        public:
        the_iterator(Subproblem *table, size_type size, size_type idx, size_type unique)
            : table_(table) , size_(size) , idx_(idx), unique_(unique)
        { }

        the_iterator & operator++() {
            M_insist(unique_ < size_, "out of bounds");
            ++unique_; // count unique subproblems
            X_ |= table_[idx_++]; // remember unique subproblem
            if (unique_ < size_) // not at end
                while (table_[idx_].is_subset(X_)) ++idx_; // find next unique subproblem
            return *this;
        }

        bool operator==(the_iterator other) const {
            return this->table_ == other.table_ and this->unique_ == other.unique_;
        }

        bool operator!=(the_iterator other) const { return not operator==(other); }

        the_iterator operator++(int) { the_iterator clone = *this; operator++(); return clone; }

        reference operator*() const { return table_[idx_]; }
        pointer operator->() const { return &operator*(); }
    };
    public:
    using iterator = the_iterator<false>;
    using const_iterator = the_iterator<true>;


    /*----- The Big Four and a Half, copy & swap idiom ---------------------------------------------------------------*/
    public:
    friend void swap(SubproblemTableBottomUp &first, SubproblemTableBottomUp &second) {
        using std::swap;
        swap(first.g_,      second.g_);
        swap(first.size_,   second.size_);
        swap(first.marker_, second.marker_);
        swap(first.table_,  second.table_);
    }

    SubproblemTableBottomUp() = default;

    /** Creates a state with cost `g` and given `table`. */
    template<typename PlanTable>
    SubproblemTableBottomUp(const PlanTable&, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction&,
                        const CardinalityEstimator&, double g, size_type size, Subproblem marker, Subproblem *table)
        : g_(g)
        , size_(size)
        , marker_(marker)
        , table_(table)
    {
        M_insist(size == 0 or table);
        M_insist(size <= G.num_sources());
        base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
    }

    /** Creates an initial state. */
    template<typename PlanTable>
    SubproblemTableBottomUp(const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                        const CardinalityEstimator &CE)
        : SubproblemTableBottomUp(PT, G, M, CF, CE, 0., G.num_sources(), Subproblem(), allocator_.allocate(G.num_sources()))
    {
        for (uint64_t i = 0; i != G.num_sources(); ++i)
            table_[i] = Subproblem::Singleton(i);
    }

    /** Copy c'tor. */
    SubproblemTableBottomUp(const SubproblemTableBottomUp&) = delete;

    template<typename PlanTable>
    SubproblemTableBottomUp(const SubproblemTableBottomUp &other, const PlanTable&, const QueryGraph &G,
                            const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&)
        : g_(other.g_)
        , size_(other.size_)
        , marker_(other.marker_)
        , table_(allocator_.allocate(size_))
    {
        base_type::INCREMENT_NUM_STATES_CONSTRUCTED();
        M_insist(table_);
        std::copy_n(other.table_, G.num_sources(), this->table_);
    }

    /** Move c'tor. */
    SubproblemTableBottomUp(SubproblemTableBottomUp &&other) : SubproblemTableBottomUp() { swap(*this, other); }
    /** Assignment. */
    SubproblemTableBottomUp & operator=(SubproblemTableBottomUp other) { swap(*this, other); return *this; }

    /** D'tor. */
    ~SubproblemTableBottomUp() {
        if (table_) base_type::INCREMENT_NUM_STATES_DISPOSED();
        allocator_.deallocate(table_, size_);
    }

    /*----- Getters --------------------------------------------------------------------------------------------------*/

    template<typename PlanTable>
    bool is_goal(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                 const CardinalityEstimator&) const
    {
        return size() <= 1;
    }
    double g() const { return g_; }
    Subproblem marker() const { return marker_; }
    void decrease_g(double new_g) const { M_insist(new_g <= g_); g_ = new_g; }
    size_type size() const { return size_; }
    Subproblem & operator[](std::size_t idx) { M_insist(idx < size_); return table_[idx]; }
    const Subproblem & operator[](std::size_t idx) const { return table_[idx]; }

    template<typename PlanTable>
    static unsigned num_partitions(const PlanTable&, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction&,
                                   const CardinalityEstimator&)
    {
        return G.num_sources();
    }

    template<typename PlanTable>
    unsigned partition_id(const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                          const CardinalityEstimator&) const
    {
        M_insist(size() > 0);
        return size() - 1;
    }

    template<typename Callback>
    void for_each_subproblem(Callback &&callback, const QueryGraph&) const {
        for (Subproblem S : *this)
            callback(S);
    }

    /*----- Iteration ------------------------------------------------------------------------------------------------*/

    iterator begin() { return iterator(table_, size_, 0, 0); }
    iterator end() { return iterator(table_, size_, 0, size_); }
    const_iterator begin() const { return const_iterator(table_, size_, 0, 0); }
    const_iterator end() const { return const_iterator(table_, size_, 0, size_); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }

    /*----- Comparison -----------------------------------------------------------------------------------------------*/

    /** Returns `true` iff `this` and `other` have the exact same `Subproblem`s. */
    bool operator==(const SubproblemTableBottomUp &other) const {
        if (this->size() != other.size()) return false;
        M_insist(this->size() == other.size());

        Subproblem X;
        std::size_t i = 0, j = 0;
        while (i != size()) {
            Subproblem T = this->table_[j];
            if (T.is_subset(X)) { // subproblem already seen
                ++j;
                continue;
            }
            /* New unique subproblem, compare. */
            Subproblem O = this->table_[j];
            if (T != O) return false;
            ++i; // count unique subproblems
            X |= T; // remember unique subproblem
        }
        return true;
    }

    bool operator!=(const SubproblemTableBottomUp &other) const { return not operator==(other); }

    bool operator<(const SubproblemTableBottomUp&) const {
        M_unreachable("not implemented");
    }

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const SubproblemTableBottomUp &S) {
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
#ifdef COUNTERS
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
#ifdef COUNTERS
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
#ifdef COUNTERS
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
#ifdef COUNTERS
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
            new (&subproblems[idx]) Subproblem(Subproblem::Singleton(idx));
        }

        /*----- Update index with joins performed so far. -----*/
        for (unsigned join_idx : *this) {
            const Join &join = *G.joins()[join_idx];
            const auto &sources = join.sources();
            const unsigned left_idx  = datasource_to_subproblem[sources[0].get().id()];
            const unsigned right_idx = datasource_to_subproblem[sources[1].get().id()];
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
        Subproblem All = Subproblem::All(G.num_sources());
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


struct EdgePtrBottomUp : Base<EdgePtrBottomUp>
{
    using base_type = Base<EdgePtrBottomUp>;
    using size_type = typename base_type::size_type;

    private:
    template<bool IsConst>
    struct the_iterator
    {
        static constexpr bool is_const = IsConst;

        using pointer_type = std::conditional_t<is_const, const EdgePtrBottomUp*, EdgePtrBottomUp*>;
        using reference_type = std::conditional_t<is_const, const EdgePtrBottomUp&, EdgePtrBottomUp&>;

        private:
        pointer_type state_ = nullptr;

        public:
        the_iterator() = default;
        the_iterator(pointer_type state) : state_(state) { }

        bool operator==(the_iterator other) const { return this->state_ == other.state_; }
        bool operator!=(the_iterator other) const { return not operator==(other); }

        the_iterator & operator++() {
            M_notnull(state_);
            state_ = state_->parent();
            return *this;
        }

        the_iterator operator++(int) {
            the_iterator clone(this->state_);
            operator++();
            return clone;
        }

        pointer_type operator->() const { return M_notnull(state_); }
        reference_type operator*() const { return *M_notnull(state_); }
    };
    public:
    using iterator = the_iterator<false>;
    using const_iterator = the_iterator<true>;

    private:
    /*----- Scratchpad -----*/
    struct Scratchpad
    {
        friend void swap(Scratchpad &first, Scratchpad &second) {
            using std::swap;
            swap(first.owner,                    second.owner);
            swap(first.subproblems,              second.subproblems);
            swap(first.datasource_to_subproblem, second.datasource_to_subproblem);
        }

        const EdgePtrBottomUp *owner = nullptr;
        Subproblem *subproblems = nullptr;
        uint8_t *datasource_to_subproblem = nullptr;

        Scratchpad() = default;
        Scratchpad(const QueryGraph &G)
            : subproblems(new Subproblem[G.num_sources()])
            , datasource_to_subproblem(new uint8_t[G.num_sources()])
         { }

        Scratchpad(const Scratchpad&) = delete;
        Scratchpad(Scratchpad &&other) : Scratchpad() { swap(*this, other); }

        ~Scratchpad() {
            delete[] subproblems;
            delete[] datasource_to_subproblem;
            owner = nullptr;
            subproblems = nullptr;
            datasource_to_subproblem = nullptr;
        }

        Scratchpad & operator=(Scratchpad other) { swap(*this, other); return *this; }

        bool is_allocated() const { return subproblems != nullptr; }
        bool owned_by(const EdgePtrBottomUp *state) const { return owner == state; }

        void make_owner(const EdgePtrBottomUp *state, const QueryGraph &G) {
            M_insist(is_allocated());
            if (owned_by(state)) return; // nothing to be done
            owner = state;
            state->compute_datasource_to_subproblem_index(G, subproblems, datasource_to_subproblem);
        }
    };
    static Scratchpad scratchpad_;

    private:
    mutable double g_;
    const EdgePtrBottomUp *parent_ = nullptr;
    unsigned join_id_;
    unsigned num_joins_;

    /*----- The Big Four and a Half, copy & swap idiom ---------------------------------------------------------------*/
    public:
    friend void swap(EdgePtrBottomUp &first, EdgePtrBottomUp &second) {
        using std::swap;
        swap(first.g_,         second.g_);
        swap(first.parent_,    second.parent_);
        swap(first.join_id_,   second.join_id_);
        swap(first.num_joins_, second.num_joins_);
    }

    /** Creates an initial state. */
    EdgePtrBottomUp(const QueryGraph&)
        : g_(0)
        , num_joins_(0)
    {
        INCREMENT_NUM_STATES_CONSTRUCTED();
    }

    /** Creates a state with actual costs `g` and given `subproblems`. */
    EdgePtrBottomUp(const EdgePtrBottomUp *parent, unsigned num_joins, double g, unsigned join_id)
        : g_(g)
        , parent_(parent)
        , join_id_(join_id)
        , num_joins_(num_joins)
    {
        INCREMENT_NUM_STATES_CONSTRUCTED();
    }

    ~EdgePtrBottomUp() {
        if (parent_)
            INCREMENT_NUM_STATES_DISPOSED();
    }

    /** Copy c'tor. */
    EdgePtrBottomUp(const EdgePtrBottomUp&) = delete;
    /** Move c'tor. */
    EdgePtrBottomUp(EdgePtrBottomUp &&other) { swap(*this, other); }
    /** Assignment. */
    EdgePtrBottomUp & operator=(EdgePtrBottomUp other) { swap(*this, other); return *this; }

    /*----- Factory methods. -----------------------------------------------------------------------------------------*/

    static EdgePtrBottomUp CreateInitial(const QueryGraph &G, const AdjacencyMatrix&) {
        if (not scratchpad_.is_allocated())
            scratchpad_ = Scratchpad(G);
        return EdgePtrBottomUp(G);
    }

    /*----- Getters --------------------------------------------------------------------------------------------------*/

    template<typename PlanTable>
    bool is_goal(const PlanTable&, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction&,
                 const CardinalityEstimator&) const
    {
        return num_joins() == G.num_sources() - 1;
    }

    double g() const { return g_; }
    void decrease_g(double new_g) const { M_insist(new_g <= g_); g_ = new_g; }
    const EdgePtrBottomUp * parent() const { return parent_; }
    unsigned join_id() const { M_notnull(parent_); return join_id_; }
    unsigned num_joins() const { return num_joins_; }
    unsigned num_joins_remaining(const QueryGraph &G) const { return G.num_sources() - 1 - num_joins(); }

    /*----- Iteration ------------------------------------------------------------------------------------------------*/

    iterator begin() { return iterator(this); }
    iterator end()   { return iterator(); }
    const_iterator begin() const { return const_iterator(this); }
    const_iterator end()   const { return const_iterator(); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend()   const { return end(); }

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
    bool operator==(const EdgePtrBottomUp &other) const {
        if (this->num_joins() != other.num_joins())
            return false;

        /*----- Collect all joins of `this`. -----*/
        unsigned this_joins[num_joins()];
        auto ptr = this_joins;
        for (auto it = this->cbegin(); it->parent(); ++it, ++ptr)
            *ptr = it->join_id();

        /*----- Compare with joins of `other`. -----*/
        const auto end = this_joins + num_joins();
        for (auto it = other.cbegin(); it->parent(); ++it) {
            auto pos = std::find(this_joins, end, it->join_id());
            if (pos == end)
                return false;
        }
        return true;
    }

    bool operator!=(const EdgePtrBottomUp &other) const { return not operator==(other); }

    /*----- Edge calculations ----------------------------------------------------------------------------------------*/

    /** Computes the `Subproblem`s produced by the `Join`s of this state in `subproblems`.  Simultaniously, buils a
     * reverse index that maps from `DataSource` ID to `Subproblem` in `datasource_to_subproblem`.  */
    void compute_datasource_to_subproblem_index(const QueryGraph &G, Subproblem *subproblems,
                                                uint8_t *datasource_to_subproblem) const
    {
        /*----- Initialize datasource_to_subproblem reverse index. -----*/
        for (std::size_t idx = 0; idx != G.num_sources(); ++idx) {
            new (&datasource_to_subproblem[idx]) int8_t(idx);
            new (&subproblems[idx]) Subproblem(Subproblem::Singleton(idx));
        }

        /*----- Update index with joins performed so far. -----*/
        const EdgePtrBottomUp *runner = this;
        while (runner->parent()) {
            const Join &join = *G.joins()[runner->join_id()];
            const auto &sources = join.sources();
            const unsigned left_idx  = datasource_to_subproblem[sources[0].get().id()];
            const unsigned right_idx = datasource_to_subproblem[sources[1].get().id()];
            subproblems[left_idx] |= subproblems[right_idx];
            for (auto id : subproblems[right_idx])
                datasource_to_subproblem[id] = left_idx;
            subproblems[right_idx] = Subproblem();
            runner = runner->parent();
        }

#ifndef NDEBUG
        for (std::size_t idx = 0; idx != G.num_sources(); ++idx) {
            Subproblem S = subproblems[datasource_to_subproblem[idx]];
            M_insist(not S.empty(), "reverse index must never point to an empty subproblem");
        }
        Subproblem All = Subproblem::All(G.num_sources());
        Subproblem combined;
        for (std::size_t idx = 0; idx != G.num_sources(); ++idx) {
            Subproblem S = subproblems[idx];
            if (S.empty()) continue;
            M_insist((S & combined).empty(), "current subproblem must be disjoint from all others");
            combined |= S;
        }
        M_insist(All == combined, "must not loose a relation");
#endif
    }


    /*----- Debugging ------------------------------------------------------------------------------------------------*/
M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const EdgePtrBottomUp &S) {
        out << "g = " << S.g() << ", [";
        for (auto it = S.cbegin(); it->parent(); ++it) {
            if (it != S.cbegin()) out << ", ";
            out << it->join_id();
        }
        return out << ']';
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
};

}

}


namespace std {

template<>
struct hash<m::pe::hs::search_states::SubproblemsArray>
{
    uint64_t operator()(const m::pe::hs::search_states::SubproblemsArray &state) const {
        /* Rolling hash with multiplier taken from [1] where the moduli is 2^64.
         * [1] http://www.ams.org/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf */
        uint64_t hash = 0;
        for (m::pe::hs::Subproblem s : state) {
            hash = hash ^ uint64_t(s);
            hash = hash * 1181783497276652981UL + 4292484099903637661UL;
        }
        return hash;
    }
};

template<>
struct hash<m::pe::hs::search_states::SubproblemTableBottomUp>
{
    uint64_t operator()(const m::pe::hs::search_states::SubproblemTableBottomUp &state) const {
        /* Rolling hash with multiplier taken from [1] where the moduli is 2^64.
         * [1] http://www.ams.org/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf */
        uint64_t hash = 0;
        for (m::pe::hs::Subproblem s : state) {
            hash = hash ^ uint64_t(s);
            hash = hash * 1181783497276652981UL + 4292484099903637661UL;
        }
        return hash;
    }
};

template<>
struct hash<m::pe::hs::search_states::EdgesBottomUp>
{
    uint64_t operator()(const m::pe::hs::search_states::EdgesBottomUp &state) const {
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

template<>
struct hash<m::pe::hs::search_states::EdgePtrBottomUp>
{
    uint64_t operator()(const m::pe::hs::search_states::EdgePtrBottomUp &state) const {
        /*----- Collect all joins in sorted order. -----*/
        unsigned joins[state.num_joins()];
        unsigned *ptr = joins;
        for (auto it = state.cbegin(); it->parent(); ++it, ++ptr)
            *ptr = it->join_id();
        std::sort(joins, joins + state.num_joins());

        /* Rolling hash with multiplier taken from [1] where the moduli is 2^64.
         * [1] http://www.ams.org/mcom/1999-68-225/S0025-5718-99-00996-5/S0025-5718-99-00996-5.pdf */
        uint64_t hash = 0;
        for (auto it = joins; it != joins + state.num_joins(); ++it) {
            hash = hash ^ *it;
            hash = hash * 1181783497276652981UL + 4292484099903637661UL;
        }
        return hash;
    }
};

}


namespace m::pe::hs {

/*======================================================================================================================
 * Heuristic Search State Expansions
 *====================================================================================================================*/

///> heuristic search state expansions namespace
namespace expansions {

using namespace m::pe::hs;
using namespace m::pe::hs::search_states;


struct BottomUp
{
    template<typename State, typename PlanTable>
    static State Start(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                       const CardinalityEstimator &CE) {
        return State::Bottom(PT, G, M, CF, CE);
    }

    template<typename State, typename PlanTable>
    static bool is_goal(const State &state, const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                        const CostFunction &CF, const CardinalityEstimator &CE)
    {
        return state.is_top(PT, G, M, CF, CE);
    }

    template<typename State, typename PlanTable>
    void reset_marked(State &state, const PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                      const CardinalityEstimator&)
    {
        state.mark(Subproblem(1UL));
    }
};

struct BottomUpComplete : BottomUp
{
    using direction = BottomUp;
    using direction::is_goal;

    template<typename Callback, typename PlanTable>
    void operator()(const SubproblemsArray &state, Callback &&callback, PlanTable &PT,
                    const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                    const CardinalityEstimator &CE) const
    {
        state.INCREMENT_NUM_STATES_EXPANDED();
        M_insist(not is_goal(state, PT, G, M, CF, CE), "cannot expand goal");
#ifndef NDEBUG
        bool has_successor = false;
#endif

        const Subproblem marked = state.marked();
        const Subproblem All = Subproblem::All(G.num_sources());

        /* Enumerate all potential join pairs and check whether they are connected. */
        for (auto outer_it = state.cbegin(), outer_end = std::prev(state.cend()); outer_it != outer_end; ++outer_it)
        {
            const auto neighbors = M.neighbors(*outer_it);
            for (auto inner_it = std::next(outer_it); inner_it != state.cend(); ++inner_it) {
                M_insist(subproblem_lt(*outer_it, *inner_it), "subproblems must be sorted");
                M_insist((*outer_it & *inner_it).empty(), "subproblems must not overlap");

                if (subproblem_lt(*inner_it, marked)) // *inner_it < marked, implies *outer_it < marked
                    continue; // prune symmetrical paths

                if (neighbors & *inner_it) { // inner and outer are joinable.
                    /* Compute joined subproblem. */
                    const Subproblem joined = *outer_it | *inner_it;

                    /* Compute new subproblems after join */
                    Subproblem *subproblems = state.get_allocator().allocate(state.size() - 1);
                    Subproblem *ptr = subproblems;
                    for (auto it = state.cbegin(); it != state.cend(); ++it) {
                        if (it == outer_it) continue; // skip outer
                        else if (it == inner_it) new (ptr++) Subproblem(joined); // replace inner
                        else new (ptr++) Subproblem(*it);
                    }
                    M_insist(std::is_sorted(subproblems, subproblems + state.size() - 1, subproblem_lt));

                    /* Compute total cost. */
                    cnf::CNF condition; // TODO use join condition
                    if (not PT[joined].model) {
                        auto &model_left  = *PT[*outer_it].model;
                        auto &model_right = *PT[*inner_it].model;
                        PT[joined].model = CE.estimate_join(G, model_left, model_right, condition);
                        PT[joined].cost = 0;
                    }
                    /* The cost of the final join is always the size of the result set, and hence the same for all
                     * plans.  We therefore omit this cost, as otherwise goal states might be artificially postponed in
                     * the priority queue.   */
                    const double action_cost = joined == All ? 0 : CE.predict_cardinality(*PT[joined].model);

                    /* Create new search state. */
                    SubproblemsArray S(
                        /* Context=     */ PT, G, M, CF, CE,
                        /* parent=      */ &state,
                        /* g=           */ state.g() + action_cost,
                        /* size=        */ state.size() - 1,
                        /* marked=      */ joined,
                        /* subproblems= */ subproblems
                    );
                    state.INCREMENT_NUM_STATES_GENERATED();
                    callback(std::move(S));
#ifndef NDEBUG
                    has_successor = true;
#endif
                }
            }
        }
#ifndef NDEBUG
        M_insist(has_successor, "must expand to at least one successor");
#endif
    }

    template<typename Callback, typename PlanTable>
    void operator()(const SubproblemTableBottomUp &state, Callback &&callback, PlanTable &PT,
                    const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                    const CardinalityEstimator &CE) const
    {
        state.INCREMENT_NUM_STATES_EXPANDED();

        /*----- Enumerate all left sides. -----*/
        const Subproblem All = Subproblem::All(G.num_sources());
        Subproblem X;
        std::size_t i = 0;
        while (X != All) {
            const Subproblem L = state[i++];
            if (L.is_subset(X)) continue;
            X |= L; // remember left side

            Subproblem N = M.neighbors(L) - X;
            /*----- Enumerate all right sides of left side. -----*/
            while (not N.empty()) {
                auto it = N.begin();
                const Subproblem R = state[*it];
                N -= R;

                // M_insist(uint64_t(L) < uint64_t(R));
                M_insist(M.is_connected(L, R));
                M_insist((L & R).empty());

                /* Compute joined subproblem. */
                const Subproblem joined = L|R;

                if (subproblem_lt(joined, state.marker())) continue; // prune symmetrical paths

                /* Compute new subproblem table after join. */
                auto table = state.get_allocator().allocate(G.num_sources());

#if 1
                for (std::size_t j = 0; j != G.num_sources(); ++j)
                    table[j] = joined[j] ? joined : state[j];
#else
                std::copy_n(&state[0], G.num_sources(), table);
                for (std::size_t i : joined)
                    table[i] = joined;
#endif

                /* Compute total cost. */
                cnf::CNF condition; // TODO use join condition
                PT.update(G, CE, CF, L, R, condition);

                /* Compute action cost. */
                const double total_cost = CF.calculate_join_cost(G, PT, CE, L, R, condition);
                const double action_cost = total_cost - (PT[L].cost + PT[R].cost);

                /* Create new search state. */
                SubproblemTableBottomUp S(
                    /* Context= */ PT, G, M, CF, CE,
                    /* g=       */ state.g() + action_cost,
                    /* size=    */ state.size() - 1,
                    /* mark=    */ joined,
                    /* table_   */ table
                );
                state.INCREMENT_NUM_STATES_GENERATED();
                callback(std::move(S));
            }
        }
    }

    template<typename Callback, typename PlanTable>
    void operator()(const EdgesBottomUp &state, Callback &&callback, PlanTable &PT,
                    const QueryGraph &G, const AdjacencyMatrix&, const CostFunction &CF,
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

            const Join &join = *G.joins()[joins[j]];
            const auto &sources = join.sources();

            /*----- Check whether the join is subsumed by joins in `state`. -----*/
            const unsigned left  = datasource_to_subproblem[sources[0].get().id()];
            const unsigned right = datasource_to_subproblem[sources[1].get().id()];
            if (left == right) goto next; // the data sources joined already belong to the same subproblem

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
                PT.update(G, CE, CF, subproblems[left], subproblems[right], condition);

                /* Compute action cost. */
                const double total_cost = CF.calculate_join_cost(G, PT, CE, subproblems[left], subproblems[right], condition);
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

    template<typename Callback, typename PlanTable>
    void operator()(const EdgePtrBottomUp &state, Callback &&callback, PlanTable &PT,
                    const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                    const CardinalityEstimator &CE) const
    {
        state.INCREMENT_NUM_STATES_EXPANDED();

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

        /*----- Collect all joins of `state`. -----*/
        unsigned joins_taken[state.num_joins()];
        {
            unsigned *ptr = joins_taken;
            for (auto it = state.cbegin(); it->parent(); ++it, ++ptr)
                *ptr = it->join_id();
            std::sort(joins_taken, joins_taken + state.num_joins());
        }

        /*----- Enumerate all joins that can still be performed. -----*/
        unsigned *sweepline = joins_taken;
        for (std::size_t j = 0; j != G.num_joins(); ++j) {
            if (sweepline != joins_taken + state.num_joins() and j == *sweepline) {
                /*----- Join already present. -----*/
                ++sweepline;
                continue;
            }

            const Join &join = *G.joins()[j];
            const auto &sources = join.sources();

            /*----- Check whether the join is subsumed by joins in `state`. -----*/
            const unsigned left  = datasource_to_subproblem[sources[0].get().id()];
            const unsigned right = datasource_to_subproblem[sources[1].get().id()];
            if (left == right) continue; // the data sources joined already belong to the same subproblem

            /*----- Check whether the join is subsumed by a previously considered join. -----*/
            if (bool &was_joined_before = joined(join_matrix, left, right); was_joined_before)
                continue;
            else
                was_joined_before = true; // this will be joined now

            /*----- This is a feasible join.  Compute the successor state. -----*/
            M_insist(not subproblems[left].empty());
            M_insist(not subproblems[right].empty());
            M_insist(M.is_connected(subproblems[left], subproblems[right]));

            /*----- Compute total cost. -----*/
            cnf::CNF condition; // TODO use join condition
            PT.update(G, CE, CF, subproblems[left], subproblems[right], condition);

            /* Compute action cost. */
            const double total_cost = CF.calculate_join_cost(G, PT, CE, subproblems[left], subproblems[right], condition);
            const double action_cost = total_cost - (PT[subproblems[left]].cost + PT[subproblems[right]].cost);

            EdgePtrBottomUp S(/* parent=    */ &state,
                              /* num_joins= */ state.num_joins() + 1,
                              /* g=         */ state.g() + action_cost,
                              /* join_id=   */ j);
            state.INCREMENT_NUM_STATES_GENERATED();
            callback(std::move(S));
        }
    }
};


struct TopDown
{
    template<typename State, typename PlanTable>
    static State Start(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                       const CardinalityEstimator &CE) {
        return State::Top(PT, G, M, CF, CE);
    }

    template<typename State, typename PlanTable>
    static bool is_goal(const State &state, const PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                        const CostFunction &CF, const CardinalityEstimator &CE)
    {
        return state.is_bottom(PT, G, M, CF, CE);
    }

    template<typename State, typename PlanTable>
    void reset_marked(State &state, const PlanTable&, const QueryGraph &G, const AdjacencyMatrix&, const CostFunction&,
                      const CardinalityEstimator&)
    {
        const Subproblem All = Subproblem::All(G.num_sources());
        state.mark(All);
    }
};

struct TopDownComplete : TopDown
{
    using direction = TopDown;
    using direction::is_goal;

    template<typename Callback, typename PlanTable>
    void operator()(const SubproblemsArray &state, Callback &&callback, PlanTable &PT,
                    const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                    const CardinalityEstimator &CE) const
    {
        state.INCREMENT_NUM_STATES_EXPANDED();
        M_insist(not is_goal(state, PT, G, M, CF, CE), "cannot expand goal");
#ifndef NDEBUG
        bool has_successor = false;
#endif

        const Subproblem All = Subproblem::All(G.num_sources());
        auto enumerate_ccp = [&](Subproblem S1, Subproblem S2) {
            using std::swap;

            if (subproblem_lt(S2, S1)) swap(S1, S2);
            M_insist(subproblem_lt(S1, S2));

            /*----- Merge subproblems of state, excluding S1|S2, and partitions S1 and S2. -----*/
            Subproblem *subproblems = state.get_allocator().allocate(state.size() + 1);
            {
                Subproblem partitions[2] = { S1, S2 };
                auto left_it = state.cbegin(), left_end = state.cend();
                auto right_it = partitions, right_end = partitions + 2;
                auto out = subproblems;
                for (;;) {
                    M_insist(out <= subproblems + state.size() + 1, "out of bounds");
                    M_insist(left_it <= left_end, "out of bounds");
                    M_insist(right_it <= right_end, "out of bounds");
                    if (left_it == left_end) {
                        if (right_it == right_end) break;
                        *out++ = *right_it++;
                    } else if (*left_it == (S1|S2)) [[unlikely]] {
                        ++left_it; // skip partitioned subproblem S1|S2
                    } else if (right_it == right_end) {
                        *out++ = *left_it++;
                    } else if (subproblem_lt(*left_it, *right_it)) {
                        *out++ = *left_it++;
                    } else {
                        *out++ = *right_it++;
                    }
                }
                M_insist(out == subproblems + state.size() + 1);
                M_insist(left_it == left_end);
                M_insist(right_it == right_end);
            }
            M_insist(std::is_sorted(subproblems, subproblems + state.size() + 1, subproblem_lt));

            cnf::CNF condition; // TODO use join condition
            /* The cost of the final join is always the size of the result set, and hence the same for all plans.  We
             * therefore omit this cost, as otherwise goal states might be artificially postponed in the priority queue.
             * */
            double action_cost = 0;
            if ((S1|S2) != All) {
                if (not PT[S1|S2].model)
                    PT[S1|S2].model = CE.estimate_join_all(G, PT, S1|S2, condition);
                action_cost = CE.predict_cardinality(*PT[S1|S2].model);
            }

            /* Create new search state. */
            SubproblemsArray S(
                /* Context=     */ PT, G, M, CF, CE,
                /* parent=      */ &state,
                /* g=           */ state.g() + action_cost,
                /* size=        */ state.size() + 1,
                /* marked=      */ S1|S2,
                /* subproblems= */ subproblems
            );
            state.INCREMENT_NUM_STATES_GENERATED();
            callback(std::move(S));
#ifndef NDEBUG
            has_successor = true;
#endif
        };

        for (const Subproblem S : state) {
            if (not S.is_singleton()) { // partition only the first non-singleton
                MinCutAGaT{}.partition(M, enumerate_ccp, S);
                break;
            }
        }

#ifndef NDEBUG
        M_insist(has_successor, "must expand to at least one successor");
#endif
    }

};

}

}


namespace m::pe::hs {

/*======================================================================================================================
 * Heuristic Search Heuristics
 *====================================================================================================================*/

namespace heuristics {

using namespace m::pe::hs::search_states;
using namespace m::pe::hs::expansions;


template<typename PlanTable, typename State, typename Expand>
struct zero
{
    using state_type = State;

    static constexpr bool is_admissible = true;

    zero(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&)
    { }

    double operator()(const state_type&, PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                      const CardinalityEstimator&) const
    {
        return 0;
    }
};


/** This heuristic estimates the distance from a state to the nearest goal state as the sum of the sizes of all
 * `Subproblem`s yet to be joined.
 * This heuristic is admissible, yet dramatically underestimates the actual distance to a goal state. */
template<typename PlanTable, typename State, typename Expand>
struct sum;

template<typename PlanTable, typename State>
struct sum<PlanTable, State, BottomUp>
{
    using state_type = State;

    /** For bottom up, the sum heuristic is actually not admissible.  The sizes of the subproblems in the current state
     * can be significantly larger than the sizes of the join results, hence over-estimating the remaining cost. */
    static constexpr bool is_admissible = false;

    sum(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&) { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        double distance = 0;
        if (not state.is_top(PT, G, M, CF, CE)) {
            state.for_each_subproblem([&](const Subproblem S) {
                distance += CE.predict_cardinality(*PT[S].model);
            }, G);
        }
        return distance;
    }
};

template<typename PlanTable, typename State>
struct sum<PlanTable, State, TopDown>
{
    using state_type = State;

    static constexpr bool is_admissible = true;

    sum(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&) { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        static cnf::CNF condition; // TODO use join condition
        if (state.is_top(PT, G, M, CF, CE))
            return 0;
        double distance = 0;
        state.for_each_subproblem([&](const Subproblem S) {
            if (not S.is_singleton()) { // skip base relations
                if (not PT[S].model)
                    PT[S].model = CE.estimate_join_all(G, PT, S, condition);
                distance += CE.predict_cardinality(*PT[S].model);
            }
        }, G);
        return distance;
    }
};

template<typename PlanTable, typename State, typename Expand>
struct sum : sum<PlanTable, State, typename Expand::direction>
{
    using sum<PlanTable, State, typename Expand::direction>::sum; // forward base c'tor
};


template<typename PlanTable, typename State, typename Expand>
struct sqrt_sum;

template<typename PlanTable, typename State>
struct sqrt_sum<PlanTable, State, TopDown>
{
    using state_type = State;

    sqrt_sum(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix&,
                      const CostFunction&, const CardinalityEstimator &CE) const
    {
        static cnf::CNF condition; // TODO use join condition
        double distance = 0;
        state.for_each_subproblem([&](const Subproblem S) {
            if (not S.is_singleton()) { // skip base relations
                if (not PT[S].model)
                    PT[S].model = CE.estimate_join_all(G, PT, S, condition);
                distance += 2 * std::sqrt(CE.predict_cardinality(*PT[S].model));
            }
        }, G);
        return distance;
    }
};

template<typename PlanTable, typename State, typename Expand>
struct sqrt_sum : sqrt_sum<PlanTable, State, typename Expand::direction>
{
    using sqrt_sum<PlanTable, State, typename Expand::direction>::sqrt_sum; // forward base c'tor
};


template<typename PlanTable, typename State, typename Expand>
struct scaled_sum;

template<typename PlanTable, typename State>
struct scaled_sum<PlanTable, State, BottomUp>
{
    using state_type = State;

    scaled_sum(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        double distance = 0;
        double cardinalities[G.num_sources()];
        std::size_t num_sources = 0;

        if (not state.is_top(PT, G, M, CF, CE)) {
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

template<typename PlanTable, typename State, typename Expand>
struct scaled_sum : scaled_sum<PlanTable, State, typename Expand::direction>
{
    using scaled_sum<PlanTable, State, typename Expand::direction>::scaled_sum; // forward base c'tor
};


/** This heuristic estimates the distance from a state to the nearest goal state as the product of the sizes of all
 * `Subproblem`s yet to be joined.
 * This heuristic is not admissible and dramatically overestimates the actual distance to a goal state. Serves as a
 * starting point for development of other heuristics that try to estimate the cost by making the margin of error for
 * the overestimation smaller.
 */
template<typename PlanTable, typename State, typename Expand>
struct product;

template<typename PlanTable, typename State>
struct product<PlanTable, State, BottomUp>
{
    using state_type = State;

    product(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&) { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph&, const AdjacencyMatrix&,
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

template<typename PlanTable, typename State, typename Expand>
struct product : product<PlanTable, State, typename Expand::direction>
{
    using product<PlanTable, State, typename Expand::direction>::product;
};


template<typename PlanTable, typename State>
struct bottomup_lookahead_cheapest
{
    using state_type = State;

    bottomup_lookahead_cheapest(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&,
                                const CardinalityEstimator&)
    { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        if (state.is_top(PT, G, M, CF, CE)) return 0;

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


/** Inspired by GOO: Greedy Operator Ordering.  https://link.springer.com/chapter/10.1007/BFb0054528 */
template<typename PlanTable, typename State, typename Expand>
struct GOO;

template<typename PlanTable, typename State>
struct GOO<PlanTable, State, BottomUp>
{
    using state_type = State;

    GOO(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&) { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        using std::swap;

        /*----- Initialize nodes. -----*/
        m::pe::GOO::node nodes[G.num_sources()];
        std::size_t num_nodes = 0;
        state.for_each_subproblem([&](Subproblem S) {
            new (&nodes[num_nodes++]) m::pe::GOO::node(S, M.neighbors(S));
        }, G);

        /*----- Greedily enumerate all joins. -----*/
        const Subproblem All = Subproblem::All(G.num_sources());
        double cost = 0;
        m::pe::GOO{}.for_each_join([&](Subproblem left, Subproblem right) {
            static cnf::CNF condition; // TODO: use join condition
            if (All != (left|right)) {
                const double old_cost_left = std::exchange(PT[left].cost, 0);
                const double old_cost_right = std::exchange(PT[right].cost, 0);
                cost += CF.calculate_join_cost(G, PT, CE, left, right, condition);
                PT[left].cost = old_cost_left;
                PT[right].cost = old_cost_right;
            }
        }, PT, G, M, CF, CE, nodes, nodes + num_nodes);

        return cost;
    }
};

/** Inspired by GOO: Greedy Operator Ordering.  https://link.springer.com/chapter/10.1007/BFb0054528 */
template<typename PlanTable, typename State>
struct GOO<PlanTable, State, TopDown>
{
    using state_type = State;

    GOO(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&) { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M,
                      const CostFunction &CF, const CardinalityEstimator &CE) const
    {
        /*----- Initialize worklist from subproblems. -----*/
        std::vector<Subproblem> worklist;
        worklist.reserve(G.num_sources());
        worklist.insert(worklist.end(), state.cbegin(), state.cend());

        /*----- Greedily enumerate all joins. -----*/
        double cost = 0;
        m::pe::TDGOO{}.for_each_join([&](Subproblem left, Subproblem right) {
            cost += CE.predict_cardinality(*PT[left].model) + CE.predict_cardinality(*PT[right].model);
        }, PT, G, M, CF, CE, std::move(worklist));

        return cost;
    }
};

template<typename PlanTable, typename State, typename Expand>
struct GOO : GOO<PlanTable, State, typename Expand::direction>
{
    using GOO<PlanTable, State, typename Expand::direction>::GOO;
};


template<typename PlanTable, typename State, typename Expand>
struct avg_sel;

template<typename PlanTable, typename State>
struct avg_sel<PlanTable, State, BottomUp>
{
    using state_type = State;

    avg_sel(PlanTable&, const QueryGraph&, const AdjacencyMatrix&, const CostFunction&, const CardinalityEstimator&) { }

    double operator()(const state_type &state, PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix&,
                      const CostFunction&, const CardinalityEstimator &CE) const
    {
        using std::swap;
        if (state.size() <= 2) return 0;

        double cardinalities[state.size()];
        double *end;
        {
            double *ptr = cardinalities;
            for (const Subproblem S : state)
                *ptr++ = CE.predict_cardinality(*PT[S].model);
            end = ptr;
            std::sort(cardinalities, end); // sort cardinalities in ascending order
        }

        const Subproblem All = Subproblem::All(G.num_sources());
        if (not PT[All].model) {
            static cnf::CNF condition;
            PT[All].model = CE.estimate_join_all(G, PT, All, condition);
        }

        double Cprod = std::reduce(cardinalities, end, 1., std::multiplies<double>{});
        const double sel_remaining = CE.predict_cardinality(*PT[All].model) / Cprod;
        M_insist(sel_remaining <= 1.1);

        const std::size_t num_joins_remaining = state.size() - 1;
        const double avg_sel = std::pow(sel_remaining, 1. / num_joins_remaining);

        /*----- Keep joining the two smallest subproblems and accumulate the cost. -----*/
        double accumulated_cost = 0;
        for (auto ptr = cardinalities; ptr < end - 1; ++ptr) {
            const double card = ptr[0] * ptr[1] * avg_sel;
            accumulated_cost += card;
            *++ptr = card;
            for (auto runner = ptr; runner != end - 1 and runner[0] > runner[1]; ++runner)
                swap(runner[0], runner[1]);
        }

        return accumulated_cost;
    }
};

template<typename PlanTable, typename State, typename Expand>
struct avg_sel : avg_sel<PlanTable, State, typename Expand::direction>
{
    using avg_sel<PlanTable, State, typename Expand::direction>::avg_sel;
};

}

}


namespace m::pe::hs {

namespace config {

struct Fibonacci_heap
{
    template<typename Cmp>
    using compare = boost::heap::compare<Cmp>;

    template<typename T, typename... Options>
    using heap_type = boost::heap::fibonacci_heap<T, Options...>;

    template<typename T>
    using allocator_type = boost::container::node_allocator<T>;
};

template<long Num, long Denom = 1>
struct weight
{
    static constexpr float Weight = float(Num) / Denom;
};

template<long Num, long Denom = 1>
struct beam
{
    using BeamWidth = std::ratio<Num, Denom>;
};

template<bool B>
struct lazy
{
    static constexpr bool Lazy = B;
};

template<bool B>
struct monotone
{
    static constexpr bool IsMonotone = B;
};

template<bool B>
struct cost_based_pruning
{
    static constexpr bool PerformCostBasedPruning = B;
};

/** Combines multiple configuration parameters into a single configuration type. */
template<typename T, typename... Ts>
struct combine : T, combine<Ts...> { };

/** Combines multiple configuration parameters into a single configuration type.  This is the base case of the recursive
 * variadic template. */
template<typename T>
struct combine<T> : T { };


/*===== Pre-configured Search Strategies =============================================================================*/

#define DEFINE_SEARCH(NAME, ...) \
    template<typename State, typename Expand, typename Heuristic, typename... Context> \
    using NAME = ai::genericAStar<State, Expand, Heuristic, combine<__VA_ARGS__>, Context...>

DEFINE_SEARCH(AStar,                monotone<true>, Fibonacci_heap, weight<1>, lazy<false>, cost_based_pruning<false>, beam<0>);
DEFINE_SEARCH(lazyAStar,            monotone<true>, Fibonacci_heap, weight<1>, lazy<true>,  cost_based_pruning<false>, beam<0>);
DEFINE_SEARCH(beam_search,          monotone<true>, Fibonacci_heap, weight<1>, lazy<false>, cost_based_pruning<false>, beam<2>);
DEFINE_SEARCH(dynamic_beam_search,  monotone<true>, Fibonacci_heap, weight<1>, lazy<false>, cost_based_pruning<false>, beam<1, 5>);
DEFINE_SEARCH(AStar_with_cbp,       monotone<true>, Fibonacci_heap, weight<1>, lazy<false>, cost_based_pruning<true>,  beam<0>);
DEFINE_SEARCH(beam_search_with_cbp, monotone<true>, Fibonacci_heap, weight<1>, lazy<false>, cost_based_pruning<true>,  beam<2>);

#undef DEFINE_SEARCH

}

}


namespace m::pe::hs {

template<
    typename PlanTable,
    typename State,
    typename Expand,
    template<typename, typename, typename> typename Heuristic,
    template<typename, typename, typename, typename...> typename Search
>
bool heuristic_search(PlanTable &PT, const QueryGraph &G, const AdjacencyMatrix &M, const CostFunction &CF,
                      const CardinalityEstimator &CE);

/** Computes the join order using heuristic search */
struct HeuristicSearch final : PlanEnumeratorCRTP<HeuristicSearch>
{
    using base_type = PlanEnumeratorCRTP<HeuristicSearch>;
    using base_type::operator();

    template<typename PlanTable>
    void operator()(enumerate_tag, PlanTable &PT, const QueryGraph &G, const CostFunction &CF) const;
};

}
