#pragma once

#include <algorithm>
#include <boost/container/allocator.hpp>
#include <boost/container/node_allocator.hpp>
#include <boost/heap/binomial_heap.hpp>
// #include <boost/heap/fibonacci_heap.hpp>
// #include <boost/heap/pairing_heap.hpp>
#include <exception>
#include <map>
#include <queue>
#include <ratio>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>


namespace m {

/*----- forward declarations -----------------------------------------------------------------------------------------*/
struct AdjacencyMatrix;

namespace ai {


/*======================================================================================================================
 * Type Traits for Heuristic Search
 *====================================================================================================================*/

/*===== State ========================================================================================================*/

/*----- double State::g() const --------------------------------------------------------------------------------------*/
template<typename State>
struct has_method_g : std::conjunction<
    std::is_member_function_pointer<decltype(&State::g)>,
    std::is_invocable_r<double, decltype(&State::g), const State&>,
    std::is_member_function_pointer<decltype(&State::decrease_g)>,
    std::is_invocable_r<void, decltype(&State::decrease_g), State&, double>
> { };

/*----- bool State::is_goal() const ----------------------------------------------------------------------------------*/
template<typename State, typename... Context>
struct provides_is_goal
{
    template<typename = void> struct helper : std::false_type { };
    template<> struct helper<bool> : std::true_type { };
    static constexpr bool value = helper<decltype(std::declval<const State>().is_goal(std::declval<Context>()...))>::value;
};

/*----- partitioning -------------------------------------------------------------------------------------------------*/
template<typename State, typename... Context>
struct provides_partitioning
{
    template<typename, typename = void>
    struct helper : std::false_type { };
    template<typename S>
    struct helper<S, std::void_t<
        decltype(S::num_partitions(std::declval<Context>()...)),
        decltype(std::declval<const S>().partition_id(std::declval<Context>()...))
    >> : std::true_type { };

    static constexpr bool value = helper<State>::value;
};
template<typename State, typename... Context>
static constexpr bool provides_partitioning_v = provides_partitioning<State, Context...>::value;

/*----- State --------------------------------------------------------------------------------------------------------*/
template<typename State, typename... Context>
struct is_search_state : std::conjunction<
    std::is_class<State>,
    std::is_class<typename State::base_type>,
    std::is_move_constructible<State>,
    std::is_move_assignable<State>,
    has_method_g<State>,
    provides_is_goal<State, Context...>
> { };
template<typename State, typename... Context>
inline constexpr bool is_search_state_v = is_search_state<State, Context...>::value;


/*===== Heuristic ====================================================================================================*/

/*----- double Heuristic::operator(const State&, Context...) const ---------------------------------------------------*/
template<typename Heuristic, typename... Context>
struct is_callable : std::conjunction<
    std::is_member_function_pointer<decltype(&Heuristic::operator())>,
    std::is_invocable_r<double, decltype(&Heuristic::operator()), Heuristic&, const typename Heuristic::state_type&, Context...>
> { };

/*----- Heuristic ----------------------------------------------------------------------------------------------------*/
template<typename Heuristic, typename... Context>
struct is_search_heuristic : std::conjunction<
    is_search_state<typename Heuristic::state_type, Context...>,
    is_callable<Heuristic, Context...>
> { };
template<typename Heuristic, typename... Context>
inline constexpr bool is_search_heuristic_v = is_search_heuristic<Heuristic, Context...>::value;


/*===== Search Algorithm =============================================================================================*/

/*----- void Search::search(State, Heuristic&, Context...) -----------------------------------------------------------*/
template<typename Search, typename... Context>
struct has_method_search : std::conjunction<
    std::is_member_function_pointer<decltype(&Search::search)>,
    std::is_invocable<decltype(&Search::search), Search&, typename Search::state_type, typename Search::heuristic_type&,
                      typename Search::expand_type, Context...>
> { };

/*----- Search Algorithm ---------------------------------------------------------------------------------------------*/
template<typename Search, typename... Context>
struct is_search_search : std::conjunction<
    is_search_state<typename Search::state_type, Context...>,
    is_search_heuristic<typename Search::heuristic_type, Context...>,
    std::is_same<typename Search::state_type, typename Search::heuristic_type::state_type>,
    has_method_search<Search, Context...>
> { };
template<typename Search, typename... Context>
inline constexpr bool is_search_search_v = is_search_search<Search, Context...>::value;


/*======================================================================================================================
 * Heuristic Search
 *====================================================================================================================*/

/** Find a path from an `initial_state` to a goal state and return its cost. */
template<
    typename State,
    typename Heuristic,
    typename Expand,
    template</* State */ typename, /*Heuristic */ typename, /* Expand */ typename, /*Context */ typename...>
        typename SearchAlgorithm,
    typename... Context
>
double search(State initial_state, Heuristic &heuristic, Expand expand, Context&... context)
{
    static_assert(is_search_state_v<State, Context...>, "State is not a valid state for this search");
    static_assert(is_search_heuristic_v<Heuristic, Context...>, "Heuristic is not a valid heuristic");
    static_assert(std::is_same_v<State, typename Heuristic::state_type>, "Heuristic is not applicable to State");

    using search_algorithm = SearchAlgorithm<State, Heuristic, Expand, Context...>;
    static_assert(is_search_search_v<search_algorithm, Context...>,
                  "SearchAlgorithm is not a valid search algorithm for this search");

    search_algorithm S(context...);
    double final_cost = S.search(std::move(initial_state), heuristic, std::move(expand),
                                 std::forward<Context>(context)...);
    return final_cost;
}


/*======================================================================================================================
 * Heuristic Search Classes
 *====================================================================================================================*/

/** Tracks states and their presence in queues. */
template<typename State, bool HasRegularQueue, bool HasBeamQueue, typename... Context>
struct StateManager
{
    static_assert(is_search_state_v<State, Context...>, "State is not a valid search state");
    static_assert(HasRegularQueue or HasBeamQueue, "must have at least one kind of queue");

    using state_type = State;
    static constexpr bool has_regular_queue = HasRegularQueue;
    static constexpr bool has_beam_queue = HasBeamQueue;

    private:
    ///> type for a pointer to an entry in the map of states
    using pointer_type = void*;
    ///> comparator for map entries based on states' *g + h* value
    struct comparator { bool operator()(pointer_type p_left, pointer_type p_right) const; };
    ///> the type of heap to implement the priority queues
    using heap_type = boost::heap::binomial_heap<pointer_type, boost::heap::compare<comparator>>;
    // using heap_type = boost::heap::fibonacci_heap<pointer_type, boost::heap::compare<comparator>>;
    // using heap_type = boost::heap::pairing_heap<pointer_type, boost::heap::compare<comparator>>;

    /*----- Counters -------------------------------------------------------------------------------------------------*/
#if 1
#define DEF_COUNTER(NAME) \
    std::size_t num_##NAME##_ = 0; \
    void inc_##NAME() { ++num_##NAME##_; } \
    std::size_t num_##NAME() const { return num_##NAME##_; }
#else
#define DEF_COUNTER(NAME) \
    void inc_##NAME() { } \
    std::size_t num_##NAME() const { return 0; }
#endif

    DEF_COUNTER(new)
    DEF_COUNTER(duplicates)
    DEF_COUNTER(regular_to_beam)
    DEF_COUNTER(none_to_beam)
    DEF_COUNTER(discarded)
    DEF_COUNTER(cheaper)
    DEF_COUNTER(decrease_key)

#undef DEF_COUNTER

    ///> information attached to each state
    struct StateInfo
    {
        ///> heuristic value of the state
        double h;
        ///> the heap in which the state is currently present; may be `nullptr`
        heap_type *queue;
        ///> handle to the state's node in a heap
        typename heap_type::handle_type handle;

        StateInfo() = delete;
        StateInfo(double h, heap_type *queue) : h(h), queue(queue) { }
        StateInfo(const StateInfo&) = delete;
        StateInfo(StateInfo&&) = default;
        StateInfo & operator=(StateInfo&&) = default;
    };

    using map_value_type = std::pair<const state_type, StateInfo>;
    using map_type = std::unordered_map<
        /* Key=       */ state_type,
        /* Mapped=    */ StateInfo,
        /* Hash=      */ std::hash<state_type>,
        /* KeyEqual=  */ std::equal_to<state_type>,
        /* Allocator= */ boost::container::node_allocator<map_value_type>
    >;

    /*----- Helper type to manage potentially partitioned states. ----------------------------------------------------*/
    template<bool Partition>
    struct Partitions;

    template<>
    struct Partitions<true>
    {
        std::vector<map_type> partitions_;

        Partitions(Context&... context)
            : partitions_(state_type::num_partitions(context...))
        {
            std::cerr << "initialize with " << partitions_.size() << " partitions\n";
        }
        Partitions(const Partitions&) = delete;
        Partitions(Partitions&&) = default;

        ~Partitions() {
            std::cerr << partitions_.size() << " partitions:";
            for (auto &P : partitions_)
                std::cerr << "\n  " << P.size();
            std::cerr << std::endl;
        }

        map_type & operator()(const state_type &state, Context&... context) {
            M_insist(state.partition_id(context...) < partitions_.size(), "index out of bounds");
            return partitions_[state.partition_id(context...)];
        }
        const map_type & operator()(const state_type &state, Context&... context) const {
            M_insist(state.partition_id(context...) < partitions_.size(), "index out of bounds");
            return partitions_[state.partition_id(context...)];
        }

        std::size_t size() const {
            std::size_t n = 0;
            for (auto &P : partitions_)
                n += P.size();
            return n;
        }

        void clear() {
            for (auto &P : partitions_)
                P.clear();
        }
    };

    template<>
    struct Partitions<false>
    {
        map_type states_;

        Partitions(Context&...) { }
        Partitions(const Partitions&) = delete;
        Partitions(Partitions&&) = default;

        map_type & operator()(const state_type&, Context&...) { return states_; }
        const map_type & operator()(const state_type&, Context&...) const { return states_; }

        std::size_t size() const { return states_.size(); }

        void clear() { states_.clear(); }
    };

    ///> map of all states ever explored, mapping state to its info; partitioned by state partition id
    Partitions<provides_partitioning_v<State, Context...>> partitions_;

    ///> map of all states ever explored, mapping state to its info
    // map_type states_;

    heap_type regular_queue_;
    heap_type beam_queue_;

    public:
    StateManager(Context&... context) : partitions_(context...) { }
    StateManager(const StateManager&) = delete;
    StateManager(StateManager&&) = default;
    StateManager & operator=(StateManager&&) = default;

    template<bool ToBeamQueue>
    void push(state_type state, double h, Context&... context) {
        static_assert(not ToBeamQueue or HasBeamQueue, "ToBeamQueue implies HasBeamQueue");
        static_assert(ToBeamQueue or HasRegularQueue, "not ToBeamQueue implies HasRegularQueue");

        auto &Q = ToBeamQueue ? beam_queue_ : regular_queue_;
        auto &P = partition(state, context...);

#if 1
        if (auto it = P.find(state); it == P.end()) [[likely]] {
            /*----- Entirely new state, never seen before. -----*/
            it = P.emplace_hint(it, std::move(state), StateInfo(h, &Q));
            /*----- Enqueue state, obtain handle, and add to `StateInfo`. -----*/
            it->second.handle = Q.push(&*it);
            inc_new();
        } else {
            /*----- Duplicate, seen before. -----*/
            M_insist(it->second.h == h, "must not have a different heuristic value for the same state");
            inc_duplicates();

            if (ToBeamQueue and it->second.queue == &regular_queue_) {
                /*----- The state is in the regular queue and needs to be moved to the beam queue. -----*/
                if constexpr (HasRegularQueue)
                    it->second.queue->erase(it->second.handle); // erase from regular queue
                it->first.decrease_g(std::min(state.g(), it->first.g())); // update *g* value
                it->second.handle = beam_queue_.push(&*it); // add to beam queue and update handle
                it->second.queue = &beam_queue_; // update queue
                if constexpr (HasRegularQueue)
                    inc_regular_to_beam();
                else
                    inc_none_to_beam();
            } else if (state.g() >= it->first.g()) [[likely]] {
                /*----- The state wasn't reached on a cheaper path and hence cannot produce better solutions. -----*/
                inc_discarded();
                return; // XXX is it safe to not add the state to any queue?
            } else {
                /*----- The state was reached on a cheaper path.  We must reconsider the state. -----*/
                M_insist(state.g() < it->first.g(), "the state was reached on a cheaper path");
                inc_cheaper();
                it->first.decrease_g(state.g()); // decrease value of *g*
                if (it->second.queue == nullptr) {
                    /*----- The state is currently not present in a queue. -----*/
                    it->second.handle = Q.push(&*it); // add to dedicated queue and update handle
                    it->second.queue = &Q; // update queue
                } else {
                    /*----- Update the state's entry in the queue. -----*/
                    M_insist(it->second.queue == &Q, "the state must already be in its destinated queue");
                    Q.increase(it->second.handle); // we need to *increase* because boost implements a max-heap
                    inc_decrease_key();
                }
            }
        }
#else
        const auto new_g = state.g();
        auto [it, res] = P.try_emplace(std::move(state), StateInfo(h, &Q));
        Q.push(&*it);
        if (res) {
            inc_new();
        } else {
            inc_duplicates();
            if (new_g < it->first.g())
                inc_cheaper();
        }
#endif
    }

    void push_regular_queue(state_type state, double h, Context&... context) {
#if 1
        if constexpr (HasRegularQueue) {
            push<false>(std::move(state), h, context...);
        } else {
            auto &P = partition(state, context...);
            /*----- There is no regular queue.  Only update the state's mapping. -----*/
            if (auto it = P.find(state); it == P.end()) {
                /*----- This is a new state.  Simply create a new entry. -----*/
                it = P.emplace_hint(it, std::move(state), StateInfo(h, &regular_queue_));
                inc_new();
                /* We must not add the state to the regular queue, but we must remember that the state can still be
                 * "moved" to the beam queue. */
            } else {
                /*----- This is a duplicate state.  Check whether it has lower cost *g*. -----*/
                M_insist(it->second.h == h, "must not have a different heuristic value for the same state");
                inc_duplicates();
                if (state.g() < it->first.g()) {
                    inc_cheaper();
                    it->first.decrease_g(state.g());
                    if (it->second.queue == &beam_queue_) { // if state is currently in a queue
                        it->second.queue->increase(it->second.handle); // *increase* because max-heap
                        inc_decrease_key();
                    }
                } else {
                    inc_discarded();
                }
            }
        }
#else
        push<not HasRegularQueue>(std::move(state), h, context...);
#endif
    }

    void push_beam_queue(state_type state, double h, Context&... context) {
        push<true>(std::move(state), h, context...);
    }

    bool is_regular_queue_empty() const { return not HasRegularQueue or regular_queue_.empty(); }
    bool is_beam_queue_empty() const { return not HasBeamQueue or beam_queue_.empty(); }
    bool queues_empty() const { return is_regular_queue_empty() and is_beam_queue_empty(); }

    std::pair<const state_type&, double> pop() {
        M_insist(not queues_empty());
        pointer_type ptr = nullptr;
        if (HasBeamQueue and not beam_queue_.empty()) {
            ptr = beam_queue_.top();
            beam_queue_.pop();
        } else if (HasRegularQueue and not regular_queue_.empty()) {
            ptr = regular_queue_.top();
            regular_queue_.pop();
        }
        M_insist(ptr, "ptr must have been set, the queues must not have been empty");

        auto &entry = *static_cast<typename map_type::value_type*>(ptr);
        entry.second.queue = nullptr; // remove from queue
        return { entry.first, entry.second.h };
    }

    typename map_type::iterator find(const state_type &state, Context&... context) {
        return partition(state, context...).find(state);
    }
    typename map_type::iterator end(const state_type &state, Context&... context) {
        return partition(state, context...).end();
    }
    typename map_type::const_iterator find(const state_type &state, Context&... context) const {
        return partition(state, context...).find(state);
    }
    typename map_type::const_iterator end(const state_type &state, Context&... context) const {
        return partition(state, context...).end();
    }

    void clear() {
        if constexpr (HasRegularQueue)
            regular_queue_.clear();
        if constexpr (HasBeamQueue)
            beam_queue_.clear();
        // states_.clear();
        partitions_.clear();
    }

    void print_counters(std::ostream &out) const {
#define X(NAME) num_##NAME() << " " #NAME
        out << X(new) ", " << X(duplicates) ", ";
        if (HasRegularQueue and HasBeamQueue)
            out << X(regular_to_beam) ", ";
        if (HasBeamQueue)
            out << X(none_to_beam) ", ";
        out << X(discarded) ", " << X(cheaper) ", " << X(decrease_key);
#undef X
    }

    map_type & partition(const state_type &state, Context&... context) { return partitions_(state, context...); }
    const map_type & partition(const state_type &state, Context&... context) const {
        return partitions_(state, context...);
    }

    friend std::ostream & operator<<(std::ostream &out, const StateManager &SM) {
        SM.print_counters(out);
        out << ", " << SM.partitions_.size() << " seen, " << SM.regular_queue_.size() << " currently in regular queue, "
            << SM.beam_queue_.size() << " currently in beam queue";
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

template<typename State, bool HasRegularQueue, bool HasBeamQueue, typename... Context>
bool StateManager<State, HasRegularQueue, HasBeamQueue, Context...>::comparator::
operator()(StateManager::pointer_type p_left, StateManager::pointer_type p_right) const
{
    auto left  = static_cast<typename map_type::value_type*>(p_left);
    auto right = static_cast<typename map_type::value_type*>(p_right);
    /* Compare greater than (`operator >`) to turn max-heap into min-heap. */
    return left->first.g() + left->second.h > right->first.g() + right->second.h;
}


/** Implements a generic A* search algorithm.  Allows for specifying a weighting factor for the heuristic value.  Allows
 * for lazy evaluation of the heuristic. */
template<
    typename State,
    typename Heuristic,
    typename Expand,
    typename Weight,
    unsigned BeamWidth,
    bool Lazy,
    bool IsMonotone,
    typename... Context
>
struct genericAStar
{
    static_assert(is_search_state_v<State, Context...>, "State is not a valid state for this search");
    static_assert(is_search_heuristic_v<Heuristic, Context...>, "Heuristic is not a valid heuristic");
    static_assert(std::is_same_v<State, typename Heuristic::state_type>, "Heuristic is not applicable to State");
    static_assert(std::is_arithmetic_v<decltype(Weight::num)> and std::is_arithmetic_v<decltype(Weight::den)>,
                  "Weight must be specified as std::ratio<Num, Denom>");

    using state_type = State;
    using heuristic_type = Heuristic;
    using expand_type = Expand;
    using weight = Weight;

    ///> The width of a beam used for *beam search*.  Set to 0 to disable beam search.
    static constexpr unsigned beam_width = BeamWidth;
    ///> Whether to perform beam search or regular A*
    static constexpr bool use_beam_search = beam_width != 0;
    ///> Whether to use a dynamic beam width for beam search
    static constexpr bool use_dynamic_beam_sarch = beam_width == decltype(beam_width)(-1);
    ///> Whether to evaluate the heuristic lazily
    static constexpr bool is_lazy = Lazy;
    ///> Whether the state space is acyclic and without dead ends (useful in combination with beam search)
    static constexpr bool is_monotone = IsMonotone;
    ///> The fraction of a state's successors to add to the beam (if performing beam search)
    static constexpr float BEAM_FACTOR = .2f;

    using callback_t = std::function<void(state_type, double)>;

    private:
#if 1
#define DEF_COUNTER(NAME) \
    std::size_t num_##NAME##_ = 0; \
    void inc_##NAME() { ++num_##NAME##_; } \
    std::size_t num_##NAME() const { return num_##NAME##_; }
#else
#define DEF_COUNTER(NAME) \
    void inc_##NAME() { } \
    std::size_t num_##NAME() const { return 0; }
#endif

    DEF_COUNTER(cached_heuristic_value)

#undef DEF_COUNTER

    /** A state plus its heuristic value. */
    struct weighted_state
    {
        state_type state;
        double h;

        weighted_state(state_type state, double h) : state(std::move(state)), h(h) { }
        weighted_state(const weighted_state&) = delete;
        weighted_state(weighted_state&&) = default;
        weighted_state & operator=(weighted_state&&) = default;

        bool operator<(const weighted_state &other) const { return this->weight() < other.weight(); }
        bool operator>(const weighted_state &other) const { return this->weight() > other.weight(); }
        bool operator<=(const weighted_state &other) const { return this->weight() <= other.weight(); }
        bool operator>=(const weighted_state &other) const { return this->weight() >= other.weight(); }

        private:
        double weight() const { return state.g() + h; }

    };

    StateManager</* State=           */ State,
                 /* HasRegularQueue= */ not (use_beam_search and is_monotone),
                 /* HasBeamQueue=    */ use_beam_search,
                 /* Context...=      */ Context...
    > state_manager_;

    ///> candidates for the beam
    std::vector<weighted_state> candidates;

    public:
    explicit genericAStar(Context&... context)
        : state_manager_(context...)
    {
        if constexpr (use_beam_search) {
            if constexpr (not use_dynamic_beam_sarch)
                candidates.reserve(beam_width + 1);
        }
    }

    ~genericAStar() {
        dump();
    }

    genericAStar(const genericAStar&) = delete;
    genericAStar(genericAStar&&) = default;

    genericAStar & operator=(genericAStar&&) = default;

    /** Search for a path from the given `initial_state` to a goal state.  Uses the given heuristic to guide the search.
     *
     * @return the cost of the computed path from `initial_state` to a goal state
     */
    double search(state_type initial_state, heuristic_type &heuristic, expand_type expand, Context&... context);

    /** Resets the state of the search. */
    void clear() {
        state_manager_.clear();
        candidates.clear();
    }

    private:
    /*------------------------------------------------------------------------------------------------------------------
     * Helper methods
     *----------------------------------------------------------------------------------------------------------------*/

    /* Try to add the successor state `s` to the beam.  If the weight of `s` is already higher than the highest weight
     * in the beam, immediately bypass the beam. */
    void beam(state_type state, double h, Context&... context) {
        auto &top = candidates.front();
#ifndef NDEBUG
        M_insist(std::is_heap(candidates.begin(), candidates.end()), "candidates must always be a max-heap");
        for (auto &elem : candidates)
            M_insist(top >= elem, "the top candidate at the front must be no less than any other candidate");
#endif
        if (candidates.size() < beam_width) {
            /* There is still space in the candidates, so simply add the state to the heap. */
            candidates.emplace_back(std::move(state), h);
            if (candidates.size() == beam_width) // heapify when filled
                std::make_heap(candidates.begin(), candidates.end());
        } else if (state.g() + h >= top.state.g() + top.h) {
            /* The state has higher g+h than the top of the candidates heap, so bypass the candidates immediately. */
            state_manager_.push_regular_queue(std::move(state), h, context...);
        } else {
            /* The state has less g+h than the top of candidates heap.  Pop the current top and insert the state into
             * the candidates heap. */
            M_insist(candidates.size() == beam_width);
            M_insist(std::is_heap(candidates.begin(), candidates.end()));
            candidates.emplace_back(std::move(state), h);
            std::pop_heap(candidates.begin(), candidates.end());
            weighted_state worst_candidate = std::move(candidates.back()); // extract worst candidate
            candidates.pop_back();
            M_insist(std::is_heap(candidates.begin(), candidates.end()));
            M_insist(candidates.size() == beam_width);
#ifndef NDEBUG
            for (auto &elem : candidates)
                M_insist(worst_candidate >= elem, "worst candidate must be no less than any other candidate");
#endif
            state_manager_.push_regular_queue(std::move(worst_candidate.state), worst_candidate.h, context...); // move to regular
        }
    };

    /* Compute a beam of dynamic (relative) size from the set of successor states. */
    void beam_dynamic(Context&... context) {
        std::sort(candidates.begin(), candidates.end());
        const std::size_t num_beamed = std::ceil(candidates.size() * BEAM_FACTOR);
        M_insist(not candidates.size() or num_beamed, "if the state has successors, at least one must be in the beam");
        auto it = candidates.begin();
        /*----- Add states in the beam to the beam queue. -----*/
        for (auto end = it + num_beamed; it != end; ++it)
            state_manager_.push_beam_queue(std::move(it->state), it->h, context...);
        /*----- Add remaining states to the regular queue. -----*/
        for (auto end = candidates.end(); it != end; ++it)
            state_manager_.push_regular_queue(std::move(it->state), it->h, context...);
    };

    /** Expands the given `state` by *lazily* evaluating the heuristic function. */
    void for_each_successor_lazily(callback_t &&callback, const state_type &state, heuristic_type &heuristic,
                                   expand_type &expand, Context&... context)
    {
        /*----- Evaluate heuristic lazily by using heurisitc value of current state. -----*/
        double h_current_state;
        if (auto it = state_manager_.find(state, context...); it == state_manager_.end(state, context...)) {
            h_current_state = double(Weight::num) * heuristic(state, context...) / Weight::den;
        } else {
            inc_cached_heuristic_value();
            h_current_state = it->second.h; // use cached `h`
        }
        expand(state, [&callback, h_current_state](state_type successor) {
            callback(std::move(successor), h_current_state);
        }, context...);
    };

    /** Expands the given `state` by *eagerly* evaluating the heuristic function. */
    void for_each_successor_eagerly(callback_t &&callback, const state_type &state, heuristic_type &heuristic,
                                    expand_type &expand, Context&... context)
    {
        /*----- Evaluate heuristic eagerly. -----*/
        expand(state, [this, callback=std::move(callback), &state, &heuristic, &context...](state_type successor) {
            if (auto it = state_manager_.find(successor, context...); it == state_manager_.end(state, context...)) {
                const double h = double(Weight::num) * heuristic(successor, context...) / Weight::den;
                callback(std::move(successor), h);
            } else {
                inc_cached_heuristic_value();
                callback(std::move(successor), it->second.h); // use cached `h`
            }
        }, context...);
    };

    /** Expands the given `state` according to `is_lazy`. */
    void for_each_successor(callback_t &&callback, const state_type &state, heuristic_type &heuristic,
                            expand_type &expand, Context&... context)
    {
        if constexpr (is_lazy)
            for_each_successor_lazily(std::move(callback), state, heuristic, expand, context...);
        else
            for_each_successor_eagerly(std::move(callback), state, heuristic, expand, context...);
    };

    /** Explores the given `state`. */
    void explore_state(const state_type &state, heuristic_type &heuristic, expand_type &expand, Context&... context) {
        if constexpr (use_dynamic_beam_sarch) {
            /*----- Add all successors to candidates.  Only best `BEAM_FACTOR` will be added to beam queue. -----*/
            candidates.clear();
            for_each_successor([this](state_type successor, double h) {
                candidates.emplace_back(std::move(successor), h);
            }, state, heuristic, expand, context...);
            beam_dynamic(context...); // order by g+h and partition into beam and regular states
        } else if constexpr (use_beam_search) {
            /*----- Keep only `beam_width` best successors in `candidates`, rest is added to regular queue. ----*/
            candidates.clear();
            for_each_successor([this, &context...](state_type successor, double h) {
                beam(std::move(successor), h, context...); // try to add to candidates
            }, state, heuristic, expand, context...);
            /*----- The states remaining in `candidates` are within the beam. -----*/
            for (auto &s : candidates)
                state_manager_.push_beam_queue(std::move(s.state), s.h, context...);
        } else {
            /*----- Have only regular queue. -----*/
            for_each_successor([this, &context...](state_type successor, double h) {
                state_manager_.push_regular_queue(std::move(successor), h, context...);
            }, state, heuristic, expand, context...);
        }
    };

    public:
    friend std::ostream & operator<<(std::ostream &out, const genericAStar &AStar) {
        return out << AStar.state_manager_ << ", used cached heuristic value " << AStar.num_cached_heuristic_value()
                   << " times";
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

template<
    typename State,
    typename Heuristic,
    typename Expand,
    typename Weight,
    unsigned BeamWidth,
    bool Lazy,
    bool IsMonotone,
    typename... Context
>
double genericAStar<State, Heuristic, Expand, Weight, BeamWidth, Lazy, IsMonotone, Context...>::search(
    state_type initial_state,
    heuristic_type &heuristic,
    expand_type expand,
    Context&... context
) {
    /* Initialize queue with initial state. */
    state_manager_.template push<use_beam_search and is_monotone>(std::move(initial_state), 0, context...);

    /* Run work list algorithm. */
    while (not state_manager_.queues_empty()) {
        auto top = state_manager_.pop();
        const state_type &state = top.first;
        const double h = top.second;

        if (state.is_goal(context...))
            return state.g();

        explore_state(state, heuristic, expand, context...);

        M_insist(not (is_monotone and use_beam_search) or not state_manager_.is_beam_queue_empty(),
                 "the beam queue must not run empty with beam search on a monotone search space");
        M_insist(not state_manager_.queues_empty(), "queue must never run empty before finding a solution");
    }

    throw std::logic_error("goal state unreachable from provided initial state");
}

template<
    typename State,
    typename Heuristic,
    typename Expand,
    typename... Context
>
using AStar = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, 0, false, false, Context...>;

template<
    typename State,
    typename Heuristic,
    typename Expand,
    typename... Context
>
using lazy_AStar = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, 0, true, false, Context...>;

template<typename Weight>
struct wAStar
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, Weight, 0, false, false, Context...>;
};

template<typename Weight>
struct lazy_wAStar
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, Weight, 0, true, false, Context...>;
};

template<unsigned BeamWidth>
struct beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, BeamWidth, false, false, Context...>;
};

template<unsigned BeamWidth>
struct lazy_beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, BeamWidth, true, false, Context...>;
};

template<unsigned BeamWidth>
struct monotone_beam_search
{
    template<
        typename State,
        typename Heuristic,
        typename Expand,
        typename... Context
    >
    using type = genericAStar<State, Heuristic, Expand, std::ratio<1, 1>, BeamWidth, false, true, Context...>;
};

}

}
