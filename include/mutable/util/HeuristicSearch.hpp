#pragma once

#include <algorithm>
#include <concepts>
#include <exception>
#include <experimental/type_traits> // std::is_detected
#include <iosfwd>
#include <map>
#include <mutable/Options.hpp>
#include <queue>
#include <ratio>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <thread>



namespace m {

/*----- forward declarations -----------------------------------------------------------------------------------------*/
struct AdjacencyMatrix;

namespace ai {


/*======================================================================================================================
 * Type Traits for Heuristic Search
 *====================================================================================================================*/

/*===== State ========================================================================================================*/

/*----- State --------------------------------------------------------------------------------------------------------*/
template<typename State>
concept heuristic_search_state =
    std::is_class_v<State> and
    std::is_class_v<typename State::base_type> and
    std::movable<State> and
    requires (const State &S, const State *parent) {
        { S.g() } -> std::convertible_to<double>;
        { S.decrease_g(parent, double(0)) } -> std::same_as<void>;
    };

/*----- partitioning -------------------------------------------------------------------------------------------------*/
template<typename State, typename... Context>
concept supports_partitioning =
    requires (const State &S, Context&... ctx) {
        { State::num_partitions(ctx...) } -> std::convertible_to<unsigned>;
        { S.partition_id(ctx...) } -> std::convertible_to<unsigned>;
    };


/*===== Heuristic ====================================================================================================*/

template<typename Heuristic, typename... Context>
concept heuristic_search_heuristic =
    heuristic_search_state<typename Heuristic::state_type> and
    requires (Heuristic &H, const typename Heuristic::state_type &S, Context&... ctx) {
        { H.operator()(S, ctx...) } -> std::convertible_to<double>;
    };


/*===== Search Algorithm =============================================================================================*/

template<typename Search, typename... Context>
concept heuristic_search =
    heuristic_search_state<typename Search::state_type> and
    heuristic_search_heuristic<typename Search::heuristic_type, Context...> and
    std::same_as<typename Search::state_type, typename Search::heuristic_type::state_type> and
    requires (
            Search &search,
            typename Search::state_type state,
            typename Search::heuristic_type &heuristic,
            typename Search::expand_type ex,
            Context&... ctx
    ) {
        { search.search(state, ex, heuristic, ctx...) };
    };


/*======================================================================================================================
 * Heuristic Search Classes
 *====================================================================================================================*/

/** Tracks states and their presence in queues. */
template<
    heuristic_search_state State,
    bool HasRegularQueue,
    bool HasBeamQueue,
    typename Config,
    typename... Context
>
struct StateManager
{
    static_assert(HasRegularQueue or HasBeamQueue, "must have at least one kind of queue");

    using state_type = State;
    static constexpr bool has_regular_queue = HasRegularQueue;
    static constexpr bool has_beam_queue = HasBeamQueue;

    static constexpr bool detect_duplicates = true;

    private:
    ///> type for a pointer to an entry in the map of states
    using pointer_type = void*;
    ///> comparator for map entries based on states' *g + h* value
    struct comparator { bool operator()(pointer_type p_left, pointer_type p_right) const; };
    ///> the type of heap to implement the priority queues
    using heap_type = typename Config::template heap_type<pointer_type, typename Config::template compare<comparator>>;

    /*----- Counters -------------------------------------------------------------------------------------------------*/
#ifndef NDEBUG
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
        /* Allocator= */ typename Config::template allocator_type<map_value_type>
    >;

    /*----- Helper type to manage potentially partitioned states. ----------------------------------------------------*/
    template<bool Partition>
    struct Partitions;

    template<>
    struct Partitions<true>
    {
        std::vector<map_type> partitions_;

        Partitions(Context&... context) : partitions_(state_type::num_partitions(context...)) { }
        Partitions(const Partitions&) = delete;
        Partitions(Partitions&&) = default;

        ~Partitions() {
            if (Options::Get().statistics) {
                std::cout << partitions_.size() << " partitions:";
                for (auto &P : partitions_)
                    std::cout << "\n  " << P.size();
                std::cout << std::endl;
            }
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
    Partitions<supports_partitioning<State, Context...>> partitions_;

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

        if constexpr (detect_duplicates) {
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
                    if (state.g() < it->first.g())
                        it->first.decrease_g(state.parent(), state.g()); // update *g* value
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
                    it->first.decrease_g(state.parent(), state.g()); // decrease value of *g*
                    if (it->second.queue == nullptr) {
                        /*----- The state is currently not present in a queue. -----*/
                        it->second.handle = Q.push(&*it); // add to dedicated queue and update handle
                        it->second.queue = &Q; // update queue
                    } else {
                        /*----- Update the state's entry in the queue. -----*/
                        M_insist(it->second.queue == &Q, "the state must already be in its destinated queue");
                        Q.increase(it->second.handle); // we need to *increase* because the heap is a max-heap
                        inc_decrease_key();
                    }
                }
            }
        } else {
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
        }
    }

    void push_regular_queue(state_type state, double h, Context&... context) {
        if constexpr (detect_duplicates) {
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
                        it->first.decrease_g(state.parent(), state.g());
                        if (it->second.queue == &beam_queue_) { // if state is currently in a queue
                            it->second.queue->increase(it->second.handle); // *increase* because max-heap
                            inc_decrease_key();
                        }
                    } else {
                        inc_discarded();
                    }
                }
            }
        } else {
            push<not HasRegularQueue>(std::move(state), h, context...);
        }
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
#ifndef NDEBUG
        SM.print_counters(out);
        out << ", ";
#endif
        out << SM.partitions_.size() << " seen, " << SM.regular_queue_.size() << " currently in regular queue, "
            << SM.beam_queue_.size() << " currently in beam queue";
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

template<
    heuristic_search_state State,
    bool HasRegularQueue,
    bool HasBeamQueue,
    typename Config,
    typename... Context
>
bool StateManager<State, HasRegularQueue, HasBeamQueue, Config, Context...>::comparator::
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
    heuristic_search_state State,
    typename Expand,
    typename Heuristic,
    typename Weight,
    unsigned BeamWidth,
    bool Lazy,
    bool IsMonotone,
    typename Config,
    typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...> and
         std::convertible_to<decltype(Weight::num), double> and std::convertible_to<decltype(Weight::den), double>
struct genericAStar
{
    using state_type = State;
    using expand_type = Expand;
    using heuristic_type = Heuristic;
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
                 /* Config=          */ Config,
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

    genericAStar(const genericAStar&) = delete;
    genericAStar(genericAStar&&) = default;

    genericAStar & operator=(genericAStar&&) = default;

    /** Search for a path from the given `initial_state` to a goal state.  Uses the given heuristic to guide the search.
     *
     * @return the cost of the computed path from `initial_state` to a goal state
     */
    const State & search(state_type initial_state, expand_type expand, heuristic_type &heuristic, Context&... context);

    /** Resets the state of the search. */
    void clear() {
        state_manager_.clear();
        candidates.clear();
    }

    private:
    /*------------------------------------------------------------------------------------------------------------------
     * Helper methods
     *----------------------------------------------------------------------------------------------------------------*/

    template<typename T>
    using has_mark = decltype(std::declval<T>().mark(Subproblem()));

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
    void beam_dynamic(expand_type &expand, Context&... context) {
        std::sort(candidates.begin(), candidates.end());
        const std::size_t num_beamed = std::ceil(candidates.size() * BEAM_FACTOR);
        M_insist(not candidates.size() or num_beamed, "if the state has successors, at least one must be in the beam");
        auto it = candidates.begin();
        /*----- Add states in the beam to the beam queue. -----*/
        for (auto end = it + num_beamed; it != end; ++it) {
            if constexpr (std::experimental::is_detected_v<has_mark, state_type>)
                expand.reset_marked(it->state, context...);
            state_manager_.push_beam_queue(std::move(it->state), it->h, context...);
        }
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
            beam_dynamic(expand, context...); // order by g+h and partition into beam and regular states
        } else if constexpr (use_beam_search) {
            /*----- Keep only `beam_width` best successors in `candidates`, rest is added to regular queue. ----*/
            candidates.clear();
            for_each_successor([this, &context...](state_type successor, double h) {
                beam(std::move(successor), h, context...); // try to add to candidates
            }, state, heuristic, expand, context...);
            /*----- The states remaining in `candidates` are within the beam. -----*/
            for (auto &s : candidates) {
                if constexpr (std::experimental::is_detected_v<has_mark, state_type>)
                    expand.reset_marked(s.state, context...);
                state_manager_.push_beam_queue(std::move(s.state), s.h, context...);
            }
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
    heuristic_search_state State,
    typename Expand,
    typename Heuristic,
    typename Weight,
    unsigned BeamWidth,
    bool Lazy,
    bool IsMonotone,
    typename Config,
    typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...> and
         std::convertible_to<decltype(Weight::num), double> and std::convertible_to<decltype(Weight::den), double>
const State & genericAStar<State, Expand, Heuristic, Weight, BeamWidth, Lazy, IsMonotone, Config, Context...>::search(
    state_type initial_state,
    expand_type expand,
    heuristic_type &heuristic,
    Context&... context
) {
    /* Initialize queue with initial state. */
    state_manager_.template push<use_beam_search and is_monotone>(std::move(initial_state), 0, context...);

    /* Run work list algorithm. */
    while (not state_manager_.queues_empty()) {
        M_insist(not (is_monotone and use_beam_search) or not state_manager_.is_beam_queue_empty(),
                 "the beam queue must not run empty with beam search on a monotone search space");
        auto top = state_manager_.pop();
        const state_type &state = top.first;

        if (expand.is_goal(state, context...))
            return state;

        explore_state(state, heuristic, expand, context...);
    }

    throw std::logic_error("goal state unreachable from provided initial state");
}

/////////////////////////////////////////////////////////////////////////////////////////
//////////////////////hanwenSearch -> IDDFS, currently not in use////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        bool IsIDDFS,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
struct hanwenSearch
{
    using state_type = State;
    using expand_type = Expand;
    using heuristic_type = Heuristic;

    ///> Whether to perform IDDFS
    static constexpr bool use_iddfs = IsIDDFS;
    const double INF = std::numeric_limits<double>::max();

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

    // decide the core details in queue
    StateManager</* State=           */ State,
            /* HasRegularQueue= */ true,
            /* HasBeamQueue=    */ false,
            /* Config=          */ Config,
            /* Context...=      */ Context...
    > state_manager_;


public:
    explicit hanwenSearch(Context&... context)
            : state_manager_(context...)
    {}

    hanwenSearch(const hanwenSearch&) = delete;
    hanwenSearch(hanwenSearch&&) = default;

    hanwenSearch & operator=(hanwenSearch&&) = default;

    /** Search for a path from the given `initial_state` to a goal state.  Uses the given heuristic to guide the search.
     *
     * @return the cost of the computed path from `initial_state` to a goal state
     */
    const State &search(state_type initial_state, expand_type expand, heuristic_type &heuristic, Context &... context);

    /** Resets the state of the search. */
    void clear() {
        state_manager_.clear();
    }

private:
    /*------------------------------------------------------------------------------------------------------------------
     * Helper methods
     *----------------------------------------------------------------------------------------------------------------*/

    template<typename T>
    using has_mark = decltype(std::declval<T>().mark(Subproblem()));


    void hanwen_for_each_successor(callback_t &&callback, const state_type &state, heuristic_type &heuristic,
                                   expand_type &expand, Context &... context) {
        expand(state, [this, callback=std::move(callback), &heuristic, &context...](state_type successor) {
            if (auto it = state_manager_.find(successor, context...);it == state_manager_.end(successor, context...)) {
                const double h = heuristic(successor, context...);
                callback(std::move(successor), h);
            } else {
                inc_cached_heuristic_value();
                callback(std::move(successor), it->second.h);
            }
        }, context...);
    }

    void hanwen_explore_state(double bound, double &min, state_type &final_state, state_type &state,
                              heuristic_type &heuristic, expand_type &expand,
                              Context &... context) {
        hanwen_for_each_successor(
                [this, bound, &min, &final_state, &heuristic, &expand, &context...](state_type successor, double h) {
                    state_manager_.push_regular_queue(std::move(successor), h, context...);
                    hanwen_path.push_back(std::move(successor));
                    double temp = id_search(bound, min, final_state, heuristic, expand, context...);
                    if (temp == FOUND) { return FOUND; }
                    if (temp < min) { min = temp; }
                    hanwen_path.pop_back();
                }, state, heuristic, expand, context...);
    }

    std::vector<State> hanwen_path;
    const double NOT_FOUND = -1;  // Value to indicate goal not found
    const double FOUND = std::numeric_limits<int>::max();  // Unique value to indicate goal found

public:
    friend std::ostream & operator<<(std::ostream &out, const hanwenSearch &AStar) {
        return out << AStar.state_manager_ << ", used cached heuristic value " << AStar.num_cached_heuristic_value()
                   << " times";
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }

    double id_search(double bound, double &min, state_type &final_state, heuristic_type &heuristic, expand_type expand,
                     Context &... context);
};

template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        bool IsIDDFS,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
const State &hanwenSearch<State, Expand, Heuristic, IsIDDFS, Config, Context...>::search(
        state_type initial_state,
        expand_type expand,
        heuristic_type &heuristic,
        Context &... context
) {
    state_type &final_state = initial_state;
    double bound = heuristic(initial_state, context...);
    double min = INF - 1;
    hanwen_path.emplace_back(std::move(initial_state));

    while (true) {
        double result = id_search(bound, min, final_state, heuristic, expand, context...);
        if (result == FOUND) {
            return final_state;
        }

        if (result == NOT_FOUND) {
            throw std::logic_error("goal state unreachable from provided initial state");
        }
        bound = result;
    }
}

template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        bool IsIDDFS,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
double hanwenSearch<State, Expand, Heuristic, IsIDDFS, Config, Context...>::id_search(
        double bound, double &min, state_type &final_state,
        heuristic_type &heuristic, expand_type expand, Context &... context
) {
    state_type &state = hanwen_path.back();
    double f = state.g() + heuristic(state, context...);
    if (f > bound) { return f; }
    if (expand.is_goal(state, context...)) {
        final_state = std::move(state);
        return FOUND;
    }

    hanwen_explore_state(bound, min, final_state, state, heuristic, expand, context...);
    return min;
}
/////////////////////////////////////////////////////////////////////////////////////////
//////////////////////cleanAStar -> For Reuse////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
struct cleanAStarSearch
{
    using state_type = State;
    using expand_type = Expand;
    using heuristic_type = Heuristic;

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

    // decide the core details in queue
    StateManager</* State=           */ State,
            /* HasRegularQueue= */ true,
            /* HasBeamQueue=    */ false,
            /* Config=          */ Config,
            /* Context...=      */ Context...
    > state_manager_;


public:
    explicit cleanAStarSearch(Context&... context)
            : state_manager_(context...)
    {}

    cleanAStarSearch(const cleanAStarSearch&) = delete;
    cleanAStarSearch(cleanAStarSearch&&) = default;

    cleanAStarSearch & operator=(cleanAStarSearch&&) = default;

    /** Search for a path from the given `initial_state` to a goal state.  Uses the given heuristic to guide the search.
     *
     * @return the cost of the computed path from `initial_state` to a goal state
     */
    const State &search(state_type initial_state, expand_type expand, heuristic_type &heuristic, Context &... context);

    /** Resets the state of the search. */
    void clear() {
        state_manager_.clear();
    }

private:
    /*------------------------------------------------------------------------------------------------------------------
     * Helper methods
     *----------------------------------------------------------------------------------------------------------------*/

    template<typename T>
    using has_mark = decltype(std::declval<T>().mark(Subproblem()));


    void for_each_successor(callback_t &&callback, const state_type &state, heuristic_type &heuristic,
                                          expand_type &expand, Context &... context) {
        expand(state, [this, callback=std::move(callback), &heuristic, &context...](state_type successor) {
            if (auto it = state_manager_.find(successor, context...);it == state_manager_.end(successor, context...)) {
                const double h = heuristic(successor, context...);
                callback(std::move(successor), h);
            } else {
                inc_cached_heuristic_value();
                callback(std::move(successor), it->second.h);
            }
        }, context...);
    }

    void explore_state(const state_type &state, heuristic_type &heuristic, expand_type &expand, Context&... context) {
        for_each_successor([this, &context...](state_type successor, double h) {
            state_manager_.push_regular_queue(std::move(successor), h, context...);
        }, state, heuristic, expand, context...);
    }

public:
    friend std::ostream & operator<<(std::ostream &out, const cleanAStarSearch &AStar) {
        return out << AStar.state_manager_ << ", used cached heuristic value " << AStar.num_cached_heuristic_value()
                   << " times";
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }


};

template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
const State &cleanAStarSearch<State, Expand, Heuristic, Config, Context...>::search(
        state_type initial_state,
        expand_type expand,
        heuristic_type &heuristic,
        Context &... context
) {
    state_manager_.template push<false>(std::move(initial_state), 0, context...);
    while (not state_manager_.queues_empty()) {
        auto top = state_manager_.pop();
        const state_type &state = top.first;
        std::cout << "Current state.size()" << state.size() << "\n";

        if (expand.is_goal(state, context...))
            return state;
        explore_state(state, heuristic, expand, context...);
    }
    throw std::logic_error("goal state unreachable from provided initial state");
}
/////////////////////////////////////////////////////////////////////////////////////////
//////////////////////biDirectionSearch -> TopDown & BottomUp////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

template<
        heuristic_search_state State,
        bool HasRegularQueue,
        bool HasBeamQueue,
        typename Config,
        typename... Context
>
struct BiDirectionStateManager
{
    static_assert(HasRegularQueue or HasBeamQueue, "must have at least one kind of queue");

    using state_type = State;
    static constexpr bool has_regular_queue = HasRegularQueue;
    static constexpr bool has_beam_queue = HasBeamQueue;

    static constexpr bool detect_duplicates = true;

private:
    ///> type for a pointer to an entry in the map of states
    using pointer_type = void*;
    ///> comparator for map entries based on states' *g + h* value
    struct comparator { bool operator()(pointer_type p_left, pointer_type p_right) const; };
    ///> the type of heap to implement the priority queues
    using heap_type = typename Config::template heap_type<pointer_type, typename Config::template compare<comparator>>;

    /*----- Counters -------------------------------------------------------------------------------------------------*/
#ifndef NDEBUG
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
            /* Allocator= */ typename Config::template allocator_type<map_value_type>
    >;

    /*----- Helper type to manage potentially partitioned states. ----------------------------------------------------*/
    template<bool Partition>
    struct Partitions;

    template<>
    struct Partitions<true>
    {
        std::vector<map_type> partitions_;

        Partitions(Context&... context) : partitions_(state_type::num_partitions(context...)) { }
        Partitions(const Partitions&) = delete;
        Partitions(Partitions&&) = default;

        ~Partitions() {
            if (Options::Get().statistics) {
                std::cout << partitions_.size() << " partitions:";
                for (auto &P : partitions_)
                    std::cout << "\n  " << P.size();
                std::cout << std::endl;
            }
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
    Partitions<supports_partitioning<State, Context...>> partitions_;



public:
    ///> map of all states ever explored, mapping state to its info
    // map_type states_;
    heap_type regular_queue_;
    heap_type beam_queue_;

    BiDirectionStateManager(Context&... context) : partitions_(context...) { }
    BiDirectionStateManager(const BiDirectionStateManager&) = delete;
    BiDirectionStateManager(BiDirectionStateManager&&) = default;
    BiDirectionStateManager & operator=(BiDirectionStateManager&&) = default;

    template<bool ToBeamQueue>
    void push(state_type state, double h, Context&... context) {
        static_assert(not ToBeamQueue or HasBeamQueue, "ToBeamQueue implies HasBeamQueue");
        static_assert(ToBeamQueue or HasRegularQueue, "not ToBeamQueue implies HasRegularQueue");

        auto &Q = ToBeamQueue ? beam_queue_ : regular_queue_;
        auto &P = partition(state, context...);

        if constexpr (detect_duplicates) {
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
                    if (state.g() < it->first.g())
                        it->first.decrease_g(state.parent(), state.g()); // update *g* value
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
                    it->first.decrease_g(state.parent(), state.g()); // decrease value of *g*
                    if (it->second.queue == nullptr) {
                        /*----- The state is currently not present in a queue. -----*/
                        it->second.handle = Q.push(&*it); // add to dedicated queue and update handle
                        it->second.queue = &Q; // update queue
                    } else {
                        /*----- Update the state's entry in the queue. -----*/
                        M_insist(it->second.queue == &Q, "the state must already be in its destinated queue");
                        Q.increase(it->second.handle); // we need to *increase* because the heap is a max-heap
                        inc_decrease_key();
                    }
                }
            }
        } else {
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
        }
    }

    void push_regular_queue(state_type state, double h, Context&... context) {
        if constexpr (detect_duplicates) {
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
                        it->first.decrease_g(state.parent(), state.g());
                        if (it->second.queue == &beam_queue_) { // if state is currently in a queue
                            it->second.queue->increase(it->second.handle); // *increase* because max-heap
                            inc_decrease_key();
                        }
                    } else {
                        inc_discarded();
                    }
                }
            }
        } else {
            push<not HasRegularQueue>(std::move(state), h, context...);
        }
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

    friend std::ostream & operator<<(std::ostream &out, const BiDirectionStateManager &SM) {
#ifndef NDEBUG
        SM.print_counters(out);
        out << ", ";
#endif
        out << SM.partitions_.size() << " seen, " << SM.regular_queue_.size() << " currently in regular queue, "
            << SM.beam_queue_.size() << " currently in beam queue";
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

template<
        heuristic_search_state State,
        bool HasRegularQueue,
        bool HasBeamQueue,
        typename Config,
        typename... Context
>
bool BiDirectionStateManager<State, HasRegularQueue, HasBeamQueue, Config, Context...>::comparator::
operator()(BiDirectionStateManager::pointer_type p_left, BiDirectionStateManager::pointer_type p_right) const
{
    auto left  = static_cast<typename map_type::value_type*>(p_left);
    auto right = static_cast<typename map_type::value_type*>(p_right);
    /* Compare greater than (`operator >`) to turn max-heap into min-heap. */
    return left->first.g() + left->second.h > right->first.g() + right->second.h;
}

template<
        heuristic_search_state State,
        typename Expand,
        typename Expand2,
        typename Heuristic,
        typename Heuristic2,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
struct biDirectionalSearch
{
    using state_type = State;
    using expand_type = Expand;
    using expand_type2 = Expand2;
    using heuristic_type = Heuristic;
    using heuristic_type2 = Heuristic2;

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

    // decide the core details in queue
    BiDirectionStateManager</* State=           */ State,
            /* HasRegularQueue= */ true,
            /* HasBeamQueue=    */ false,
            /* Config=          */ Config,
            /* Context...=      */ Context...
    > state_manager_bottomup;

    BiDirectionStateManager</* State=           */ State,
            /* HasRegularQueue= */ true,
            /* HasBeamQueue=    */ false,
            /* Config=          */ Config,
            /* Context...=      */ Context...
    > state_manager_topdown;

public:
    explicit biDirectionalSearch(Context&... context)
            : state_manager_bottomup(context...),state_manager_topdown(context...)
    {}

    biDirectionalSearch(const biDirectionalSearch&) = delete;
    biDirectionalSearch(biDirectionalSearch&&) = default;

    biDirectionalSearch & operator=(biDirectionalSearch&&) = default;

    /** Search for a path from the given `initial_state` to a goal state.  Uses the given heuristic to guide the search.
     *
     * @return the cost of the computed path from `initial_state` to a goal state
     */
    const State &search(state_type bottom_state, state_type top_state,
                        expand_type expand, expand_type2 expand2,
                        heuristic_type &heuristic, heuristic_type2 &heuristic2,
                        Context &... context);

    /** Resets the state of the search. */
    void clear() {
        state_manager_bottomup.clear();
        state_manager_topdown.clear();
    }

private:
    /*------------------------------------------------------------------------------------------------------------------
     * Helper methods
     *----------------------------------------------------------------------------------------------------------------*/

    template<typename T>
    using has_mark = decltype(std::declval<T>().mark(Subproblem()));


    void bidirectional_for_each_successor_bottomup(callback_t &&callback,
                                                   const state_type &state, heuristic_type &heuristic,
                                                   expand_type &expand,
                                                   Context &... context) {
        expand(state, [this, callback = std::move(callback), &heuristic, &context...](state_type successor) {
            if (auto it = state_manager_bottomup.find(successor, context...);
                    it == state_manager_bottomup.end(successor, context...)) {
                const double h = heuristic(successor, context...);
                callback(std::move(successor), h);
            } else {
                inc_cached_heuristic_value();
                callback(std::move(successor), it->second.h);
            }
        }, context...);
    }

    void bidirectional_for_each_successor_topdown(callback_t &&callback,
                                                  const state_type &state, heuristic_type2 &heuristic2,
                                                  expand_type2 &expand2,
                                                  Context &... context) {
        expand2(state, [this, callback = std::move(callback), &heuristic2, &context...](state_type successor) {
            if (auto it = state_manager_topdown.find(successor, context...); it == state_manager_topdown.end(successor, context...)) {
                const double h = heuristic2(successor, context...);
                callback(std::move(successor), h);
            } else {
                inc_cached_heuristic_value();
                callback(std::move(successor), it->second.h);
            }
        }, context...);
    }

    void explore_state_bottomup(const state_type &state, heuristic_type &heuristic, expand_type &expand,
                                Context &... context) {
        bidirectional_for_each_successor_bottomup([this, &context...](state_type successor, double h) {
            state_manager_bottomup.push_regular_queue(std::move(successor), h, context...);
        }, state, heuristic, expand, context...);
    }

    void explore_state_topdown(const state_type &state, heuristic_type2 &heuristic, expand_type2 &expand2,
                               Context &... context) {
        bidirectional_for_each_successor_topdown([this, &context...](state_type successor, double h) {
            state_manager_topdown.push_regular_queue(std::move(successor), h, context...);
        }, state, heuristic, expand2, context...);
    }

    const state_type &
    reverse_the_topdown(const state_type &topdown_current_state, const state_type &bottomup_state_to_connect) {
        /// Return the goal -> State Top
        const state_type *current = &topdown_current_state;
        const state_type *prev = &bottomup_state_to_connect;

        while (current) {
            const state_type *parent = current->parent();
            current->set_parent(prev);
            prev = current;
            current = parent;
        }
        return *prev;
    }

    const state_type &
    reverse_the_middle_case(const state_type &topdown_current_state, const state_type &bottomup_state_to_connect) {
        /// Return the goal -> State Top
        /// Currently in the same layer

        const state_type *tmp = &topdown_current_state;
        const state_type *current = tmp->parent();
        const state_type *prev = &bottomup_state_to_connect;

        while (current) {
            const state_type *parent = current->parent();
            current->set_parent(prev);
            prev = current;
            current = parent;
        }
        return *prev;
    }


public:
    friend std::ostream &operator<<(std::ostream &out, const biDirectionalSearch &AStar) {
        return out << AStar.state_manager_ << ", used cached heuristic value " << AStar.num_cached_heuristic_value()
                   << " times";
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }

    void dump() const { dump(std::cerr); }
};

template<
        heuristic_search_state State,
        typename Expand,
        typename Expand2,
        typename Heuristic,
        typename Heuristic2,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
const State &biDirectionalSearch<State, Expand, Expand2, Heuristic, Heuristic2, Config, Context...>::search(
        state_type bottom_state,
        state_type top_state,
        expand_type expand,
        expand_type2 expand2,
        heuristic_type &heuristic,
        heuristic_type2 &heuristic2,
        Context &... context
) {
    /// Core: we will not change any reconstruct logic in the outside
    /// Current is in BottomUpComplete: So we should return a top states
    /// 1. Init the Bidirectional State Manager
    /// Including front and back - two direction, init and push element - two operations
    /// We can ignore the input initial_state
    state_manager_bottomup.template push<false>(std::move(bottom_state), 0, context...);
    state_manager_topdown.template push<false>(std::move(top_state), 0, context...);
    /// 2. while loop
    while (not state_manager_bottomup.queues_empty() && not state_manager_topdown.queues_empty()) {
        auto bottomup_node = state_manager_bottomup.pop();
        const state_type &bottomup_state = bottomup_node.first;
        auto topdown_node = state_manager_topdown.pop();
        const state_type &topdown_state = topdown_node.first;

        /// 2. Exit case
        size_t diff = bottomup_state.size() - topdown_state.size();
        std::cout << "diff=" << diff
                  << "\tbottomup_state.size()" << bottomup_state.size()
                  << "\ttopdown_state.size()" << topdown_state.size() << "\n";

        assert(diff >= 0 && "Reach minus!");

        // Currently not using diff==1, we have found diff==0 method has better connection
        //        if (diff == 1) {
        //            std::cout << "Currently diff==1 Processing...\n";
        //            /// Build connection from bottomup_state to topdown_state
        //            const state_type &goal = reverse_the_topdown(topdown_state, bottomup_state);
        //            return goal;
        //        }
        if (diff == 0) {
            std::cout << "!! diff==0 Processing... \n";
            /// Naive version of logic
            const state_type &goal = reverse_the_middle_case(topdown_state, bottomup_state);
            return goal;
        }

        /// 3. Bidirectionally extend to step forward
        explore_state_bottomup(bottomup_state, heuristic, expand, context...);
        explore_state_topdown(topdown_state, heuristic2, expand2, context...);
    }
    throw std::logic_error("goal state unreachable from provided initial state");
}

/////////////////////////////////////////////////////////////////////////////////////////
//////////////////////layeredBiDirectionSearch -> TopDown & BottomUp/////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

template<
        heuristic_search_state State,
        bool HasRegularQueue,
        bool HasBeamQueue,
        typename Config,
        typename... Context
>
struct LayeredBiDirectionSearchStateManager
{
    static_assert(HasRegularQueue or HasBeamQueue, "must have at least one kind of queue");

    using state_type = State;
    static constexpr bool has_regular_queue = HasRegularQueue;
    static constexpr bool has_beam_queue = HasBeamQueue;

    static constexpr bool detect_duplicates = true;

private:
    ///> type for a pointer to an entry in the map of states
    using pointer_type = void*;
    ///> comparator for map entries based on states' *g + h* value
    struct comparator { bool operator()(pointer_type p_left, pointer_type p_right) const; };
    ///> the type of heap to implement the priority queues
    using heap_type = typename Config::template heap_type<pointer_type, typename Config::template compare<comparator>>;

/*----- Counters -------------------------------------------------------------------------------------------------*/
#ifndef NDEBUG
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
            /* Allocator= */ typename Config::template allocator_type<map_value_type>
    >;

    /*----- Helper type to manage potentially partitioned states. ----------------------------------------------------*/
    template<bool Partition>
    struct Partitions;

    template<>
    struct Partitions<true>
    {
        std::vector<map_type> partitions_;

        Partitions(Context&... context) : partitions_(state_type::num_partitions(context...)) { }
        Partitions(const Partitions&) = delete;
        Partitions(Partitions&&) = default;

        ~Partitions() {
            if (Options::Get().statistics) {
                std::cout << partitions_.size() << " partitions:";
                for (auto &P : partitions_)
                    std::cout << "\n  " << P.size();
                std::cout << std::endl;
            }
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
    Partitions<supports_partitioning<State, Context...>> partitions_;

public:
    ///> map of all states ever explored, mapping state to its info
    // map_type states_;
    heap_type regular_queue_;
    heap_type beam_queue_;

    LayeredBiDirectionSearchStateManager(Context&... context) : partitions_(context...) { }
    LayeredBiDirectionSearchStateManager(const LayeredBiDirectionSearchStateManager&) = delete;
    LayeredBiDirectionSearchStateManager(LayeredBiDirectionSearchStateManager&&) = default;
    LayeredBiDirectionSearchStateManager & operator=(LayeredBiDirectionSearchStateManager&&) = default;

    template<bool ToBeamQueue>
    void push(state_type state, double h, Context&... context) {
        static_assert(not ToBeamQueue or HasBeamQueue, "ToBeamQueue implies HasBeamQueue");
        static_assert(ToBeamQueue or HasRegularQueue, "not ToBeamQueue implies HasRegularQueue");

        auto &Q = ToBeamQueue ? beam_queue_ : regular_queue_;
        auto &P = partition(state, context...);

        if constexpr (detect_duplicates) {
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
                    if (state.g() < it->first.g())
                        it->first.decrease_g(state.parent(), state.g()); // update *g* value
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
                    it->first.decrease_g(state.parent(), state.g()); // decrease value of *g*
                    if (it->second.queue == nullptr) {
                        /*----- The state is currently not present in a queue. -----*/
                        it->second.handle = Q.push(&*it); // add to dedicated queue and update handle
                        it->second.queue = &Q; // update queue
                    } else {
                        /*----- Update the state's entry in the queue. -----*/
                        M_insist(it->second.queue == &Q, "the state must already be in its destinated queue");
                        Q.increase(it->second.handle); // we need to *increase* because the heap is a max-heap
                        inc_decrease_key();
                    }
                }
            }
        } else {
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
        }
    }

    void push_regular_queue(state_type state, double h, Context&... context) {
        if constexpr (detect_duplicates) {
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
                        it->first.decrease_g(state.parent(), state.g());
                        if (it->second.queue == &beam_queue_) { // if state is currently in a queue
                            it->second.queue->increase(it->second.handle); // *increase* because max-heap
                            inc_decrease_key();
                        }
                    } else {
                        inc_discarded();
                    }
                }
            }
        } else {
            push<not HasRegularQueue>(std::move(state), h, context...);
        }
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

    friend std::ostream & operator<<(std::ostream &out, const LayeredBiDirectionSearchStateManager &SM) {
#ifndef NDEBUG
        SM.print_counters(out);
        out << ", ";
#endif
        out << SM.partitions_.size() << " seen, " << SM.regular_queue_.size() << " currently in regular queue, "
            << SM.beam_queue_.size() << " currently in beam queue";
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

template<
        heuristic_search_state State,
        bool HasRegularQueue,
        bool HasBeamQueue,
        typename Config,
        typename... Context
>
bool LayeredBiDirectionSearchStateManager<State, HasRegularQueue, HasBeamQueue, Config, Context...>::comparator::
operator()(LayeredBiDirectionSearchStateManager::pointer_type p_left, LayeredBiDirectionSearchStateManager::pointer_type p_right) const
{
    auto left  = static_cast<typename map_type::value_type*>(p_left);
    auto right = static_cast<typename map_type::value_type*>(p_right);
    /* Compare greater than (`operator >`) to turn max-heap into min-heap. */
    return left->first.g() + left->second.h > right->first.g() + right->second.h;
}

template<
        heuristic_search_state State,
        typename Expand,
        typename Expand2,
        typename Heuristic,
        typename Heuristic2,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
struct layeredBiDirectionSearch
{
    using state_type = State;
    using expand_type = Expand;
    using expand_type2 = Expand2;
    using heuristic_type = Heuristic;
    using heuristic_type2 = Heuristic2;

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

    // decide the core details in queue
    LayeredBiDirectionSearchStateManager</* State=           */ State,
            /* HasRegularQueue= */ true,
            /* HasBeamQueue=    */ false,
            /* Config=          */ Config,
            /* Context...=      */ Context...
    > state_manager_bottomup;

    LayeredBiDirectionSearchStateManager</* State=           */ State,
            /* HasRegularQueue= */ true,
            /* HasBeamQueue=    */ false,
            /* Config=          */ Config,
            /* Context...=      */ Context...
    > state_manager_topdown;

public:
    explicit layeredBiDirectionSearch(Context&... context)
            : state_manager_bottomup(context...),state_manager_topdown(context...)
    {}

    layeredBiDirectionSearch(const layeredBiDirectionSearch&) = delete;
    layeredBiDirectionSearch(layeredBiDirectionSearch&&) = default;

    layeredBiDirectionSearch & operator=(layeredBiDirectionSearch&&) = default;

    /** Search for a path from the given `initial_state` to a goal state.  Uses the given heuristic to guide the search.
     *
     * @return the cost of the computed path from `initial_state` to a goal state
     */
    const State &search(state_type bottom_state, state_type top_state,
                        expand_type expand, expand_type2 expand2,
                        heuristic_type &heuristic, heuristic_type2 &heuristic2,
                        Context &... context);

    /** Resets the state of the search. */
    void clear() {
        state_manager_bottomup.clear();
        state_manager_topdown.clear();
    }

private:
    /*------------------------------------------------------------------------------------------------------------------
     * Helper methods
     *----------------------------------------------------------------------------------------------------------------*/

    template<typename T>
    using has_mark = decltype(std::declval<T>().mark(Subproblem()));


    void bidirectional_for_each_successor_bottomup(callback_t &&callback,
                                                   const state_type &state, heuristic_type &heuristic,
                                                   expand_type &expand,
                                                   Context &... context) {
        expand(state, [this, callback = std::move(callback), &heuristic, &context...](state_type successor) {
            if (auto it = state_manager_bottomup.find(successor, context...);
                    it == state_manager_bottomup.end(successor, context...)) {
                const double h = heuristic(successor, context...);
                callback(std::move(successor), h);
            } else {
                inc_cached_heuristic_value();
                callback(std::move(successor), it->second.h);
            }
        }, context...);
    }

    void bidirectional_for_each_successor_topdown(callback_t &&callback,
                                                  const state_type &state, heuristic_type2 &heuristic2,
                                                  expand_type2 &expand2,
                                                  Context &... context) {
        expand2(state, [this, callback = std::move(callback), &heuristic2, &context...](state_type successor) {
            if (auto it = state_manager_topdown.find(successor, context...); it == state_manager_topdown.end(successor, context...)) {
                const double h = heuristic2(successor, context...);
                callback(std::move(successor), h);
            } else {
                inc_cached_heuristic_value();
                callback(std::move(successor), it->second.h);
            }
        }, context...);
    }

    void explore_state_bottomup(const state_type &state, heuristic_type &heuristic, expand_type &expand,
                                Context &... context) {
        bidirectional_for_each_successor_bottomup([this, &context...](state_type successor, double h) {
            state_manager_bottomup.push_regular_queue(std::move(successor), h, context...);
        }, state, heuristic, expand, context...);
    }

    void explore_state_topdown(const state_type &state, heuristic_type2 &heuristic, expand_type2 &expand2,
                               Context &... context) {
        bidirectional_for_each_successor_topdown([this, &context...](state_type successor, double h) {
            state_manager_topdown.push_regular_queue(std::move(successor), h, context...);
        }, state, heuristic, expand2, context...);
    }

    /*
     * Return the goal -> State Top
     * Currently topdown state & bottomup state are in the same layer
     *      topdown state
     *          - marked/joined = 23, S1 = 2, S2 = 21, subproblems_ = [2, 8, 21], layer/size = 3
     *          - subproblems_ -> How to build the graph from current state to the top
     *      bottomup state
     *          - marked/joined = 21, subproblems_ = [2, 16, 21], layer/size = 3
     *          - subproblems_ -> Current graph structure
     */
    const state_type &
    reverse_the_middle_case(const state_type &topdown_current_state, const state_type &bottomup_state_to_connect) {
        /// TODO: Add comparation to better utilized the code
//        bool canConnect = [&topdown_current_state, &bottomup_state_to_connect] {
//            if (topdown_current_state.size() != bottomup_state_to_connect.size()) {
//                return false;
//            }
//
//            bool found = [&topdown_current_state,&bottomup_state_to_connect]() {
//                auto & parent = *topdown_current_state.parent();
//                size_t times = parent.size();
//
//                std::array<Subproblem, 2> D = [&bottomup_state_to_connect,&parent]() {
//                    std::array<Subproblem, 2> delta;
//                    M_insist(bottomup_state_to_connect.size() == parent.size() + 1);
//                    auto after_it = parent.cbegin();
//                    auto before_it = bottomup_state_to_connect.cbegin();
//                    auto out_it = delta.begin();
//
//                    while (out_it != delta.end()) {
//                        M_insist(before_it != bottomup_state_to_connect.cend());
//                        if (after_it == parent.cend()) {
//                            *out_it++ = *before_it++;
//                        } else if (*before_it == *after_it) {
//                            ++before_it;
//                            ++after_it;
//                        } else {
//                            *out_it++ = *before_it++;
//                        }
//                    }
//                    return delta;
//                }();
//
//                auto target = D[0].get_bits() | D[1].get_bits();
//
//                for (size_t i{0}; i < times; i++) {
//                    if (parent.get_subproblems()[i].get_bits() == target) {
//                        return true;
//                    }
//                }
//                return false;
//            }();
//            if (not found) { return false; }
//            return true;
//        }();
//
//        assert(canConnect == true && "\n\n\nYou can not connect them\n\n\n");

        const state_type *tmp = &topdown_current_state;
        const state_type *current = tmp->parent();
        const state_type *prev = &bottomup_state_to_connect;

        while (current) {
            const state_type *parent = current->parent();
            current->set_parent(prev);
            prev = current;
            current = parent;
        }
        return *prev;
    }


public:
    friend std::ostream &operator<<(std::ostream &out, const layeredBiDirectionSearch &AStar) {
        return out << AStar.state_manager_ << ", used cached heuristic value " << AStar.num_cached_heuristic_value()
                   << " times";
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }

    void dump() const { dump(std::cerr); }
};

template<
        heuristic_search_state State,
        typename Expand,
        typename Expand2,
        typename Heuristic,
        typename Heuristic2,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
const State &layeredBiDirectionSearch<State, Expand, Expand2, Heuristic, Heuristic2, Config, Context...>::search(
        state_type bottom_state,
        state_type top_state,
        expand_type expand,
        expand_type2 expand2,
        heuristic_type &heuristic,
        heuristic_type2 &heuristic2,
        Context &... context
)
{
    std::cout << "\nCurrently in the entrance for the LayeredBiDirectional" << std::endl;
    /// Core: we will not change any reconstruct logic in the outside
    /// Current is in BottomUpComplete: So we should return a top states
    /// 1. Init the Bidirectional State Manager
    /// Including front and back - two direction, init and push element - two operations
    /// We can ignore the input initial_state
    state_manager_bottomup.template push<false>(std::move(bottom_state), 0, context...);
    state_manager_topdown.template push<false>(std::move(top_state), 0, context...);
    /// 2. while loop
    while (not state_manager_bottomup.queues_empty() && not state_manager_topdown.queues_empty()) {
        auto bottomup_node = state_manager_bottomup.pop();
        const state_type &bottomup_state = bottomup_node.first;
        auto topdown_node = state_manager_topdown.pop();
        const state_type &topdown_state = topdown_node.first;

        /// 2. Exit case
        size_t diff = bottomup_state.size() - topdown_state.size();
        std::cout << "diff=" << diff
                  << "\tbottomup_state.size()" << bottomup_state.size()
                  << "\ttopdown_state.size()" << topdown_state.size() << "\n";

        assert(diff >= 0 && "Reach minus!");

        if (diff == 0) {
            std::cout << "!! diff==0 Processing... \n";
            /// Naive version of logic
            const state_type &goal = reverse_the_middle_case(topdown_state, bottomup_state);
            return goal;
        }

        /// 3. Bidirectionally extend to step forward
        explore_state_bottomup(bottomup_state, heuristic, expand, context...);
        explore_state_topdown(topdown_state, heuristic2, expand2, context...);
//        std::thread thread1(&layeredBiDirectionSearch::explore_state_bottomup, bottomup_state, heuristic, expand,
//                            context...);
//        std::thread thread2(&layeredBiDirectionSearch::explore_state_topdown, topdown_state, heuristic2, expand2,
//                            context...);
//
//        thread1.join();
//        thread2.join();

    }

    throw std::logic_error("goal state unreachable from provided initial state");
}

/////////////////////////////////////////////////////////////////////////////////////////
//////////////////////Layered Search -> For Reuse////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
/** Tracks states and their presence in queues. */
template<
        heuristic_search_state State,
        bool HasRegularQueue,
        bool HasBeamQueue,
        typename Config,
        typename... Context
>
struct LayeredStateManager
{
    static_assert(HasRegularQueue or HasBeamQueue, "must have at least one kind of queue");

    using state_type = State;
    static constexpr bool has_regular_queue = HasRegularQueue;
    static constexpr bool has_beam_queue = HasBeamQueue;

    static constexpr bool detect_duplicates = true;

private:
    ///> type for a pointer to an entry in the map of states
    using pointer_type = void*;
    ///> comparator for map entries based on states' *g + h* value
    struct comparator { bool operator()(pointer_type p_left, pointer_type p_right) const; };
    ///> the type of heap to implement the priority queues
    using heap_type = typename Config::template heap_type<pointer_type, typename Config::template compare<comparator>>;

    /*----- Counters -------------------------------------------------------------------------------------------------*/
#ifndef NDEBUG
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
            /* Allocator= */ typename Config::template allocator_type<map_value_type>
    >;

    /*----- Helper type to manage potentially partitioned states. ----------------------------------------------------*/
    template<bool Partition>
    struct Partitions;

    template<>
    struct Partitions<true>
    {
        std::vector<map_type> partitions_;

        Partitions(Context&... context) : partitions_(state_type::num_partitions(context...)) { }
        Partitions(const Partitions&) = delete;
        Partitions(Partitions&&) = default;

        ~Partitions() {
            if (Options::Get().statistics) {
                std::cout << partitions_.size() << " partitions:";
                for (auto &P : partitions_)
                    std::cout << "\n  " << P.size();
                std::cout << std::endl;
            }
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
    Partitions<supports_partitioning<State, Context...>> partitions_;

    ///> map of all states ever explored, mapping state to its info
    // map_type states_;

    heap_type regular_queue_;
    heap_type beam_queue_;

public:
    LayeredStateManager(Context&... context) : partitions_(context...) { }
    LayeredStateManager(const LayeredStateManager&) = delete;
    LayeredStateManager(LayeredStateManager&&) = default;
    LayeredStateManager & operator=(LayeredStateManager&&) = default;

    template<bool ToBeamQueue>
    void push(state_type state, double h, Context&... context) {
        static_assert(not ToBeamQueue or HasBeamQueue, "ToBeamQueue implies HasBeamQueue");
        static_assert(ToBeamQueue or HasRegularQueue, "not ToBeamQueue implies HasRegularQueue");

        auto &Q = ToBeamQueue ? beam_queue_ : regular_queue_;
        auto &P = partition(state, context...);

        if constexpr (detect_duplicates) {
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
                    if (state.g() < it->first.g())
                        it->first.decrease_g(state.parent(), state.g()); // update *g* value
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
                    it->first.decrease_g(state.parent(), state.g()); // decrease value of *g*
                    if (it->second.queue == nullptr) {
                        /*----- The state is currently not present in a queue. -----*/
                        it->second.handle = Q.push(&*it); // add to dedicated queue and update handle
                        it->second.queue = &Q; // update queue
                    } else {
                        /*----- Update the state's entry in the queue. -----*/
                        M_insist(it->second.queue == &Q, "the state must already be in its destinated queue");
                        Q.increase(it->second.handle); // we need to *increase* because the heap is a max-heap
                        inc_decrease_key();
                    }
                }
            }
        } else {
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
        }
    }

    void push_regular_queue(state_type state, double h, Context&... context) {
        if constexpr (detect_duplicates) {
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
                        it->first.decrease_g(state.parent(), state.g());
                        if (it->second.queue == &beam_queue_) { // if state is currently in a queue
                            it->second.queue->increase(it->second.handle); // *increase* because max-heap
                            inc_decrease_key();
                        }
                    } else {
                        inc_discarded();
                    }
                }
            }
        } else {
            push<not HasRegularQueue>(std::move(state), h, context...);
        }
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

    friend std::ostream & operator<<(std::ostream &out, const LayeredStateManager &SM) {
#ifndef NDEBUG
        SM.print_counters(out);
        out << ", ";
#endif
        out << SM.partitions_.size() << " seen, " << SM.regular_queue_.size() << " currently in regular queue, "
            << SM.beam_queue_.size() << " currently in beam queue";
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

template<
        heuristic_search_state State,
        bool HasRegularQueue,
        bool HasBeamQueue,
        typename Config,
        typename... Context
>
bool LayeredStateManager<State, HasRegularQueue, HasBeamQueue, Config, Context...>::comparator::
operator()(LayeredStateManager::pointer_type p_left, LayeredStateManager::pointer_type p_right) const
{
    auto left  = static_cast<typename map_type::value_type*>(p_left);
    auto right = static_cast<typename map_type::value_type*>(p_right);
    /* Compare greater than (`operator >`) to turn max-heap into min-heap. */
    return left->first.g() + left->second.h > right->first.g() + right->second.h;
}


template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        unsigned BeamWidth,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
struct layeredSearch
{
    using state_type = State;
    using expand_type = Expand;
    using heuristic_type = Heuristic;

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

    // decide the core details in queue
    LayeredStateManager
            </* State=           */ State,
            /* HasRegularQueue= */ true,
            /* HasBeamQueue=    */ false,
            /* Config=          */ Config,
            /* Context...=      */ Context...
    > state_manager_;


public:
    explicit layeredSearch(Context&... context)
            : state_manager_(context...)
    {}

    layeredSearch(const layeredSearch&) = delete;
    layeredSearch(layeredSearch&&) = default;

    layeredSearch & operator=(layeredSearch&&) = default;

    /** Search for a path from the given `initial_state` to a goal state.  Uses the given heuristic to guide the search.
     *
     * @return the cost of the computed path from `initial_state` to a goal state
     */
    const State &search(state_type initial_state, expand_type expand, heuristic_type &heuristic, Context &... context);

    /** Resets the state of the search. */
    void clear() {
        state_manager_.clear();
    }

private:
    /*------------------------------------------------------------------------------------------------------------------
     * Helper methods
     *----------------------------------------------------------------------------------------------------------------*/

    template<typename T>
    using has_mark = decltype(std::declval<T>().mark(Subproblem()));


    void for_each_successor(callback_t &&callback, const state_type &state, heuristic_type &heuristic,
                            expand_type &expand, Context &... context) {
        expand(state, [this, callback=std::move(callback), &heuristic, &context...](state_type successor) {
            if (auto it = state_manager_.find(successor, context...);it == state_manager_.end(successor, context...)) {
                const double h = heuristic(successor, context...);
                callback(std::move(successor), h);
            } else {
                inc_cached_heuristic_value();
                callback(std::move(successor), it->second.h);
            }
        }, context...);
    }

    void explore_state(const state_type &state, heuristic_type &heuristic, expand_type &expand, Context&... context) {
        for_each_successor([this, &context...](state_type successor, double h) {
            state_manager_.push_regular_queue(std::move(successor), h, context...);
        }, state, heuristic, expand, context...);
    }

public:
    friend std::ostream & operator<<(std::ostream &out, const layeredSearch &AStar) {
        return out << AStar.state_manager_ << ", used cached heuristic value " << AStar.num_cached_heuristic_value()
                   << " times";
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }


};

template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        unsigned BeamWidth,
        typename Config,
        typename... Context
>
requires heuristic_search_heuristic<Heuristic, Context...>
const State &layeredSearch<State, Expand, Heuristic, BeamWidth, Config, Context...>::search(
        state_type initial_state,
        expand_type expand,
        heuristic_type &heuristic,
        Context &... context
) {
    state_manager_.template push<false>(std::move(initial_state), 0, context...);
    while (not state_manager_.queues_empty()) {
        auto top = state_manager_.pop();
        const state_type &state = top.first;
        std::cout << "Current state.size()" << state.size() << "\n";

        if (expand.is_goal(state, context...))
            return state;
        explore_state(state, heuristic, expand, context...);
    }
    throw std::logic_error("goal state unreachable from provided initial state");

//    /// Below change to my code
//    /* Initialize queue with initial state. */
//    state_manager_.template push<true>(std::move(initial_state),0,context...);
//
//    /* Run work list algorithm. */
//    while (not state_manager_.queues_empty()) {
//        M_insist(not (is_monotone and use_beam_search) or not state_manager_.is_beam_queue_empty(),
//                 "the beam queue must not run empty with beam search on a monotone search space");
//        auto top = state_manager_.pop();
//        const state_type &state = top.first;
//
//        if (expand.is_goal(state, context...))
//            return state;
//
//        explore_state(state, heuristic, expand, context...);
//    }
//
//    throw std::logic_error("goal state unreachable from provided initial state");
}

/////////////////////////////////////////////////////////////////////////////////////////
//////////////////////Definition -> Heuristic Algorithm//////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////
template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        typename Config,
        typename... Context
>
using cleanAStar = cleanAStarSearch<State, Expand, Heuristic, Config, Context...>;

//template<
//        heuristic_search_state State,
//        typename Expand,
//        typename Expand2,
//        typename Heuristic,
//        typename Heuristic2,
//        typename Config,
//        typename... Context
//>
//using BIDIRECTIONAL = biDirectionalSearch<State, Expand, Expand2, Heuristic, Heuristic2, Config, Context...>;

template<unsigned BeamWidth>
struct hanwen_layeredSearch
{
    template<
            heuristic_search_state State,
            typename Expand,
            typename Heuristic,
            typename Config,
            typename... Context
    >
    using type = layeredSearch<State, Expand, Heuristic, BeamWidth, Config, Context...>;
};


template<
    heuristic_search_state State,
    typename Expand,
    typename Heuristic,
    typename Config,
    typename... Context
>
using IDDFS = hanwenSearch<State, Expand, Heuristic, true, Config, Context...>;

template<
    heuristic_search_state State,
    typename Expand,
    typename Heuristic,
    typename Config,
    typename... Context
>
using AStar = genericAStar<State, Expand, Heuristic, std::ratio<1, 1>, 0, false, false, Config, Context...>;

template<
    heuristic_search_state State,
    typename Expand,
    typename Heuristic,
    typename Config,
    typename... Context
>
using lazy_AStar = genericAStar<State, Expand, Heuristic, std::ratio<1, 1>, 0, true, false, Config, Context...>;

template<typename Weight>
struct wAStar
{
    template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        typename Config,
        typename... Context
    >
    using type = genericAStar<State, Expand, Heuristic, Weight, 0, false, false, Config, Context...>;
};

template<typename Weight>
struct lazy_wAStar
{
    template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        typename Config,
        typename... Context
    >
    using type = genericAStar<State, Expand, Heuristic, Weight, 0, true, false, Config, Context...>;
};

template<unsigned BeamWidth>
struct beam_search
{
    template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        typename Config,
        typename... Context
    >
    using type = genericAStar<State, Expand, Heuristic, std::ratio<1, 1>, BeamWidth, false, false, Config, Context...>;
};

template<unsigned BeamWidth>
struct lazy_beam_search
{
    template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        typename Config,
        typename... Context
    >
    using type = genericAStar<State, Expand, Heuristic, std::ratio<1, 1>, BeamWidth, true, false, Config, Context...>;
};

template<unsigned BeamWidth>
struct monotone_beam_search
{
    template<
        heuristic_search_state State,
        typename Expand,
        typename Heuristic,
        typename Config,
        typename... Context
    >
    using type = genericAStar<State, Expand, Heuristic, std::ratio<1, 1>, BeamWidth, false, true, Config, Context...>;
};

}

}