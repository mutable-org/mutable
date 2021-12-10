#pragma once

#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <string>
#include <utility>
#include <vector>


namespace m {

/** Collect timings of events. */
struct Timer
{
    /** A proxy class to record a `Measurement` with a `Timer`. */
    struct TimingProcess
    {
        friend struct Timer;

        private:
        Timer &timer_; ///< the `Timer` instance
        std::size_t id_; ///< the ID of the `Measurement`

        TimingProcess(Timer &timer, std::size_t index) : timer_(timer), id_(index) { }

        public:
        ~TimingProcess() { timer_.stop(id_); }
    };

    using clock = std::chrono::high_resolution_clock;
    using duration = clock::duration;
    using time_point = clock::time_point;

    struct Measurement
    {
        std::string name; ///< the name of this `Measurement`
        time_point begin, end; ///< the begin and end time points of this `Measurement`

        explicit Measurement(std::string name) : name(std::move(name)) { }
        Measurement(std::string name, time_point begin) : name(std::move(name)), begin(std::move(begin)) { }
        Measurement(std::string name, time_point begin, time_point end)
            : name(std::move(name))
            , begin(std::move(begin))
            , end(std::move(end))
        { }

        /** Clear this `Measurement`, rendering it unused. */
        void clear() { begin = end = time_point(); }

        /** Start this `Measurement` by setting the start time point to *NOW*. */
        void start() {
            M_insist(is_unused());
            begin = clock::now();
        }

        /** Stop this `Measurement` by setting the end time point to *NOW*. */
        void stop() {
            M_insist(is_active());
            end = clock::now();
        }

        /** Returns `true` iff the `Measurement` is unused, i.e. has not started (and hence also not finished). */
        bool is_unused() const {
            M_insist(begin != time_point() or end == time_point(),
                   "if the measurement hasn't started it must not have ended");
            return begin == time_point();
        }
        /** Returns `true` iff this `Measurement` has begun. */
        bool has_started() const { return begin != time_point(); }
        /** Returns `true` iff this `Measurement` has ended. */
        bool has_ended() const { return end != time_point(); }
        /** Returns `true` iff this `Measurement` is currently in process. */
        bool is_active() const { return has_started() and not has_ended(); }
        /** Returns `true` iff this `Measurement` has completed. */
        bool is_finished() const { return has_started() and has_ended(); }

        /** Returns the duration of a *finished* `Measurement`. */
        duration duration() const {
            M_insist(is_finished(), "can only compute duration of finished measurements");
            return end - begin;
        }

        friend std::ostream & operator<<(std::ostream &out, const Measurement &M);
        void dump(std::ostream &out) const;
        void dump() const;
    };

    private:
    std::vector<Measurement> measurements_;

    public:
    auto begin() const { return measurements_.cbegin(); }
    auto end()   const { return measurements_.cend(); }
    auto cbegin() const { return measurements_.cbegin(); }
    auto cend()   const { return measurements_.cend(); }

    const std::vector<Measurement> & measurements() const { return measurements_; }
    const Measurement & get(std::size_t i) const {
        if (i >= measurements_.size())
            throw m::out_of_range("index i out of bounds");
        return measurements_[i];
    }
    const Measurement & get(const std::string &name) const {
        auto it = std::find_if(measurements_.begin(), measurements_.end(),
                               [&](auto &elem) { return elem.name == name; });
        if (it == measurements_.end())
            throw m::out_of_range("a measurement with that name does not exist");
        return *it;
    }

    /** Erase all `Measurement`s from this `Timer`. */
    void clear() { measurements_.clear(); }

    private:
    /** Start a new `Measurement` with the name `name`.  Returns the ID assigned to that `Measurement`. */
    std::size_t start(std::string name) {
        auto it = std::find_if(measurements_.begin(), measurements_.end(),
                               [&](auto &elem) { return elem.name == name; });

        if (it != measurements_.end()) { // overwrite existing, finished measurement
            if (it->is_active())
                throw m::invalid_argument("a measurement with that name is already in progress");
            const auto idx = std::distance(measurements_.begin(), it);
            it->end = time_point();
            it->begin = clock::now();
            return idx;
        } else { // create new measurement
            auto id = measurements_.size();
            auto &M = measurements_.emplace_back(name);
            M.start();
            return id;
        }
    }

    /** Stops the `Measurement` with the given ID. */
    void stop(std::size_t id) {
        if (id >= measurements_.size())
            throw m::out_of_range("id out of bounds");
        auto &M = measurements_[id];
        if (M.has_ended())
            throw m::invalid_argument("cannot stop that measurement because it has already been stopped");
        M.stop();
    }

    /** Erase a `Measurement` from this `Timer`. */
    void erase(std::size_t id) {
        if (id >= measurements_.size())
            throw m::out_of_range("id out of bounds");
        auto &M = measurements_[id];
        M.clear(); // just clear it, don't remove it to avoid moving other measurements within the container
    }

    public:
    /** Creates a new `TimingProcess` with the given `name`. */
    TimingProcess create_timing(std::string name) { return TimingProcess(*this, /* ID= */ start(name)); }

    /** Print all finished and in-process timings of `timer` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, const Timer &timer) {
        out << "Timer measurements:\n";
        for (auto &M : timer)
            out << "  " << M << '\n';

        return out;
    }

    void dump(std::ostream &out) const;
    void dump() const;

    /** Computes the total duration of all `Measurement`s. */
    duration total() const {
        duration d(0);
        for (auto &m : measurements_)
            d += m.end - m.begin;
        return d;
    }
};

#define M_TIME_EXPR(EXPR, DESCR, TIMER) \
    [&]() { auto TP = (TIMER).create_timing((DESCR)); return (EXPR); }();
#define M_TIME_BLOCK(DESCR, TIMER, BLOCK) {\
    auto TP = (TIMER).create_timing((DESCR)); \
    BLOCK \
}
#define M_TIME_THIS(DESCR, TIMER) \
    auto M_PASTE(__timer_, __COUNTER__) = (TIMER).create_timing((DESCR));

}
