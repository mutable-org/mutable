#pragma once

#include "util/fn.hpp"
#include "util/macro.hpp"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <string>
#include <utility>
#include <vector>


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

    private:
    struct Measurement
    {
        std::string name; ///< the name of this `Measurement`
        time_point begin, end; ///< the begin and end time points of this `Measurement`
    };
    std::vector<Measurement> measurements_;

    public:
    auto begin() const { return measurements_.cbegin(); }
    auto end()   const { return measurements_.cend(); }
    auto cbegin() const { return measurements_.cbegin(); }
    auto cend()   const { return measurements_.cend(); }

    auto & measurements() const { return measurements_; }
    auto & get(std::size_t i) const { insist(i < measurements_.size()); return measurements_[i]; }
    auto & get(const std::string &name) const {
        auto it = std::find_if(measurements_.begin(), measurements_.end(),
                               [&](auto &elem) { return elem.name == name; });
        insist(it != measurements_.end(), "a measurement with that name does not exist");
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
            insist(it->end != time_point(), "a measurement with that name is already in progress");
            it->begin = clock::now();
            it->end = time_point();
            return std::distance(measurements_.begin(), it);
        } else { // create new measurement
            Measurement m{ name, clock::now(), time_point() };
            auto id = measurements_.size();
            measurements_.emplace_back(std::move(m));
            return id;
        }
    }

    /** Stops the `Measurement` with the given ID. */
    void stop(std::size_t id) {
        insist (id < measurements_.size(), "id out of bounds");
        auto &ref = measurements_[id];
        insist(ref.end == time_point(), "cannot stop that measurement because it has already been stopped");
        ref.end = clock::now();
    }

    /** Erase a `Measurement` from this `Timer`. */
    void erase(std::size_t id) {
        insist (id < measurements_.size(), "id out of bounds");
        auto &ref = measurements_[id];
        ref.begin = ref.end = time_point();
    }

    public:
    /** Creates a new `TimingProcess` with the given `name`. */
    TimingProcess create_timing(std::string name) { return TimingProcess(*this, /* ID= */ start(name)); }

    /** Print all finished and in-process timings of `timer` to `out`. */
    friend std::ostream & operator<<(std::ostream &out, const Timer &timer) {
        using std::chrono::duration_cast;
        using std::chrono::microseconds;

        for (auto &m : timer) {
            if (m.begin == time_point()) continue; // measurement has been removed
            out << m.name << ": ";
            if (m.end == time_point()) {
                out << "started at " << m.begin << ", not finished\n";
            } else {
                out << duration_cast<microseconds>(m.end - m.begin).count() / 1e3 << " ms\n";
            }
        }

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

#define TIME_EXPR(EXPR, DESCR, TIMER) \
    [&]() { auto TP = (TIMER).create_timing((DESCR)); return (EXPR); }();
#define TIME_BLOCK(DESCR, TIMER, BLOCK) {\
    auto TP = (TIMER).create_timing((DESCR)); \
    BLOCK \
}
#define TIME_THIS(DESCR, TIMER) \
    auto PASTE(__timer_, __COUNTER__) = (TIMER).create_timing((DESCR));
