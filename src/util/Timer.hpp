#pragma once

#include "util/fn.hpp"
#include "util/macro.hpp"
#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <vector>


/** Collect times of events. */
struct Timer
{
    using clock = std::chrono::high_resolution_clock;
    using duration = clock::duration;
    using time_point = clock::time_point;

    struct Measurement { time_point begin, end; };
    using entry_type = std::pair<std::string, Measurement>;

    private:
    std::vector<entry_type> measurements_;

    public:
    /** A manager class to record a measurement with a timer. */
    struct TimingProcess
    {
        friend struct Timer;

        private:
        Timer &timer_;
        std::size_t index_;

        TimingProcess(Timer &timer, std::size_t index) : timer_(timer), index_(index) { }

        public:
        ~TimingProcess() { timer_.stop(index_); }
    };

    auto begin() const { return measurements_.cbegin(); }
    auto end()   const { return measurements_.cend(); }
    auto cbegin() const { return measurements_.cbegin(); }
    auto cend()   const { return measurements_.cend(); }

    auto & measurements() const { return measurements_; }
    auto & get(std::size_t i) const { insist(i < measurements_.size()); return measurements_[i]; }
    auto & get(const std::string &name) const {
        auto it = std::find_if(measurements_.begin(), measurements_.end(),
                               [&](auto &elem) { return elem.first == name; });
        insist(it != measurements_.end(), "a measurement with that name does not exist");
        return it->second;
    }

    std::size_t start(std::string name) {
        auto it = std::find_if(measurements_.begin(), measurements_.end(),
                               [&](auto &elem) { return elem.first == name; });
        insist(it == measurements_.end(), "a measurement with that name already exists");
        Measurement m{ clock::now(), time_point() };
        auto index = measurements_.size();
        measurements_.emplace_back(name, m);
        return index;
    }

    void stop(std::size_t index) {
        using std::to_string;
        insist (index < measurements_.size(), "index out of bounds");
        auto &ref = measurements_[index];
        insist(ref.second.end == time_point(), "cannot stop that measurement because it has already been stopped");
        ref.second.end = clock::now();
    }

    void stop(const std::string &name) {
        auto it = std::find_if(measurements_.begin(), measurements_.end(),
                               [&](auto &elem) { return elem.first == name; });
        insist(it != measurements_.end(), "a measurement with that name does not exist");
        insist(it->second.end == time_point(), "cannot stop that measurement because it has already been stopped");
        it->second.end = clock::now();
    }

    void stop() {
        auto &M = measurements_.back();
        insist(M.second.end == time_point(), "cannot stop that measurement because it has already been stopped");
        M.second.end = clock::now();
    }

    TimingProcess create_timing(std::string name) { return TimingProcess(*this, start(name)); }

    friend std::ostream & operator<<(std::ostream &out, const Timer &timer) {
        using std::chrono::duration_cast;
        using std::chrono::microseconds;

        for (auto &m : timer)
            out << m.first << ": "
                << duration_cast<microseconds>(m.second.end - m.second.begin).count() / 1e3 << " ms\n";

        return out;
    }

    duration total() const {
        duration d(0);
        for (auto &m : measurements_)
            d += m.second.end - m.second.begin;
        return d;
    }
};
