#pragma once

#include "util/fn.hpp"
#include "util/macro.hpp"
#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <vector>


struct Timer;

/** A manager class to record a measurement with a timer. */
struct TimingProcess
{
    Timer &timer;

    TimingProcess(Timer &timer, std::string name);
    ~TimingProcess();
};

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
    auto begin() const { return measurements_.cbegin(); }
    auto end()   const { return measurements_.cend(); }
    auto cbegin() const { return measurements_.cbegin(); }
    auto cend()   const { return measurements_.cend(); }

    auto & measurements() const { return measurements_; }
    auto & get(std::size_t i) const { insist(i < measurements_.size()); return measurements_[i]; }
    auto & get(const std::string &name) const {
        auto it = std::find_if(measurements_.begin(), measurements_.end(),
                               [&](auto elem) { return elem.first == name; });
        if (it == measurements_.end())
            throw std::out_of_range("a measurement with the name '" + name + "' does not exist");
        return it->second;
    }

    void start(std::string name) {
        auto it = std::find_if(measurements_.begin(), measurements_.end(),
                               [&](auto elem) { return elem.first == name; });
        if (it != measurements_.end())
            throw std::invalid_argument("a measurement with the name '" + name + "' already exists");
        Measurement m{ clock::now(), time_point() };
        measurements_.emplace_back(name, m);
    }

    void stop() {
        insist(not measurements_.empty());
        auto &e = measurements_.back();
        if (e.second.end != time_point())
            throw std::logic_error("cannot stop the measurement '" + e.first + "' because it has already been stopped");
        e.second.end = clock::now();
    }

    void stop(const std::string &name) {
        auto &m = get(name);
        if (m.end != time_point())
            throw std::logic_error("cannot stop the measurement '" + name + "' because it has already been stopped");
        const_cast<Measurement&>(m).end = clock::now();
    }

    TimingProcess create_timing(std::string name) { return TimingProcess(*this, name); }

    friend std::ostream & operator<<(std::ostream &out, const Timer &timer) {
        using std::chrono::duration_cast;
        using std::chrono::time_point_cast;
        using std::chrono::microseconds;

        for (auto &m : timer) {
            out << m.first << ": " << m.second.begin << " - " << m.second.end << " ("
                << duration_cast<microseconds>(m.second.end - m.second.begin).count() / 1e3 << " ms)\n";
        }
        return out;
    }

    duration total() const {
        duration d(0);
        for (auto &m : measurements_)
            d += m.second.end - m.second.begin;
        return d;
    }
};
