#include <mutable/util/Timer.hpp>


using namespace m;


M_LCOV_EXCL_START
std::ostream & m::operator<<(std::ostream &out, const Timer::Measurement &M)
{
    out << M.name;
    if (M.is_unused()) {
        out << " (removed)";
    } else if (M.is_active()) {
        out << " started at " << put_timepoint(M.begin) << ", not finished";
    } else {
        M_insist(M.is_finished());
        using namespace std::chrono;
        out << " took " << duration_cast<microseconds>(M.duration()).count() / 1e3 << " ms";
    }
    return out;
}

void Timer::Measurement::dump(std::ostream &out) const { out << *this << std::endl; }
void Timer::Measurement::dump() const { dump(std::cerr); }

void Timer::dump(std::ostream &out) const { out << *this; out.flush(); }
void Timer::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
