#include "util/Timer.hpp"


void Timer::dump(std::ostream &out) const { out << *this << std::endl; }
void Timer::dump() const { dump(std::cerr); }
