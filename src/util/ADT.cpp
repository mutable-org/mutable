#include <mutable/util/ADT.hpp>


using namespace m;

M_LCOV_EXCL_START
void SmallBitset::dump(std::ostream &out) const { out << *this << std::endl; }
void SmallBitset::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
