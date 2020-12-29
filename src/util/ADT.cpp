#include "util/ADT.hpp"


using namespace m;


void SmallBitset::dump(std::ostream &out) const { out << *this << std::endl; }
void SmallBitset::dump() const { dump(std::cerr); }
