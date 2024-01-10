#include <mutable/IR/PhysicalOptimizer.hpp>


using namespace m;


/*======================================================================================================================
 * MatchBase
 *====================================================================================================================*/

M_LCOV_EXCL_START
void MatchBase::dump(std::ostream &out) const { out << *this << std::endl; }
void MatchBase::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * PhysicalOptimizerImpl
 *====================================================================================================================*/

template<typename PhysicalPlanTable>
void PhysicalOptimizerImpl<PhysicalPlanTable>::accept(PhysOptVisitor &v) { v(*this); }
template<typename PhysicalPlanTable>
void PhysicalOptimizerImpl<PhysicalPlanTable>::accept(ConstPhysOptVisitor &v) const { v(*this); }

// explicit template instantiations
#define INSTANTIATE(CLASS) \
    template struct m::CLASS;
M_PHYS_OPT_LIST(INSTANTIATE)
#undef INSTANTIATE
