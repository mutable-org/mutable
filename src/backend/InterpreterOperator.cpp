#include "backend/InterpreterOperator.hpp"


using namespace m;


void m::register_interpreter_operators(PhysicalOptimizer &phys_opt)
{
#define REGISTER(CLASS) phys_opt.register_operator<interpreter::CLASS>();
M_OPERATOR_LIST(REGISTER)
#undef REGISTER
}
