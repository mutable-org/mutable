#include "backend/PhysicalOperator.hpp"

#include "backend/WasmDSL.hpp"
#include "backend/WasmMacro.hpp"
#include "backend/WasmUtil.hpp"


using namespace m;
using namespace m::wasm;


void PhysicalOptimizer::execute(const Operator &plan) const
{
    /* Emit code for run function which computes the last pipeline and calls other pipeline functions. */
    FUNCTION(run, void(void))
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
        get_plan(plan).match->execute(setup_t::Make_Without_Parent(), pipeline_t(), teardown_t::Make_Without_Parent());
    }

    /* Call run function. */
    run();

#ifndef NDEBUG
    plan.reset_ids(); // XXX: ok?
#endif
}

void PhysicalOptimizer::dump_plan(const Operator &plan, std::ostream &out) const
{
    out << *get_plan(plan).match << std::endl;
}

void PhysicalOptimizer::dump_plan(const Operator &plan) const { dump_plan(plan, std::cerr); }
