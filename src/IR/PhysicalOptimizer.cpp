#include <mutable/IR/PhysicalOptimizer.hpp>

#include "backend/WasmDSL.hpp"
#include "backend/WasmMacro.hpp"
#include "backend/WasmUtil.hpp"


using namespace m;
using namespace m::wasm;


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
void PhysicalOptimizerImpl<PhysicalPlanTable>::execute(const Operator &plan) const
{
    /* Emit code for run function which computes the last pipeline and calls other pipeline functions. */
    FUNCTION(run, void(void))
    {
        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function
        get_plan(plan).match().execute(setup_t::Make_Without_Parent(), pipeline_t(), teardown_t::Make_Without_Parent());
    }

    /* Call run function. */
    run();

#ifndef NDEBUG
    plan.reset_ids(); // XXX: ok?
#endif
}

template<typename PhysicalPlanTable>
void PhysicalOptimizerImpl<PhysicalPlanTable>::accept(PhysOptVisitor &v) { v(*this); }
template<typename PhysicalPlanTable>
void PhysicalOptimizerImpl<PhysicalPlanTable>::accept(ConstPhysOptVisitor &v) const { v(*this); }

M_LCOV_EXCL_START
template<typename PhysicalPlanTable>
void PhysicalOptimizerImpl<PhysicalPlanTable>::dot_plan_helper(const entry_type &e, std::ostream &out) const
{
#define q(X) '"' << X << '"' // quote
#define id(X) q(std::hex << &X << std::dec) // convert virtual address to identifier
    out << "    " << id(e) << " [label=<<B>" << html_escape(to_string(e.match()))
        << "</B> (cumulative cost=" << e.cost() << ")>];\n";
    for (const auto &child : e.children()) {
        dot_plan_helper(child.entry, out);
        out << "    " << id(child.entry) << " -> " << id(e) << ";\n";
    }
#undef id
#undef q
}
template<typename PhysicalPlanTable>
void PhysicalOptimizerImpl<PhysicalPlanTable>::dot_plan(const Operator &plan, std::ostream &out) const
{
    out << "digraph plan\n{\n"
        << "    forcelabels=true;\n"
        << "    overlap=false;\n"
        << "    rankdir=BT;\n"
        << "    graph [compound=true];\n"
        << "    graph [fontname = \"DejaVu Sans\"];\n"
        << "    node [fontname = \"DejaVu Sans\"];\n"
        << "    edge [fontname = \"DejaVu Sans\"];\n";
    dot_plan_helper(get_plan(plan), out);
    out << "}\n";
};

template<typename PhysicalPlanTable>
void PhysicalOptimizerImpl<PhysicalPlanTable>::dump_plan(const Operator &plan, std::ostream &out) const
{
    out << get_plan(plan).match() << std::endl;
}
template<typename PhysicalPlanTable>
void PhysicalOptimizerImpl<PhysicalPlanTable>::dump_plan(const Operator &plan) const { dump_plan(plan, std::cerr); }
M_LCOV_EXCL_STOP

// explicit template instantiations
#define INSTANTIATE(CLASS) \
    template struct m::CLASS;
M_PHYS_OPT_LIST(INSTANTIATE)
#undef INSTANTIATE
