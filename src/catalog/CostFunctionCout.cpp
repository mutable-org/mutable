#include <mutable/catalog/CostFunctionCout.hpp>

#include <mutable/catalog/Catalog.hpp>


using namespace m;


__attribute__((constructor(202)))
static void register_cost_function()
{
    Catalog &C = Catalog::Get();
    C.register_cost_function(
        "CostFunctionCout",
        std::make_unique<CostFunctionCout>(),
        "implementation of cost function C_out from Sophie Cluet and Guido Moerkotte"
    );
}