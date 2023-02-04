#include <mutable/catalog/DatabaseCommand.hpp>

#include "mutable/catalog/Catalog.hpp"
#include "mutable/Options.hpp"


using namespace m;


void learn_spns::execute_instruction(const std::vector<const char*>&, Diagnostic &diag) const
{
    auto &C = Catalog::Get();
    if (not C.has_database_in_use()) { diag.err() << "No database selected.\n"; return; }

    auto &DB = C.get_database_in_use();
    if (DB.size() == 0) { diag.err() << "There are no tables in the database.\n"; return; }

    auto CE = C.create_cardinality_estimator("Spn", DB.name);
    auto spn_estimator = cast<SpnEstimator>(CE.get());
    spn_estimator->learn_spns();
    DB.cardinality_estimator(std::move(CE));

    if (not Options::Get().quiet) { diag.out() << "learned spn on every table in " << DB.name << ".\n"; }
}

__attribute__((constructor(202)))
static void register_instructions()
{
    Catalog &C = Catalog::Get();
#define REGISTER(NAME, DESCRIPTION) \
    C.register_instruction(#NAME, std::make_unique<NAME>(), DESCRIPTION)
    REGISTER(learn_spns, "learn an SPN on every table in the database that is currently in use");
#undef REGISTER
}
