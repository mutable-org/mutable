#include <mutable/catalog/DatabaseCommand.hpp>

#include <mutable/catalog/Catalog.hpp>
#include <mutable/Options.hpp>


using namespace m;


/*======================================================================================================================
 * Instructions
 *====================================================================================================================*/

void learn_spns::execute(Diagnostic &diag) const
{
    auto &C = Catalog::Get();
    if (not C.has_database_in_use()) { diag.err() << "No database selected.\n"; return; }

    auto &DB = C.get_database_in_use();
    if (DB.size() == 0) { diag.err() << "There are no tables in the database.\n"; return; }

    auto CE = C.create_cardinality_estimator("Spn", DB.name);
    auto spn_estimator = cast<SpnEstimator>(CE.get());
    spn_estimator->learn_spns();
    DB.cardinality_estimator(std::move(CE));

    if (not Options::Get().quiet) { diag.out() << "Learned SPN on every table in " << DB.name << ".\n"; }
}

__attribute__((constructor(201)))
static void register_instructions()
{
    Catalog &C = Catalog::Get();
#define REGISTER(NAME, DESCRIPTION) \
    C.register_instruction<NAME>(#NAME, DESCRIPTION)
    REGISTER(learn_spns, "create an SPN for every table in the database");
#undef REGISTER
}

/*======================================================================================================================
 * Data Manipulation Language (DML)
 *====================================================================================================================*/

void QueryDatabase::execute(Diagnostic&) const
{
    M_unreachable("not yet implemented");
}

void InsertRecords::execute(Diagnostic&) const
{
    M_unreachable("not yet implemented");
}

void UpdateRecords::execute(Diagnostic&) const
{
    M_unreachable("not yet implemented");
}

void DeleteRecords::execute(Diagnostic&) const
{
    M_unreachable("not yet implemented");
}

void ImportDSV::execute(Diagnostic&) const
{
    M_unreachable("not yet implemented");
}


/*======================================================================================================================
 * Data Definition Language
 *====================================================================================================================*/

void CreateDatabaseCommand::execute(Diagnostic&) const
{
    M_unreachable("not yet implemented");
}

void UseDatabaseCommand::execute(Diagnostic&) const
{
    M_unreachable("not yet implemented");
}

void CreateTableCommand::execute(Diagnostic&) const
{
    M_unreachable("not yet implemented");
}


#define ACCEPT(CLASS) \
    void CLASS::accept(DatabaseCommandVisitor &v) { v(*this); } \
    void CLASS::accept(ConstDatabaseCommandVisitor &v) const { v(*this); }
M_DATABASE_COMMAND_LIST(ACCEPT)
#undef ACCEPT
