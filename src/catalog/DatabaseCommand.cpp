#include <mutable/catalog/DatabaseCommand.hpp>

#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/Optimizer.hpp>
#include <mutable/Options.hpp>


using namespace m;


/*======================================================================================================================
 * Instructions
 *====================================================================================================================*/

void learn_spns::execute(Diagnostic &diag)
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

void QueryDatabase::execute(Diagnostic&)
{
    Catalog &C = Catalog::Get();
    graph_ = M_TIME_EXPR(QueryGraph::Build(ast<ast::SelectStmt>()), "Construct the query graph", C.timer());
    Optimizer Opt(C.plan_enumerator(), C.cost_function());
    std::unique_ptr<Producer> producer = M_TIME_EXPR(Opt(*graph_), "Compute the query plan", C.timer());
    M_insist(bool(producer), "logical plan must have been computed");
    if (Options::Get().benchmark)
        logical_plan_ = std::make_unique<NoOpOperator>(std::cout);
    else
        logical_plan_ = std::make_unique<PrintOperator>(std::cout);
    logical_plan_->add_child(producer.release());
    C.backend().execute(*logical_plan_);
}

void InsertRecords::execute(Diagnostic&)
{
    M_unreachable("not yet implemented");
}

void UpdateRecords::execute(Diagnostic&)
{
    M_unreachable("not yet implemented");
}

void DeleteRecords::execute(Diagnostic&)
{
    M_unreachable("not yet implemented");
}

void ImportDSV::execute(Diagnostic &diag)
{
    Catalog &C = Catalog::Get();
    try {
        DSVReader R(table_, cfg_, diag);

        errno = 0;
        std::ifstream file(path_);
        if (not file) {
            const auto errsv = errno;
            diag.err() << "Could not open file " << path_;
            if (errsv)
                diag.err() << ": " << strerror(errsv);
            diag.err() << std::endl;
        } else {
            M_TIME_EXPR(R(file, path_.c_str()), "Read DSV file", C.timer());
        }
    } catch (m::invalid_argument e) {
        diag.err() << "Error reading DSV file: " << e.what() << "\n";
    }
}


/*======================================================================================================================
 * Data Definition Language
 *====================================================================================================================*/

void CreateDatabase::execute(Diagnostic &diag)
{
    try {
        Catalog::Get().add_database(db_name_);
        if (not Options::Get().quiet)
            diag.out() << "Created database " << db_name_ << ".\n";
    } catch (std::invalid_argument) {
        diag.err() << "Database " << db_name_ << " already exists.\n";
    }
}

void UseDatabase::execute(Diagnostic &diag)
{
    auto &C = Catalog::Get();
    try {
        auto &DB = C.get_database(db_name_);
        C.set_database_in_use(DB);
    } catch (std::out_of_range) {
        diag.err() << "Database " << db_name_ << " does not exist.\n";
    }
}

void CreateTable::execute(Diagnostic &diag)
{
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    const char *table_name = table_->name;
    Table *table = nullptr;
    try {
        table = &DB.add(table_.release()); // TODO transfer of ownership with std::unique_ptr
    } catch (std::invalid_argument) {
        diag.err() << "Table " << table_name << " already exists in database " << DB.name << ".\n";
    }

    table->layout(C.data_layout());
    table->store(C.create_store(*table));
}


#define ACCEPT(CLASS) \
    void CLASS::accept(DatabaseCommandVisitor &v) { v(*this); } \
    void CLASS::accept(ConstDatabaseCommandVisitor &v) const { v(*this); }
M_DATABASE_COMMAND_LIST(ACCEPT)
#undef ACCEPT
