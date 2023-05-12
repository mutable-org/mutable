#include <mutable/catalog/DatabaseCommand.hpp>

#include "backend/StackMachine.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/Optimizer.hpp>
#include <mutable/mutable.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/DotTool.hpp>


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

void QueryDatabase::execute(Diagnostic &diag)
{
    Catalog &C = Catalog::Get();

    if (auto stmt = cast<ast::Stmt>(&ast())) {
        if (Options::Get().ast)
            stmt->dump(diag.out());
        if (Options::Get().astdot) {
            DotTool dot(diag);
            stmt->dot(dot.stream());
            dot.show("ast", false, "dot");
        }
    }

    graph_ = M_TIME_EXPR(QueryGraph::Build(ast<ast::SelectStmt>()), "Construct the query graph", C.timer());

    if (Options::Get().graph)
        graph_->dump(std::cout);
    if (Options::Get().graphdot) {
        DotTool dot(diag);
        graph_->dot(dot.stream());
        dot.show("graph", false, "fdp");
    }
    if (Options::Get().graph2sql) {
        graph_->sql(std::cout);
        std::cout.flush();
    }

    Optimizer Opt(C.plan_enumerator(), C.cost_function());
    std::unique_ptr<Producer> producer = M_TIME_EXPR(Opt(*graph_), "Compute the query plan", C.timer());

    if (Options::Get().plan)
        producer->dump(diag.out());
    if (Options::Get().plandot) {
        DotTool dot(diag);
        producer->dot(dot.stream());
        dot.show("plan", false, "dot");
    }

    M_insist(bool(producer), "logical plan must have been computed");
    if (Options::Get().benchmark)
        logical_plan_ = std::make_unique<NoOpOperator>(std::cout);
    else
        logical_plan_ = std::make_unique<PrintOperator>(std::cout);
    logical_plan_->add_child(producer.release());

    if (Options::Get().dryrun)
        return;

    static thread_local std::unique_ptr<Backend> backend;
    if (not backend)
        backend = M_TIME_EXPR(C.create_backend(), "Create backend", C.timer());
    M_TIME_EXPR(backend->execute(*logical_plan_), "Execute query", C.timer());
}

void InsertRecords::execute(Diagnostic&)
{
    Catalog &C = Catalog::Get();
    auto &DB = C.get_database_in_use();

    auto &I = ast<ast::InsertStmt>();
    auto &T = DB.get_table(I.table_name.text);
    auto &store = T.store();
    StoreWriter W(store);
    auto &S = W.schema();
    Tuple tup(S);

    /* Write all tuples to the store. */
    for (auto &t : I.tuples) {
        StackMachine get_tuple(Schema{});
        for (std::size_t i = 0; i != t.size(); ++i) {
            auto &v = t[i];
            switch (v.first) {
                case ast::InsertStmt::I_Null:
                    get_tuple.emit_St_Tup_Null(0, i);
                    break;

                case ast::InsertStmt::I_Default:
                    /* nothing to be done, Tuples are initialized to default values */
                    break;

                case ast::InsertStmt::I_Expr:
                    get_tuple.emit(*v.second);
                    get_tuple.emit_Cast(S[i].type, v.second->type());
                    get_tuple.emit_St_Tup(0, i, S[i].type);
                    break;
            }
        }
        Tuple *args[] = { &tup };
        get_tuple(args);
        W.append(tup);
    }
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

    if (not Options::Get().quiet)
        diag.out() << "Created table " << table->name << ".\n";
}


#define ACCEPT(CLASS) \
    void CLASS::accept(DatabaseCommandVisitor &v) { v(*this); } \
    void CLASS::accept(ConstDatabaseCommandVisitor &v) const { v(*this); }
M_DATABASE_COMMAND_LIST(ACCEPT)
#undef ACCEPT
