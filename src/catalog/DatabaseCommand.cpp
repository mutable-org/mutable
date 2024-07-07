#include <mutable/catalog/DatabaseCommand.hpp>

#include "backend/StackMachine.hpp"
#include "mutable/util/macro.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Schema.hpp>
#include <mutable/IR/Optimizer.hpp>
#include <mutable/mutable.hpp>
#include <mutable/Options.hpp>
#include <mutable/storage/Index.hpp>
#include <mutable/util/DotTool.hpp>


using namespace m;


void EmptyCommand::execute(Diagnostic &diag) { /* Nothing to be done. */ }


/*======================================================================================================================
 * Instructions
 *====================================================================================================================*/

void learn_spns::execute(Diagnostic &diag)
{
    auto &C = Catalog::Get();
    if (not C.has_database_in_use()) { diag.err() << "No database selected.\n"; return; }

    auto &DB = C.get_database_in_use();
    if (DB.size() == 0) { diag.err() << "There are no tables in the database.\n"; return; }

    auto CE = C.create_cardinality_estimator(C.pool("Spn"), DB.name);
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
    C.register_instruction<NAME>(C.pool(#NAME), DESCRIPTION)
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

    auto graph_construction = C.timer().create_timing("Construct the query graph");
    graph_ = QueryGraph::Build(ast<ast::SelectStmt>());
    graph_->transaction(this->transaction());
    for (auto &pre_opt : C.pre_optimizations())
        (*pre_opt.second).operator()(*graph_);
    graph_construction.stop();

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

    /* Check if the optimizer settings are conflicting. */
    if(Options::Get().result_db and Options::Get().decompose)
        throw std::logic_error("the flags `--result_db` and `--decompose` cannot be used together");

    /* Set logical optimizer to use. */
    std::unique_ptr<Producer> producer;
    auto logical_plan_computation = C.timer().create_timing("Compute the logical query plan");
    bool result_db_compatible = true;
    if (Options::Get().result_db) {
        Optimizer_ResultDB Opt;
        std::tie(producer, result_db_compatible) = Opt(*graph_);
        for (auto &post_opt : C.logical_post_optimizations())
            producer = (*post_opt.second).operator()(std::move(producer));
    } else {
        Optimizer Opt(C.plan_enumerator(), C.cost_function());
        producer = Opt(*graph_);
        for (auto &post_opt : C.logical_post_optimizations())
            producer = (*post_opt.second).operator()(std::move(producer));
    }
    logical_plan_computation.stop();
    M_insist(bool(producer), "logical plan must have been computed");

    if (Options::Get().plan)
        producer->dump(diag.out());
    if (Options::Get().plandot) {
        DotTool dot(diag);
        producer->dot(dot.stream());
        dot.show("logical_plan", false, "dot");
    }

    if ((Options::Get().result_db and result_db_compatible) or Options::Get().decompose) {
        logical_plan_ = M_notnull(cast<Consumer>(producer));
    } else {
        if (Options::Get().benchmark)
            logical_plan_ = std::make_unique<NoOpOperator>(std::cout);
        else
            logical_plan_ = std::make_unique<PrintOperator>(std::cout);
        logical_plan_->add_child(producer.release());
    }

    static thread_local std::unique_ptr<Backend> backend;
    if (not backend)
        backend = M_TIME_EXPR(C.create_backend(), "Create backend", C.timer());

    auto physical_plan_computation = C.timer().create_timing("Compute the physical query plan");
    PhysicalOptimizerImpl<ConcretePhysicalPlanTable> PhysOpt;
    backend->register_operators(PhysOpt);
    PhysOpt.cover(*logical_plan_);
    physical_plan_ = PhysOpt.extract_plan();
    for (auto &post_opt : C.physical_post_optimizations())
        physical_plan_ = (*post_opt.second).operator()(std::move(physical_plan_));
    physical_plan_computation.stop();

    if (Options::Get().physplan)
        physical_plan_->dump(std::cout);

    if (not Options::Get().dryrun)
        M_TIME_EXPR(backend->execute(*physical_plan_), "Execute query", C.timer());
}

void InsertRecords::execute(Diagnostic&)
{
    Catalog &C = Catalog::Get();
    auto &DB = C.get_database_in_use();

    auto &I = ast<ast::InsertStmt>();
    auto &T = DB.get_table(I.table_name.text.assert_not_none());
    auto &store = T.store();
    StoreWriter W(store);
    auto &S = W.schema();
    Tuple tup(S);

    /* Find timestamp attributes */
    auto ts_begin = std::find_if(T.cbegin_hidden(), T.end_hidden(),
                                 [&](const Attribute & attr) {
                                    return attr.name == C.pool("$ts_begin");
    });
    auto ts_end = std::find_if(T.cbegin_hidden(), T.end_hidden(),
                               [&](const Attribute & attr) {
                                    return attr.name == C.pool("$ts_end");
    });

    /* Write all tuples to the store. */
    for (auto &t : I.tuples) {
        StackMachine get_tuple(Schema{});
        for (std::size_t i = 0; i != t.size(); ++i) {
            auto attr_id = T.convert_id(i); // hidden attributes change the actual id of the attribute
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
                    get_tuple.emit_Cast(S[attr_id].type, v.second->type());
                    get_tuple.emit_St_Tup(0, attr_id, S[attr_id].type);
                    break;
            }
        }
        Tuple *args[] = { &tup };
        get_tuple(args);

        /*----- set timestamps if available. -----*/
        if (ts_begin != T.end_hidden()) {
            tup.set(ts_begin->id, Value(transaction()->start_time()));
            /* Set $ts_end to -1. It is a special value representing infinity. */
            M_insist(ts_end != T.end_hidden());
            tup.set(ts_end->id, Value(-1));
        }

        W.append(tup);
    }
    /* Invalidate all indexes on the table. */
    DB.invalidate_indexes(T.name());
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
        DSVReader R(table_, cfg_, diag, transaction());

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

void DropDatabase::execute(Diagnostic &diag)
{
    try {
        Catalog::Get().drop_database(db_name_);
        if (not Options::Get().quiet)
            diag.out() << "Dropped database " << db_name_ << ".\n";
    } catch (std::invalid_argument) {
        diag.err() << "Database " << db_name_ << " does not exist.\n";
    }
}

void UseDatabase::execute(Diagnostic &diag)
{
    auto &C = Catalog::Get();
    try {
        auto &DB = C.get_database(db_name_);
        C.set_database_in_use(DB);
        if (not Options::Get().quiet)
            diag.out() << "Using database " << db_name_ << ".\n";
    } catch (std::out_of_range) {
        diag.err() << "Database " << db_name_ << " does not exist.\n";
    }
}

void CreateTable::execute(Diagnostic &diag)
{
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    ThreadSafePooledString table_name = table_->name();
    Table *table = nullptr;
    try {
        table = &DB.add(std::move(table_));
    } catch (std::invalid_argument) {
        diag.err() << "Table " << table_name << " already exists in database " << DB.name << ".\n";
    }

    table->layout(C.data_layout());
    table->store(C.create_store(*table));

    if (not Options::Get().quiet)
        diag.out() << "Created table " << table->name() << ".\n";
}

void DropTable::execute(Diagnostic &diag)
{
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();

    for (auto &table_name : table_names_) {
        try {
            DB.drop_table(table_name);
            if (not Options::Get().quiet)
                diag.out() << "Dropped table " << table_name << ".\n";
        } catch (std::invalid_argument) {
            diag.err() << "Table " << table_name << " does not exist in Database " << DB.name << ".\n";
        }
    }
}

void CreateIndex::execute(Diagnostic &diag)
{
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    const auto &table = DB.get_table(table_name_);

    /* Compute bulkloading schema from attribute name. */
    Schema schema;
    for (auto &entry : table.schema()) {
        if (entry.id.name == attribute_name_) {
            schema.add(entry);
            break; // only one-dimensional indexes are supported
        }
    }

    /* Bulkload index. */
    try {
        M_TIME_EXPR(index_->bulkload(table, schema), "Bulkload index", C.timer());
    } catch (invalid_argument) {
        diag.err() << "Could not bulkload index." << '\n';
    }

    /* Add index to database. */
    try {
        DB.add_index(std::move(index_), table_name_, attribute_name_, index_name_);
        if (not Options::Get().quiet)
            diag.out() << "Created index " << index_name_ << ".\n";
    } catch (std::out_of_range) {
        diag.err() << "Table " << table_name_ << " or Attribute " << attribute_name_ << " does not exist in Database "
                   << DB.name << ".\n";
    } catch (invalid_argument) {
        diag.err() << "Index " << index_name_ << " already exists in Database " << DB.name << ".\n";
    }
}

void DropIndex::execute(Diagnostic &diag)
{
    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();

    for (auto &index_name : index_names_) {
        try {
            DB.drop_index(index_name);
            if (not Options::Get().quiet)
                diag.out() << "Dropped index " << index_name << ".\n";
        } catch (invalid_argument) {
            diag.err() << "Index " << index_name << " does not exist in Database " << DB.name << ".\n";
        }
    }
}

#define ACCEPT(CLASS) \
    void CLASS::accept(DatabaseCommandVisitor &v) { v(*this); } \
    void CLASS::accept(ConstDatabaseCommandVisitor &v) const { v(*this); }
M_DATABASE_COMMAND_LIST(ACCEPT)
#undef ACCEPT
