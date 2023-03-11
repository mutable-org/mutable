#include <mutable/catalog/Catalog.hpp>

#include "backend/Interpreter.hpp"
#include "storage/ColumnStore.hpp"
#include "storage/PaxStore.hpp"
#include "storage/RowStore.hpp"
#include <mutable/catalog/CostFunctionCout.hpp>


using namespace m;


/*======================================================================================================================
 * Catalog
 *====================================================================================================================*/

Catalog * Catalog::the_catalog_(nullptr);

Catalog::Catalog()
    : allocator_(new memory::LinearAllocator())
{
    /*----- Initialize standard functions. ---------------------------------------------------------------------------*/
#define M_FUNCTION(NAME, KIND) { \
    auto name = pool(#NAME); \
    auto res = standard_functions_.emplace(name, new Function(name, Function::FN_ ## NAME, Function::KIND)); \
    M_insist(res.second, "function already defined"); \
}
#include <mutable/tables/Functions.tbl>
#undef M_FUNCTION
}


Catalog::~Catalog()
{
    for (auto db : databases_)
        delete db.second;
    for (auto fn : standard_functions_)
        delete fn.second;
}

__attribute__((constructor(200)))
Catalog & Catalog::Get()
{
    if (not the_catalog_)
        the_catalog_ = new Catalog();
    return *the_catalog_;
}

__attribute__((destructor(200)))
void Catalog::Destroy()
{
    delete Catalog::the_catalog_;
    Catalog::the_catalog_ = nullptr;
}

/*===== Databases ====================================================================================================*/

Database & Catalog::add_database(const char *name)
{
    auto it = databases_.find(name);
    if (it != databases_.end()) throw std::invalid_argument("database with that name already exist");
    it = databases_.emplace_hint(it, name, new Database(name));
    return *it->second;
}

void Catalog::drop_database(const char *name)
{
    if (has_database_in_use() and get_database_in_use().name == name)
        throw std::invalid_argument("Cannot drop database; currently in use.");
    auto it = databases_.find(name);
    if (it == databases_.end())
        throw std::invalid_argument("Database of that name does not exist.");
    delete it->second;
    databases_.erase(it);
}

__attribute__((constructor(201)))
static void add_catalog_args()
{
    Catalog &C = Catalog::Get();

    /*----- Command-line arguments -----------------------------------------------------------------------------------*/
    C.arg_parser().add<const char*>(
        /* group=       */ "Catalog",
        /* short=       */ nullptr,
        /* long=        */ "--data-layout",
        /* description= */ "data layout to use",
        [&C] (const char *str) {
            try {
                C.default_data_layout(str);
            } catch (std::invalid_argument) {
                std::cerr << "There is no data layout with the name \"" << str << "\".\n";
                std::exit(EXIT_FAILURE);
            }
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Catalog",
        /* short=       */ nullptr,
        /* long=        */ "--cardinality-estimator",
        /* description= */ "cardinality estimator to use",
        [&C] (const char *str) {
            try {
                C.default_cardinality_estimator(str);
            } catch (std::invalid_argument) {
                std::cerr << "There is no cardinality estimator with the name \"" << str << "\".\n";
                std::exit(EXIT_FAILURE);
            }
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Catalog",
        /* short=       */ nullptr,
        /* long=        */ "--plan-enumerator",
        /* description= */ "plan enumerator to use",
        [&C] (const char *str) {
            try {
                C.default_plan_enumerator(str);
            } catch (std::invalid_argument) {
                std::cerr << "There is no plan enumerator with the name \"" << str << "\".\n";
                std::exit(EXIT_FAILURE);
            }
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Catalog",
        /* short=       */ nullptr,
        /* long=        */ "--backend",
        /* description= */ "execution backend to use",
        [&C] (const char *str) {
            try {
                C.default_backend(str);
            } catch (std::invalid_argument) {
                std::cerr << "There is no execution backend with the name \"" << str << "\".\n";
                std::exit(EXIT_FAILURE);
            }
        }
    );
}
