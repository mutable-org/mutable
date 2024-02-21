#include <mutable/catalog/TableFactory.hpp>

#include <mutable/catalog/Catalog.hpp>


using namespace m;


namespace {


__attribute__((constructor(201)))
void add_table_args()
{
    Catalog &C = Catalog::Get();

    /*----- Command-line arguments -----*/
    // TODO add "multi-versioning" to default when multi-versioning support is complete.
    C.arg_parser().add<std::vector<std::string_view>>(
        /* group=       */ "TableFactory",
        /* short=       */ nullptr,
        /* long=        */ "--table-properties",
        /* description= */ "enable multiple table properties.",
        /* callback=    */ [](std::vector<std::string_view> properties) {
            Catalog &C = Catalog::Get();
            std::unique_ptr<TableFactory> table_factory = std::make_unique<ConcreteTableFactory>();
            for (auto property : properties)
                table_factory = C.apply_table_property(C.pool(property), std::move(table_factory));
            C.table_factory(std::move(table_factory));
        }
    );
}


}


__attribute__((constructor(202)))
void register_table_factories()
{
    Catalog &C = Catalog::Get();

    std::unique_ptr<TableFactory> table_factory = std::make_unique<ConcreteTableFactory>();
    C.table_factory(std::move(table_factory));

    C.register_table_property<ConcreteTableFactoryDecorator<MultiVersioningTable>>(C.pool("multi-versioning"),
                                                                                   "enables multi-versioning");
}
