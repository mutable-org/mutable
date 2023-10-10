#pragma once

#include <mutable/catalog/Schema.hpp>


namespace m {


/** The table factory creates `Table`s with all enabled decorators. */
struct TableFactory
{
    virtual ~TableFactory() = default;

    /** Returns a `Table` with the given \p name. */
    virtual std::unique_ptr<Table> make(const char *name) const = 0;
};


/** Basic implementation of `TableFactory`. */
struct ConcreteTableFactory : TableFactory
{
    /** Returns a `Table` with the given \p name. */
    std::unique_ptr<Table> make(const char *name) const override { return std::make_unique<ConcreteTable>(name); }
};


/** Abstract Decorator class that concrete `TableFactoryDecorator` inherit from. */
struct TableFactoryDecorator : TableFactory
{
    protected:
    std::unique_ptr<TableFactory> table_factory_;

    public:
    TableFactoryDecorator(std::unique_ptr<TableFactory> table_factory) : table_factory_(std::move(table_factory)) { }

    /** Returns a `Table` with the given \p name.
     * Recursively calls all internal `TableFactory`s to construct and decorate the table. */
    std::unique_ptr<Table> make(const char *name) const override {
        auto table = table_factory_->make(name);
        return decorate(std::move(table));
    }

    protected:
    virtual std::unique_ptr<Table> decorate(std::unique_ptr<Table> table) const = 0;
};


template<typename T>
requires std::derived_from<T, Table>
struct ConcreteTableFactoryDecorator : TableFactoryDecorator
{
    public:
    ConcreteTableFactoryDecorator(std::unique_ptr<TableFactory> table_factory) : TableFactoryDecorator(std::move(table_factory)) { }

    protected:
    std::unique_ptr<Table> decorate(std::unique_ptr<Table> table) const override {
        return std::make_unique<T>(std::move(table));
    }
};


}
