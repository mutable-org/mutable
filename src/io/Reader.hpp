#pragma once

#include "catalog/Schema.hpp"
#include "util/Diagnostic.hpp"
#include <iostream>


namespace db {

/** An interface for all readers.  A reader takes input from a stream (file, network, etc.), matches it to the schema of
 * a table, and inserts the data into the table's store. */
struct Reader
{
    const Table &table; ///< the table to insert the data into
    Diagnostic &diag;

    public:
    Reader(const Table &table, Diagnostic &diag) : table(table), diag(diag) { }
    virtual ~Reader() { }

    virtual void operator()(std::istream &in, const char *name = "-") const = 0;
};

/** A reader for delimiter separated value (DSV) files. */
struct DSVReader : Reader
{
    const bool has_header;
    const bool skip_header;
    const char delimiter;

    DSVReader(const Table &table, Diagnostic &diag,
              bool has_header = false,
              bool skip_header = false,
              char delimiter = ',');

    void operator()(std::istream &in, const char *name) const override;
};

}
