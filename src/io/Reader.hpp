#pragma once

#include "catalog/Schema.hpp"
#include "IR/Tuple.hpp"
#include "storage/Store.hpp"
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

    virtual void operator()(std::istream &in, const char *name = "-") = 0;
};

/** A reader for delimiter separated value (DSV) files. */
struct DSVReader : Reader, ConstTypeVisitor
{
    std::size_t num_rows;
    char delimiter;
    char escape;
    char quote;
    bool has_header;
    bool skip_header;

    private:
    Position pos;
    char c;
    std::istream *in = nullptr;
    std::vector<char> buf;
    Tuple tup; ///< intermediate tuple to store values of a row
    std::size_t col_idx;

    public:
    DSVReader(const Table &table, Diagnostic &diag,
              std::size_t num_rows = std::numeric_limits<decltype(num_rows)>::max(),
              char delimiter = ',',
              char escape = '\\',
              char quote = '\"',
              bool has_header = false,
              bool skip_header = false);

    void operator()(std::istream &in, const char *name) override;

    private:
    using ConstTypeVisitor::operator();
    void operator()(Const<ErrorType> &ty) override;
    void operator()(Const<Boolean> &ty) override;
    void operator()(Const<CharacterSequence> &ty) override;
    void operator()(Const<Numeric> &ty) override;
    void operator()(Const<FnType> &ty) override;

    int step() {
        switch (c) {
            case '\n':
                pos.column = 1;
                pos.line++;
            case EOF:
                break;

            default:
                pos.column++;
        }
        return c = in->get();
    };

    void push() { buf.push_back(c); step(); }

    bool accept(char chr) { if (c == chr) { step(); return true; } return false; }

    void discard_cell() {
        if (c == quote) {
            step();
            while (c != EOF and c != '\n' and c != quote) { step(); }
            accept(quote);
        } else
            while (c != EOF and c != '\n' and c != delimiter) { step(); }
    }
    void discard_row() { while (c != EOF and c != '\n') { step(); } }

    int64_t read_int();
};

}
