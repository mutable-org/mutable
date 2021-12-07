#pragma once

#include "catalog/Schema.hpp"
#include <mutable/IR/Tuple.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <iostream>


namespace m {

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
    private:
    std::size_t num_rows_;
    char delimiter_;
    char escape_;
    char quote_;
    bool has_header_;
    bool skip_header_;
    Position pos;
    char c;
    std::istream *in = nullptr;
    std::vector<char> buf;
    Tuple tup; ///< intermediate tuple to store values of a row
    std::size_t col_idx;

    public:
    DSVReader(const Table &table, Diagnostic &diag,
              char delimiter = ',',
              char escape = '\\',
              char quote = '\"',
              bool has_header = false,
              bool skip_header = false,
              std::size_t num_rows = std::numeric_limits<decltype(num_rows)>::max());

    void operator()(std::istream &in, const char *name) override;

    size_t num_rows() const { return num_rows_; }
    size_t delimiter() const { return delimiter_; }
    size_t escape() const { return escape_; }
    size_t quote() const { return quote_; }
    size_t has_header() const { return has_header_; }
    size_t skip_header() const { return skip_header_; }

    private:
    using ConstTypeVisitor::operator();
    void operator()(Const<ErrorType> &ty) override;
    void operator()(Const<NoneType> &ty) override;
    void operator()(Const<Boolean> &ty) override;
    void operator()(Const<CharacterSequence> &ty) override;
    void operator()(Const<Date> &ty) override;
    void operator()(Const<DateTime> &ty) override;
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
        if (c == quote_) {
            step();
            while (c != EOF and c != '\n' and c != quote_) { step(); }
            accept(quote_);
        } else
            while (c != EOF and c != '\n' and c != delimiter_) { step(); }
    }
    void discard_row() { while (c != EOF and c != '\n') { step(); } }

    int64_t read_int();
};

}
