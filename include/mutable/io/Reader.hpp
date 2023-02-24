#pragma once

#include <iostream>
#include <mutable/catalog/Schema.hpp>
#include <mutable/IR/Tuple.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/Diagnostic.hpp>


namespace m {

/** An interface for all readers.  A reader takes input from a stream (file, network, etc.), matches it to the schema of
 * a table, and inserts the data into the table's store. */
struct M_EXPORT Reader
{
    const Table &table; ///< the table to insert the data into
    Diagnostic &diag;

    public:
    Reader(const Table &table, Diagnostic &diag) : table(table), diag(diag) { }
    virtual ~Reader() { }

    virtual void operator()(std::istream &in, const char *name = "-") = 0;
};

/** A reader for delimiter separated value (DSV) files. */
struct M_EXPORT DSVReader : Reader, ConstTypeVisitor
{
    /** Configuration parameters for importing a DSV file.
     *
     * By deault, the `Config` uses the following settings:
     *
     *  | Type      | Symbol             |
     *  |-----------|--------------------|
     *  | delimiter | `,` (comma)        |
     *  | quote     | `"` (double quote) |
     *  | escape    | `\\` (backslash)   |
     *
     *  These settings can be overwritten manually after construction of a `Config` instance.  Alternatively, `Config`
     *  provides factory methods for common file formats, e.g. `CSV()`.
     */
    struct M_EXPORT Config
    {
        ///> the delimiter separating cells
        char delimiter = ',';
        ///> the quotation mark for strings
        char quote = '"';
        ///> the character to escape special characters within strings, e.g. `\n`
        char escape = '\\';
        ///> whether the first line of the file is a headline describing the columns
        bool has_header = false;
        ///> whether to ignore the headline (requires `has_header = true`)
        bool skip_header = false;
        ///> the maximum number of rows to read from the file (may exceed actual number of rows)
        std::size_t num_rows = std::numeric_limits<decltype(num_rows)>::max();

        /** Creates a `Config` for CSV files, with `delimiter`, `escape`, and `quote` set accordingly to RFC 4180 (see
         * https://www.rfc-editor.org/rfc/rfc4180 ). */
        static Config CSV() {
            Config cfg;
            cfg.delimiter = ',';
            cfg.quote = '"';
            cfg.escape = '"'; // in RFC 4180, the escape character *is* the quote character
            return cfg;
        }
    };

    private:
    Config cfg_;

    Position pos;
    char c;
    std::istream *in = nullptr;
    std::vector<char> buf;
    Tuple tup; ///< intermediate tuple to store values of a row
    std::size_t col_idx;

    public:
    DSVReader(const Table &table, Config cfg, Diagnostic &diag);

    void operator()(std::istream &in, const char *name) override;

    const Config & config() const { return cfg_; }
    size_t num_rows() const { return cfg_.num_rows; }
    size_t delimiter() const { return cfg_.delimiter; }
    size_t escape() const { return cfg_.escape; }
    size_t quote() const { return cfg_.quote; }
    size_t has_header() const { return cfg_.has_header; }
    size_t skip_header() const { return cfg_.skip_header; }

    private:
    using ConstTypeVisitor::operator();
    void operator()(Const<ErrorType> &ty) override;
    void operator()(Const<NoneType> &ty) override;
    void operator()(Const<Boolean> &ty) override;
    void operator()(Const<Bitmap> &ty) override;
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
        if (c == config().quote) {
            step();
            while (c != EOF and c != '\n' and c != config().quote) { step(); }
            accept(config().quote);
        } else
            while (c != EOF and c != '\n' and c != config().delimiter) { step(); }
    }
    void discard_row() { while (c != EOF and c != '\n') { step(); } }

    int64_t read_unsigned_int();
};

}
