#include "io/Reader.hpp"

#include "IR/Interpreter.hpp"
#include "lex/Lexer.hpp"
#include "parse/AST.hpp"
#include "storage/Store.hpp"
#include "util/macro.hpp"
#include <cctype>
#include <cerrno>
#include <exception>
#include <iterator>
#include <limits>
#include <map>
#include <string>
#include <variant>


using namespace db;


DSVReader::DSVReader(const Table &table, Diagnostic &diag,
          char delimiter,
          char escape,
          char quote,
          bool has_header,
          bool skip_header)
    : Reader(table, diag)
    , delimiter(delimiter)
    , escape(escape)
    , quote(quote)
    , has_header(has_header)
    , skip_header(skip_header)
{ }

void DSVReader::operator()(std::istream &in, const char *name)
{
    auto &C = Catalog::Get();
    auto &store = table.store();
    std::vector<const Attribute*> columns; ///< maps column offset to attribute

    /* Parsing data. */
    using buf_t = std::vector<char>;
    char c = '\n';
    Position start(name), pos(name);
    buf_t buf;

    /*----- Helper functions. ----------------------------------------------------------------------------------------*/
    auto step = [&]() -> int {
        switch (c) {
            case '\n':
                pos.column = 1;
                pos.line++;
            case EOF:
                break;

            default:
                pos.column++;
        }
        return c = in.get();
    };

    auto push = [&]() {
        buf.push_back(c);
        step();
    };

    auto read_quoted = [&]() -> void {
        insist(c == quote, "quoted strings must begin with the quote character");
        push(); // opening quote
        while (c != quote) {
            if (c == EOF or c == '\n') {
                diag.e(pos) << "Unexpected end of quoted string.\n";
                return; // unexpected end
            }
            push();
        }
        insist(c == quote);
        push(); // closing quote
    };

    auto read_cell = [&]() -> std::string {
        buf.clear();
        if (c == quote) {
            read_quoted();
            if (c != EOF and c != '\n' and c != delimiter) {
                /* superfluous characters */
                diag.e(pos) << "Unexpected characters after a quoted string.\n";
                while (c != EOF and c != '\n' and c != delimiter) step();
            }
        } else {
            while (c != EOF and c != '\n' and c != delimiter) push();
        }
        return std::string(buf.begin(), buf.end());
    };

    step(); // initialize the variable `c` by reading the first character from the input stream

    /*----- Handle header information. -------------------------------------------------------------------------------*/
    if (has_header) {
        while (c != EOF and c != '\n') {
            auto name = read_cell();
            const Attribute *attr = nullptr;
            try {
                attr = &table.at(C.pool(name.c_str()));
            } catch (std::out_of_range) { /* nothing to do */ }
            columns.push_back(attr);
            if (c == delimiter)
                step(); // discard delimiter
        }
        insist(c == EOF or c == '\n');
        step();
    } else {
        for (auto &attr : table)
            columns.push_back(&attr);
        if (skip_header) {
            in.ignore(std::numeric_limits<std::streamsize>::max(), '\n'); // skip entire line
            c = '\n';
            step(); // skip newline
        }
    }

    /*----- Read data row wise. --------------------------------------------------------------------------------------*/
    std::vector<std::string> tuple;
    tuple.reserve(columns.size());
    for (;;) {
        tuple.emplace_back(read_cell());
        if (c == delimiter) {
            step();
        } else {
            insist(c == EOF or c == '\n', "expected the end of a row");
            /* check tuple length */
            if (tuple.size() != table.size()) {
                diag.e(pos) << "Row of incorrect size.\n";
                goto next;
            }

            /* Parse strings into values. */
            {
                std::vector<value_type> values(table.size());
                for (std::size_t i = 0; i != table.size(); ++i) {
                    auto &cell = tuple[i];
                    auto &attr = *columns[i];
                    auto &value = values[i];
                    if (not parse_value(cell, attr, value)) {
                        diag.e(pos) << "Could not parse the row.\n";
                        goto next;
                    }
                }

                /* Append a new row to the store and set its values. */
                auto row = store.append();
                for (std::size_t i = 0; i != table.size(); ++i) {
                    auto &attr = *columns[i];
                    auto &value = values[i];

                    std::visit(overloaded {
                        [&](null_type)     { row->setnull(attr); },
                        [&](bool b)        { row->set(attr, b); },
                        [&](int64_t i)     { row->set(attr, i); },
                        [&](float f)       { row->set(attr, f); },
                        [&](double d)      { row->set(attr, d); },
                        [&](std::string s) { row->set(attr, s); },
                    }, value);
                }
            }
next:
            step();
            if (c == EOF)
                break;
            tuple.clear();
        }
    }
}

bool DSVReader::parse_value(std::string str, const Attribute &attr, value_type &value)
{
    using std::begin, std::end, std::next, std::prev;

    if (str.empty()) { value = null_type(); return true; }

    auto ty = attr.type;

    if (ty->is_boolean()) {
        if (str == "TRUE")  { value = true;  return true; }
        if (str == "FALSE") { value = false; return true; }
        return false;
    }

    if (ty->is_character_sequence()) {
        if (str[0] == quote) {
            std::string unquoted(next(begin(str)), prev(end(str)));
            str = unquoted;
        }
        value = unescape(str);
        return true;
    }

    if (ty->is_floating_point() or ty->is_decimal()) {
        std::size_t pos;
        double d = stod(str, &pos);
        if (pos != str.length())
            return false;
        value = d;
        return true;
    }

    if (ty->is_integral()) {
        char *end;
        int64_t i = strtoll(str.c_str(), &end, 10);
        if (end != &*str.end())
            return false;
        value = i;
        return true;
    }

    unreachable("unsupported type");
}
