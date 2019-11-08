#include "io/Reader.hpp"

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
    int c = '\n';
    Position start(name), pos(name);

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

    auto push = [&](std::string &buf) {
        buf += char(c);
        step();
    };

    auto read_quoted = [&](std::string &buf) -> void {
        insist(c == quote, "quoted strings must begin with the quote character");
        push(buf); // opening quote
        while (c != quote) {
            if (c == EOF or c == '\n') {
                diag.e(pos) << "Unexpected end of quoted string.\n";
                return; // unexpected end
            }
            push(buf);
        }
        insist(c == quote);
        push(buf); // closing quote
    };

    auto read_cell = [&]() -> std::string {
        std::string buf;
        if (c == quote) {
            read_quoted(buf);
            if (c != EOF and c != '\n' and c != delimiter) {
                /* superfluous characters */
                diag.e(pos) << "Unexpected characters after a quoted string.\n";
                while (c != EOF and c != '\n' and c != delimiter) step();
            }
        } else {
            while (c != EOF and c != '\n' and c != delimiter) push(buf);
        }
        return buf;
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
    tuple.reserve(table.size());
    for (;;) {
        tuple.emplace_back(read_cell());
        if (c == delimiter) {
            step();
        } else {
            insist(c == EOF or c == '\n', "expected the end of a row");
            /* check tuple length */
            if (tuple.size() != columns.size()) {
                diag.e(pos) << "Row of incorrect size.\n";
                goto next;
            }

            /* Parse strings into values. */
            {
                std::vector<value_type> values;
                values.reserve(table.size());
                for (std::size_t i = 0; i != columns.size(); ++i) {
                    if (not columns[i]) continue;
                    auto &attr = *columns[i];
                    auto &cell = tuple[i];
                    auto &value = values.emplace_back();
                    if (not parse_value(cell, attr, value)) {
                        diag.e(pos) << "Could not parse the row.\n";
                        goto next;
                    }
                }

                /* Append a new row to the store and set its values. */
                auto row = store.append();
                auto value_it = values.begin();
                for (std::size_t i = 0; i != columns.size(); ++i) {
                    if (not columns[i]) continue;
                    auto &attr = *columns[i];
                    auto value = *value_it++;

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

bool DSVReader::parse_value(const std::string &str, const Attribute &attr, value_type &value)
{
    using std::begin, std::end, std::next, std::prev;

    if (str.empty()) { value = null_type(); return true; }

    struct TypeDispatch : ConstTypeVisitor
    {
        const std::string &str;
        value_type &value;
        bool success = false;

        TypeDispatch(const std::string &str, value_type &value)
            : str(str)
            , value(value)
        { }

        using ConstTypeVisitor::operator();
        void operator()(Const<ErrorType>&) { unreachable("error type"); }
        void operator()(Const<Boolean>&) {
            if (str == "TRUE")  { value = true;  success = true; }
            if (str == "FALSE") { value = false; success = true; }
        }
        void operator()(Const<CharacterSequence>&) {
            value = interpret(str);
            success = true;
        }
        void operator()(Const<Numeric> &ty) {
            switch (ty.kind) {
                case Numeric::N_Int: {
                    char *end;
                    int64_t i = strtoll(str.c_str(), &end, 10);
                    if (end != &*str.end())
                        break;
                    value = i;
                    success = true;
                    break;
                }

                case Numeric::N_Decimal: { // TODO more precise way to read decimal
                    std::size_t pos;
                    double d = stod(str, &pos);
                    if (pos != str.length())
                        break;
                    value = d;
                    success = true;
                    break;
                }

                case Numeric::N_Float: {
                    std::size_t pos;
                    double d = stod(str, &pos);
                    if (pos != str.length())
                        break;
                    if (ty.precision == 32)
                        value = float(d);
                    else
                        value = d;
                    success = true;
                    break;
                }
            }
        }
        void operator()(Const<FnType>&) { unreachable("fn type"); }
    };

    TypeDispatch dispatcher(str, value);
    dispatcher(*attr.type);
    return dispatcher.success;
}
