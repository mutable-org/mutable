#include "io/Reader.hpp"

#include "IR/Interpreter.hpp"
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
    , pos(nullptr)
{ }

void DSVReader::operator()(std::istream &in, const char *name)
{
    auto &C = Catalog::Get();
    auto &store = table.store();
    std::vector<const Attribute*> columns; ///< maps column offset to attribute
    this->in = &in;
    c = '\n';
    pos = Position(name);
    step(); // initialize the variable `c` by reading the first character from the input stream

    auto read_cell = [&]() -> const char* {
        buf.clear();
        while (c != EOF and c != '\n' and c != delimiter) {
            buf.push_back(c);
            step();
        }
        buf.push_back(0);
        return C.pool(&buf[0]);
    };

    /*----- Handle header information. -------------------------------------------------------------------------------*/
    if (has_header) {
        while (c != EOF and c != '\n') {
            auto name = read_cell();
            const Attribute *attr = nullptr;
            try {
                attr = &table.at(name);
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

    /*----- Read data. -----------------------------------------------------------------------------------------------*/
    while (in.good()) {
        row = store.append();
        for (std::size_t i = 0; i != columns.size(); ++i) {
            auto col = columns[i];
            if (i != 0 and not accept(delimiter)) {
                diag.e(pos) << "Expected a delimiter (" << delimiter << ").\n";
                discard_row();
                goto next_row;
            }

            if (col) {
                if (c == delimiter) {
                    row->setnull(*col);
                    step();
                    continue;
                }
                attr = col;
                auto ty = col->type;
                (*this)(*ty);
            } else {
                discard_cell();
            }
        }
next_row:;
        if (c != EOF and c != '\n') {
            diag.e(pos) << "Expected end of row.\n";
            discard_row();
        }
        insist(c == EOF or c == '\n');
        step();
    }

    this->in = nullptr;
}


void DSVReader::operator()(Const<Boolean>&)
{
    buf.clear();
    while (c != EOF and c != '\n' and c != delimiter) { push(); }
    buf.push_back(0);
    if (streq("TRUE", &buf[0]))
        row->set(*attr, true);
    else if (streq("FALSE", &buf[0]))
        row->set(*attr, false);
    else
        diag.e(pos) << "Expected TRUE or FALSE.\n";
}

void DSVReader::operator()(Const<CharacterSequence>&)
{
    buf.clear();
    if (c == quote) {
        step();
        while (c != EOF and c != '\n' and c != quote) {
            if (c == escape) {
                push();
                if (c == quote)
                    push();
            } else {
                push();
            }
        }
        accept(quote);
    } else {
        while (c != EOF and c != '\n' and c != delimiter) push();
    }
    buf.push_back(0);
    row->set(*attr, interpret(std::string(buf.begin(), buf.end()), escape, quote));
}

void DSVReader::operator()(Const<Numeric> &ty)
{
    switch (ty.kind) {
        case Numeric::N_Int: {
            int64_t i = read_int();
            row->set(*attr, i);
            break;
        }

        case Numeric::N_Decimal: {
            // TODO more precise implementation
            /* fall through */
        }

        case Numeric::N_Float: {
            double d;
            in->unget();
            *in >> d;
            step();
            row->set(*attr, d);
            break;
        }
    }
}

void DSVReader::operator()(Const<ErrorType>&) { unreachable("invalid type"); }
void DSVReader::operator()(Const<FnType>&) { unreachable("invalid type"); }

int64_t DSVReader::read_int()
{
    int64_t i = 0;
    bool is_neg = false;
    if (accept('-'))
        is_neg = true;
    else
        accept('+');
    while (is_dec(c)) {
        i = 10 * i + c - '0';
        step();
    }
    if (is_neg) i = -i;
    return i;
}
