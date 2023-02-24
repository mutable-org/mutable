#include <mutable/io/Reader.hpp>

#include "backend/Interpreter.hpp"
#include "backend/StackMachine.hpp"
#include <cctype>
#include <cerrno>
#include <exception>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutable/storage/DataLayout.hpp>
#include <mutable/storage/Store.hpp>
#include <mutable/util/macro.hpp>
#include <string>


using namespace m;
using namespace m::storage;


DSVReader::DSVReader(const Table &table, Config cfg, Diagnostic &diag)
    : Reader(table, diag)
    , cfg_(cfg)
    , pos(nullptr)
{
    if (config().delimiter == config().quote)
        throw invalid_argument("delimiter and quote must not be the same character");
}

void DSVReader::operator()(std::istream &in, const char *name)
{
    auto &C = Catalog::Get();
    auto &store = table.store();

    /* Compute table schema. */
    Schema S;
    for (auto &attr : table) S.add({table.name, attr.name}, attr.type);

    /* Declare reference to the `StackMachine` for the current `Linearization`. */
    std::unique_ptr<StackMachine> W;
    const DataLayout *layout = nullptr;

    /* Allocate intermediate tuple. */
    tup = Tuple(S);

    std::vector<const Attribute*> columns; ///< maps column offset to attribute
    this->in = &in;
    c = '\n';
    pos = Position(name);
    step(); // initialize the variable `c` by reading the first character from the input stream

    auto read_cell = [&]() -> const char* {
        buf.clear();
        while (c != EOF and c != '\n' and c != config().delimiter) {
            buf.push_back(c);
            step();
        }
        buf.push_back(0);
        return C.pool(&buf[0]);
    };

    /*----- Handle header information. -------------------------------------------------------------------------------*/
    if (config().has_header and not config().skip_header) {
        while (c != EOF and c != '\n') {
            auto name = read_cell();
            const Attribute *attr = nullptr;
            try {
                attr = &table.at(name);
            } catch (std::out_of_range) { /* nothing to do */ }
            columns.push_back(attr);
            if (c == config().delimiter)
                step(); // discard delimiter
        }
        M_insist(c == EOF or c == '\n');
        step();
    } else {
        for (auto &attr : table)
            columns.push_back(&attr);
        if (config().skip_header) {
            in.ignore(std::numeric_limits<std::streamsize>::max(), '\n'); // skip entire line
            c = '\n';
            step(); // skip newline
        }
    }

    /*----- Read data. -----------------------------------------------------------------------------------------------*/
    std::size_t idx = 0;
    while (in.good() and idx < config().num_rows) {
        ++idx;
        store.append();
        for (std::size_t i = 0; i != columns.size(); ++i) {
            auto col = columns[i];
            if (i != 0 and not accept(config().delimiter)) {
                diag.e(pos) << "Expected a delimiter (" << config().delimiter << ").\n";
                discard_row();
                --idx;
                store.drop(); // drop the unfinished row
                goto end_of_row;
            }

            if (col) { // current cell should be read
                if ((i == columns.size() - 1 and c == '\n') or (i < columns.size() - 1 and c == config().delimiter)) { // NULL
                    tup.null(col->id);
                    continue; // keep delimiter (expected at beginning of each loop)
                }
                col_idx = col->id;
                (*this)(*col->type); // dynamic dispatch based on column type
                discard_cell(); // discard remainder of the cell
            } else {
                discard_cell();
            }
        }
        if (c != EOF and c != '\n') {
            diag.e(pos) << "Expected end of row.\n";
            discard_row();
        } else {
            if (layout != &table.layout()) {
                /* The data layout was updated, recompile stack machine. */
                layout = &table.layout();
                W = std::make_unique<StackMachine>(Interpreter::compile_store(S, store.memory().addr(), *layout,
                                                                              S, store.num_rows() - 1));
            }
            Tuple *args[] = { &tup };
            (*W)(args); // write tuple to store
        }
end_of_row:
        M_insist(c == EOF or c == '\n');
        step();
    }

    this->in = nullptr;
}


void DSVReader::operator()(Const<Boolean>&)
{
    buf.clear();
    while (c != EOF and c != '\n' and c != config().delimiter) { push(); }
    buf.push_back(0);
    if (streq("TRUE", &buf[0]))
        tup.set(col_idx, true);
    else if (streq("FALSE", &buf[0]))
        tup.set(col_idx, false);
    else
        diag.e(pos) << "Expected TRUE or FALSE.\n";
}

void DSVReader::operator()(Const<CharacterSequence>&)
{
    /* This implementation is compliant with RFC 4180.  In quoted strings, quotes have to be escaped with an additional
     * quote.  In unquoted strings, delimiter and quotes are prohibited.  In both cases, escape sequences are not
     * supported.  Note that EOF implicitly closes quoted strings.
     * Source: https://tools.ietf.org/html/rfc4180#section-2 */
    buf.clear();
    if (accept(config().quote)) {
        if (config().escape == config().quote) { // RFC 4180
            while (c != EOF) {
                if (c != config().quote) {
                    push();
                } else {
                    step();
                    if (c == config().quote)
                        push();
                    else
                        break;
                }
            }
        } else {
            while (c != EOF) {
                if (c == config().quote) {
                    step();
                    break;
                } else if (c == config().escape) {
                    step();
                    push();
                } else {
                    push();
                }
            }
        }
    } else {
        while (c != EOF and c != '\n' and c != config().delimiter) {
            if (c == config().quote) {
                diag.e(pos) << "WARNING: Illegal character " << config().quote << " found in unquoted string.\n";
                /* Entire cell is discarded. */
                tup.null(col_idx);
                while (c != EOF and c != '\n' and c != config().delimiter) step();
                return;
            } else
                push();
        }
    }
    buf.push_back(0);

    Catalog &C = Catalog::Get();
    tup.set(col_idx, C.pool(&buf[0]));
}

void DSVReader::operator()(Const<Date>&)
{
    buf.clear();
    buf.push_back('d');
    buf.push_back('\'');

    const bool has_quote = accept(config().quote);
#define DIGITS(num) for (auto i = 0; i < num; ++i) if (is_dec(c)) push(); else goto invalid;
    if ('-' == c) push();
    DIGITS(4);
    if ('-' == c) push(); else goto invalid;
    DIGITS(2);
    if ('-' == c) push(); else goto invalid;
    DIGITS(2);
#undef DIGITS
    if (has_quote and not accept(config().quote))
        goto invalid;

    buf.push_back('\'');
    buf.push_back(0);

    tup.set(col_idx, Interpreter::eval(ast::Constant(ast::Token(pos, &buf[0], TK_DATE))));
    return;

invalid:
    diag.e(pos) << "WARNING: Invalid date.\n";
    tup.null(col_idx);
}

void DSVReader::operator()(Const<DateTime>&)
{
    buf.clear();
    buf.push_back('d');
    buf.push_back('\'');

    const bool has_quote = accept(config().quote);
#define DIGITS(num) for (auto i = 0; i < num; ++i) if (is_dec(c)) push(); else goto invalid;
    if ('-' == c) push();
    DIGITS(4);
    if ('-' == c) push(); else goto invalid;
    DIGITS(2);
    if ('-' == c) push(); else goto invalid;
    DIGITS(2);
    if (' ' == c) push(); else goto invalid;
    DIGITS(2);
    if (':' == c) push(); else goto invalid;
    DIGITS(2);
    if (':' == c) push(); else goto invalid;
    DIGITS(2);
#undef DIGITS
    if (has_quote and not accept(config().quote))
        goto invalid;

    buf.push_back('\'');
    buf.push_back(0);
    tup.set(col_idx, Interpreter::eval(ast::Constant(ast::Token(pos, &buf[0], TK_DATE_TIME))));
    return;

invalid:
    diag.e(pos) << "WARNING: Invalid datetime.\n";
    tup.null(col_idx);
}

void DSVReader::operator()(Const<Numeric> &ty)
{
    switch (ty.kind) {
        case Numeric::N_Int: {
            bool is_neg = false;
            if (accept('-'))
                is_neg = true;
            else
                accept('+');
            int64_t i = read_unsigned_int();
            if (c != EOF and c != '\n' and c != config().delimiter) {
                diag.e(pos) << "WARNING: Unexpected characters encountered in an integer.\n";
                tup.null(col_idx);
                while (c != EOF and c != '\n' and c != config().delimiter) step();
                return;
            }
            if (is_neg) i = -i;
            tup.set(col_idx, i);
            break;
        }

        case Numeric::N_Decimal: {
            auto scale = ty.scale;
            /* Read pre dot digits. */
            bool is_neg = false;
            if (accept('-'))
                is_neg = true;
            else
                accept('+');
            int64_t d = read_unsigned_int();
            // std::cerr << "Read: " << d;
            d = d * powi(10, scale);
            if (accept('.')) {
                /* Read post dot digits. */
                int64_t post_dot = 0;
                auto n = scale;
                while (n > 0 and is_dec(c)) {
                    post_dot = 10 * post_dot + c - '0';
                    step();
                    n--;
                }
                post_dot *= powi(10, n);
                /* Discard further digits */
                while (is_dec(c)) { step(); }
                d += d >= 0 ? post_dot : -post_dot;
            }
            if (c != EOF and c != '\n' and c != config().delimiter) {
                diag.e(pos) << "WARNING: Unexpected characters encountered in a decimal.\n";
                tup.null(col_idx);
                while (c != EOF and c != '\n' and c != config().delimiter) step();
                return;
            }
            if (is_neg) d = -d;
            tup.set(col_idx, d);
            break;
        }

        case Numeric::N_Float: {
            std::string float_str;
            while(c != EOF and c != '\n' and c != config().delimiter) {
                float_str += c;
                step();
            }
            char* end;
            errno = 0;
            double d = std::strtod(float_str.c_str(), &end);
            if (*end != '\0') {
                diag.e(pos) << "WARNING: Unexpected characters encountered in a floating-point number.\n";
                tup.null(col_idx);
                return;
            }
            if ( errno == ERANGE and ( d == HUGE_VAL or d == HUGE_VALF or d == HUGE_VALL ) ) {
                diag.w(pos) << "WARNING: A floating-point number is larger than the maximum value.\n";
                d = std::numeric_limits<double>::max();
            } else if ( errno == ERANGE and (d == -HUGE_VAL or d == -HUGE_VALF or d == -HUGE_VALL ) ) {
                diag.w(pos) << "WARNING: A floating-point number is smaller than the minimum value.\n";
                d = std::numeric_limits<double>::min();
            }
            if (ty.is_float())
                tup.set(col_idx, float(d));
            else
                tup.set(col_idx, d);
            break;
        }
    }
}

void DSVReader::operator()(Const<ErrorType>&) { M_unreachable("invalid type"); }
void DSVReader::operator()(Const<NoneType>&) { M_unreachable("invalid type"); }
void DSVReader::operator()(Const<Bitmap>&) { M_unreachable("invalid type"); }
void DSVReader::operator()(Const<FnType>&) { M_unreachable("invalid type"); }

int64_t DSVReader::read_unsigned_int()
{
    int64_t i = 0;
    while (is_dec(c)) {
        i = 10 * i + c - '0';
        step();
    }
    return i;
}
