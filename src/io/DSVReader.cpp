#include "io/Reader.hpp"

#include "IR/Interpreter.hpp"
#include "lex/Lexer.hpp"
#include "parse/AST.hpp"
#include "storage/Store.hpp"
#include "util/macro.hpp"
#include <cctype>
#include <cerrno>
#include <exception>
#include <map>
#include <variant>


using namespace db;


DSVReader::DSVReader(const Table &table, Diagnostic &diag,
          bool has_header,
          bool skip_header,
          char delimiter)
    : Reader(table, diag)
    , has_header(has_header)
    , skip_header(skip_header)
    , delimiter(delimiter)
{
    if (std::isspace(delimiter)) {
        diag.err() << "The delimiter must not be a white space character.\n";
        throw std::invalid_argument("invalid configuration of DSVReader");
    }
}

void DSVReader::operator()(std::istream &in, const char *name) const
{
    auto &C = Catalog::Get();
    auto &store = table.store();
    Lexer lexer(diag, C.get_pool(), name, in);

    std::vector<const Attribute*> columns; ///< maps column offset to attribute

    Token tok;
    auto consume = [&]() { auto old = tok; tok = lexer.next(); return old; };
    auto is = [&](char c) { return tok.text[0] == c; };
    auto accept = [&](char c) { if (is(c)) { consume(); return true; } return false; };

    consume();

    if (has_header) {
        for (std::size_t i = 0; i != table.size(); ++i) {
            if (tok.type == TK_IDENTIFIER) {
                auto name = consume();
                try {
                    auto &attr = table.at(name.text);
                    columns.push_back(&attr);
                } catch (std::out_of_range) {
                    diag.e(name.pos) << "Column " << name.text << " does not exist.\n";
                }
            } else {
                diag.e(tok.pos) << "Expected a column name.\n";
                /* read until we find a familiar token */
                while (tok and not is(delimiter)) consume();
            }

            if (not accept(delimiter) and i != table.size() - 1)
                diag.e(tok.pos) << "Expected a delimiter (" << ::escape(delimiter) << ") but got " << tok.text << ".\n";
        }
    } else {
        /* TODO create the mapping */
    }
    insist(columns.size() == table.size());

#ifndef NDEBUG
    std::cerr << "The DSV reader collected the following header information:\n";
    for (std::size_t i = 0; i != columns.size(); ++i) {
        auto col = columns[i];
        std::cerr << '[' << i << "]: ";
        if (col)
            std::cerr << col->name;
        else
            std::cerr << "<unused>";
        std::cerr << '\n';
    }
#endif

    if (diag.num_errors()) return; // if the header is corrupted, don't read any data

    std::vector<Token> tuple;
    tuple.reserve(table.size());
    while (tok) {
        tuple.clear();

        /* Read a whole tuple. */
        for (std::size_t i = 0; i != table.size(); ++i) {
            if (accept(delimiter)) {
                tuple.push_back(Token());
                continue;
            }

            auto t = consume();
            switch (t.type) {
                case TK_PLUS:
                case TK_MINUS: {
                    /* Combine the unary operator and the literal into one token. */
                    std::string s(t.text);
                    auto t2 = consume();
                    s += t2.text;
                    t.text = C.pool(s.c_str());
                    t.type = t2.type;
                }

                default:;
            }
            tuple.push_back(t);

            if (not accept(delimiter) and i != table.size() - 1) {
                diag.e(tok.pos) << "Expected a delimiter (" << ::escape(delimiter) << ") but got " << tok.text << ".\n";
                goto incomplete_row;
            }
        }

        /* Parse the tuple to the data type of its corresponding attriubute. */
        auto row = store.append();
        insist(tuple.size() == table.size());
        for (std::size_t i = 0; i != table.size(); ++i) {
            auto &attr = *columns[i];
            auto t = tuple[i];

            if (not t or t.type == TK_Null) {
                row->setnull(attr);
            } else {
                auto value = ExpressionEvaluator::eval(Constant(t));

                std::visit(overloaded {
                    [&](db::null_type) { row->setnull(attr); },
                    [&](bool b) { row->set(attr, b); },
                    [&](int64_t i) { row->set(attr, i); },
                    [&](float f) { row->set(attr, f); },
                    [&](double d) { row->set(attr, d); },
                    [&](const std::string &s) { row->set(attr, s); },
                }, value);
            }
        }
    }

    return;

incomplete_row:
    diag.e(tok.pos) << "Incomplete row, aborting.\n";
}
