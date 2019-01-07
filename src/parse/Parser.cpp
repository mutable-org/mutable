#include "parse/Parser.hpp"

#include "catalog/Schema.hpp"
#include <cerrno>
#include <cstdlib>
#include <utility>


using namespace db;


/** Returns the precedence of an operator.  A higher value means the operator has higher precedence. */
int get_precedence(const TokenType tt)
{
    int p = 0;
    /* List all binary operators.  The higher up an operator is in the switch statement, the higher its precedence. */
    switch (tt) {
        default:                    return -1;
        /* bitwise NOT */
        case TK_TILDE:              ++p;
        /* multiplicative */
        case TK_ASTERISK:
        case TK_SLASH:
        case TK_PERCENT:            ++p;
        /* additive */
        case TK_PLUS:
        case TK_MINUS:              ++p;
        /* comparison */
        case TK_LESS:
        case TK_GREATER:
        case TK_LESS_EQUAL:
        case TK_GREATER_EQUAL:
        case TK_EQUAL:
        case TK_BANG_EQUAL:         ++p;
        /* logical NOT */
        case TK_Not:                ++p;
        /* logical AND */
        case TK_And:                ++p;
        /* logical OR */
        case TK_Or:                 ++p;
    }
    return p;
}

Stmt * Parser::parse()
{
    Stmt *stmt = nullptr;
    switch (token().type) {
        default:
            stmt = new ErrorStmt(token());
            diag.e(token().pos) << "expected a statement, got " << token().text << '\n';
            consume();
            break;

        case TK_Create: stmt = parse_CreateTableStmt(); break;
        case TK_Select: stmt = parse_SelectStmt(); break;
        case TK_Insert: stmt = parse_InsertStmt(); break;
        case TK_Update: stmt = parse_UpdateStmt(); break;
        case TK_Delete: stmt = parse_DeleteStmt(); break;
    }
    expect(TK_SEMICOL);
    return stmt;
}

/*======================================================================================================================
 * Statements
 *====================================================================================================================*/

Stmt * Parser::parse_CreateTableStmt()
{
    bool ok = true;
    Token start = token();
    std::vector<CreateTableStmt::attribute_type> attrs;

    /* 'CREATE' 'TABLE' identifier '(' */
    expect(TK_Create);
    expect(TK_Table);
    Token table_name = token();
    ok = ok and expect(TK_IDENTIFIER);
    expect(TK_LPAR);

    /* identifier data-type [ ',' identifier data-type ] */
    do {
        Token id = token();
        ok = ok and expect(TK_IDENTIFIER);
        const Type *type = parse_data_type();
        insist(type, "Must never be NULL");
        attrs.emplace_back(id, type);
    } while (accept(TK_COMMA));

    /* ')' */
    expect(TK_RPAR);

    if (not ok)
        return new ErrorStmt(start);

    return new CreateTableStmt(table_name, attrs);
}

Stmt * Parser::parse_SelectStmt()
{
    bool ok = true;
    Token start = token();
    bool select_all = false;
    std::vector<SelectStmt::selection_type> select;
    std::vector<SelectStmt::source_type> from;
    Expr *where = nullptr;
    std::vector<Expr*> group_by;
    Expr *having = nullptr;
    std::vector<SelectStmt::order_type> order_by;
    SelectStmt::limit_type limit;

    /* 'SELECT' */
    expect(TK_Select);

    /* ( '*' | expression [ 'AS' identifier ] ) */
    if (token() == TK_ASTERISK) {
        consume();
        select_all = true;
    } else {
        auto e = parse_Expr();
        Token tok;
        if (accept(TK_As)) {
            tok = token();
            ok = ok and expect(TK_IDENTIFIER);
        }
        select.push_back(std::make_pair(e, tok));
    }

    /* { ',' expression [ 'AS' identifier ] } */
    while (accept(TK_COMMA)) {
        auto e = parse_Expr();
        Token tok;
        if (accept(TK_As)) {
            tok = token();
            ok = ok and expect(TK_IDENTIFIER);
        }
        select.push_back(std::make_pair(e, tok));
    }

    /* 'FROM' identifier [ 'AS' identifier ] { ',' identifier [ 'AS' identifier ] } */
    expect(TK_From);
    do {
        Token table = token();
        Token as;
        ok = ok and expect(TK_IDENTIFIER);
        if (accept(TK_As)) {
            as = token();
            ok = ok and expect(TK_IDENTIFIER);
        }
        from.push_back(std::make_pair(table, as));
    } while (accept(TK_COMMA));

    if (accept(TK_Where)) where = parse_Expr();
    if (token() == TK_Group) group_by = parse_group_by_clause();
    if (accept(TK_Having)) having = parse_Expr();
    if (token() == TK_Order) order_by = parse_order_by_clause();
    if (token() == TK_Limit) limit = parse_limit_clause();

    if (not ok)
        return new ErrorStmt(start);

    return new SelectStmt(select_all, select, from, where, group_by, having, order_by, limit);
}

Stmt * Parser::parse_InsertStmt()
{
    bool ok = true;
    Token start = token();
    std::vector<InsertStmt::value_type> values;

    /* 'INSERT' 'INTO' identifier 'VALUES' */
    expect(TK_Insert);
    expect(TK_Into);
    Token table_name = token();
    ok = ok and expect(TK_IDENTIFIER);
    expect(TK_Values);

    /* tuple { ',' tuple } */
    do {
        /* '(' ( 'DEFAULT' | 'NULL' | expression ) { ',' ( 'DEFAULT' | 'NULL' | expression ) } ')' */
        InsertStmt::value_type value;
        expect(TK_LPAR);
        do {
            switch (token().type) {
                case TK_Default:
                    consume();
                    value.emplace_back(InsertStmt::I_Default, nullptr);
                    break;

                case TK_Null:
                    consume();
                    value.emplace_back(InsertStmt::I_Null, nullptr);
                    break;

                default: {
                    auto e = parse_Expr();
                    value.emplace_back(InsertStmt::I_Expr, e);
                    break;
                }
            }
        } while (accept(TK_COMMA));
        expect(TK_RPAR);
        values.emplace_back(value);
    } while (accept(TK_COMMA));

    if (not ok)
        return new ErrorStmt(start);

    return new InsertStmt(table_name, values);
}

Stmt * Parser::parse_UpdateStmt()
{
    bool ok = true;
    Token start = token();
    std::vector<UpdateStmt::set_type> set;
    Expr *where = nullptr;

    /* update-clause ::= 'UPDATE' identifier 'SET' identifier '=' expression { ',' identifier '=' expression } ; */
    expect(TK_Update);
    Token table_name = token();
    ok = ok and expect(TK_IDENTIFIER);
    expect(TK_Set);

    do {
        auto id = token();
        ok = ok and expect(TK_IDENTIFIER);
        expect(TK_EQUAL);
        auto e = parse_Expr();
        set.emplace_back(id, e);
    } while (accept(TK_COMMA));

    if (accept(TK_Where))
        where = parse_Expr();

    if (not ok)
        return new ErrorStmt(start);

    return new UpdateStmt(table_name, set, where);
}

Stmt * Parser::parse_DeleteStmt()
{
    bool ok = true;
    Token start = token();
    Expr *where = nullptr;

    /* delete-statement ::= 'DELETE' 'FROM' identifier [ where-clause ] ; */
    expect(TK_Delete);
    expect(TK_From);
    Token table_name = token();
    ok = ok and expect(TK_IDENTIFIER);

    if (accept(TK_Where))
        where = parse_Expr();

    if (not ok)
        return new ErrorStmt(start);

    return new DeleteStmt(table_name, where);
}

/*======================================================================================================================
 * Clauses
 *====================================================================================================================*/

std::vector<Expr*> Parser::parse_group_by_clause()
{
    /* 'GROUP' 'BY' designator { ',' designator } */
    std::vector<Expr*> group_by;
    expect(TK_Group);
    expect(TK_By);
    do
        group_by.push_back(parse_designator());
    while (accept(TK_COMMA));
    return group_by;
}

std::vector<std::pair<Expr*, bool>> Parser::parse_order_by_clause()
{
    /* 'ORDER' 'BY' designator [ 'ASC' | 'DESC' ] { ',' designator [ 'ASC' | 'DESC' ] } */
    std::vector<std::pair<Expr*, bool>> order_by;
    expect(TK_Order);
    expect(TK_By);

    do {
        auto d = parse_designator();
        if (accept(TK_Descending)) {
            order_by.push_back(std::make_pair(d, false));
        } else {
            accept(TK_Ascending);
            order_by.push_back(std::make_pair(d, true));
        }
    } while (accept(TK_COMMA));

    return order_by;
}

std::pair<Expr*, Expr*> Parser::parse_limit_clause()
{
    /* 'LIMIT' integer-constant [ 'OFFSET' integer-constant ] */
    expect(TK_Limit);
    Expr *limit = expect_integer();
    Expr *offset = nullptr;
    if (accept(TK_Offset))
        offset = expect_integer();
    return {limit, offset};
}

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

Expr * Parser::parse_Expr(const int precedence_lhs, Expr *lhs)
{
    /*
     * primary-expression::= designator | constant | '(' expression ')' ;
     * unary-expression ::= [ '+' | '-' | '~' ] postfix-expression ;
     * logical-not-expression ::= 'NOT' logical-not-expression | comparative-expression ;
     */
    switch (token().type) {
        /* primary-expression */
        case TK_IDENTIFIER:
            lhs = parse_designator(); // XXX For SUM(x), 'SUM' is parsed as designator; should be identifier.
            break;
        case TK_STRING_LITERAL:
        case TK_OCT_INT:
        case TK_DEC_INT:
        case TK_HEX_INT:
        case TK_DEC_FLOAT:
        case TK_HEX_FLOAT:
            lhs = new Constant(consume());
            break;
        case TK_LPAR:
            consume();
            lhs = parse_Expr();
            expect(TK_RPAR);
            break;

        /* unary-expression */
        case TK_PLUS:
        case TK_MINUS:
        case TK_TILDE: {
            auto tok = consume();
            int p = get_precedence(TK_TILDE); // the precedence of TK_TILDE equals that of unary plus and minus
            lhs = new UnaryExpr(tok, parse_Expr(p));
            break;
        }

        /* logical-NOT-expression */
        case TK_Not: {
            auto tok = consume();
            int p = get_precedence(tok.type);
            lhs = new UnaryExpr(tok, parse_Expr(p));
            break;
        }

        default:
            diag.e(token().pos) << "expected expression, got " << token().text << '\n';
            lhs = new ErrorExpr(token());
    }

    /* postfix-expression ::= postfix-expression '(' [ expression { ',' expression } ] ')' | primary-expression */
    while (accept(TK_LPAR)) {
        std::vector<Expr*> args;
        if (token().type != TK_RPAR) {
            do
                args.push_back(parse_Expr());
            while (accept(TK_COMMA));
        }
        expect(TK_RPAR);
        lhs = new FnApplicationExpr(lhs, args);
    }

    for (;;) {
        Token op = token();
        int p = get_precedence(op);
        if (precedence_lhs > p) return lhs; // left operator has higher precedence_lhs
        consume();

        Expr *rhs = parse_Expr(p + 1);
        lhs = new BinaryExpr(op, lhs, rhs);
    }
}

Expr * Parser::parse_designator()
{
    Token lhs = token();
    if (not expect(TK_IDENTIFIER))
        return new ErrorExpr(lhs);
    if (accept(TK_DOT)) {
        Token rhs = token();
        if (not expect(TK_IDENTIFIER))
            return new ErrorExpr(rhs);
        return new Designator(lhs, rhs); // tbl.attr
    }
    return new Designator(lhs); // attr
}

Expr * Parser::expect_integer()
{
    switch (token().type) {
        case TK_OCT_INT:
        case TK_DEC_INT:
        case TK_HEX_INT:
            return new Constant(consume());

        default:
            diag.e(token().pos) << "expected integer constant, got " << token().text << '\n';
            return new ErrorExpr(token());
    }
}

/*======================================================================================================================
 * Types
 *====================================================================================================================*/

const Type * Parser::parse_data_type()
{
    switch (token().type) {
        default:
            diag.e(token().pos) << "expected data-type, got " << token().text << '\n';
            return Type::Get_Error();

        /* BOOL */
        case TK_Bool:
            consume();
            return Type::Get_Boolean();

        /* 'CHAR' '(' decimal-constant ')' */
        case TK_Char:
        /* 'VARCHAR' '(' decimal-constant ')' */
        case TK_Varchar: {
            bool is_varying = token().type == TK_Varchar;
            consume();
            expect(TK_LPAR);
            Token tok = token();
            bool ok = expect(TK_DEC_INT);
            expect(TK_RPAR);
            if (not ok) return Type::Get_Error();
            errno = 0;
            std::size_t length = strtoul(tok.text, nullptr, 10);
            if (errno) {
                diag.e(tok.pos) << tok.text << " is not a valid length\n";
                return Type::Get_Error();
            }
            return is_varying ? Type::Get_Varchar(length) : Type::Get_Char(length);
        }

        /* 'INT' '(' decimal-constant ')' */
        case TK_Int: {
            consume();
            expect(TK_LPAR);
            Token tok = token();
            bool ok = expect(TK_DEC_INT);
            expect(TK_RPAR);
            if (not ok) return Type::Get_Error();
            errno = 0;
            std::size_t bytes = strtoul(tok.text, nullptr, 10);
            if (errno) {
                diag.e(tok.pos) << tok.text << " is not a valid size for an INT\n";
                return Type::Get_Error();
            }
            return Type::Get_Integer(bytes);
        }

        /* 'FLOAT' */
        case TK_Float:
            consume();
            return Type::Get_Float();

        /* 'DOUBLE' */
        case TK_Double:
            consume();
            return Type::Get_Double();

        /* 'DECIMAL' '(' decimal-constant [ ',' decimal-constant ] ')' */
        case TK_Decimal: {
            consume();
            expect(TK_LPAR);
            Token precision = token();
            Token scale;
            bool ok = expect(TK_DEC_INT);
            if (accept(TK_COMMA)) {
                scale = token();
                ok = ok and expect(TK_DEC_INT);
            }
            expect(TK_RPAR);
            if (not ok) return Type::Get_Error();
            errno = 0;
            std::size_t p = strtoul(precision.text, nullptr, 10);
            if (errno) {
                diag.e(precision.pos) << precision.text << " is not a valid precision for a DECIMAL\n";
                ok = false;
            }
            errno = 0;
            std::size_t s = strtoul(scale.text, nullptr, 10);
            if (errno) {
                diag.e(scale.pos) << scale.text << " is not a valid scale for a DECIMAL\n";
                ok = false;
            }
            if (not ok) return Type::Get_Error();
            return Type::Get_Decimal(p, s);
        }
    }
}
