#include "parse/Parser.hpp"

#include "util/macro.hpp"

#undef DEBUG
#define DEBUG(X)

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

void Parser::parse()
{
    switch (token().type) {
        case TK_Select: return parse_SelectStmt();
        case TK_Update: return parse_UpdateStmt();
        case TK_Delete: return parse_DeleteStmt();

        default:
            diag.e(token().pos) << "expected a statement, got " << token().text << '\n';
            consume();
    }
}

/*======================================================================================================================
 * Statements
 *====================================================================================================================*/

void Parser::parse_SelectStmt()
{
    /* select-clause from-clause [where-clause] [group_by-clause] [order_by-clause] [limit-clause] */
    parse_select_clause();
    parse_from_clause();
    if (token() == TK_Where) parse_where_clause();
    if (token() == TK_Group) parse_group_by_clause();
    if (token() == TK_Order) parse_order_by_clause();
    if (token() == TK_Limit) parse_limit_clause();
}

void Parser::parse_InsertStmt()
{
    unreachable("TODO: not implemented");
}

void Parser::parse_UpdateStmt()
{
    unreachable("TODO: not implemented");
}

void Parser::parse_DeleteStmt()
{
    unreachable("TODO: not implemented");
}

/*======================================================================================================================
 * Clauses
 *====================================================================================================================*/

void Parser::parse_select_clause()
{
    /* 'SELECT' */
    expect(TK_Select);

    /* ( '*' | expression [ 'AS' identifier ] ) */
    if (token() == TK_ASTERISK)
        consume();
    else {
        parse_Expr();
        if (accept(TK_As))
            expect(TK_IDENTIFIER);
    }

    /* { ',' expression [ 'AS' identifier ] } */
    while (token() == TK_COMMA) {
        consume();
        parse_Expr();
        if (accept(TK_As))
            expect(TK_IDENTIFIER);
    }
}

void Parser::parse_from_clause()
{
    /* 'FROM' identifier [ 'AS' identifier ] { ',' identifier [ 'AS' identifier ] } */
    expect(TK_From);
    do {
        expect(TK_IDENTIFIER);
        if (accept(TK_As))
            expect(TK_IDENTIFIER);
    } while (accept(TK_COMMA));
}

void Parser::parse_where_clause()
{
    /* 'WHERE' expression */
    expect(TK_Where);
    parse_Expr();
}

void Parser::parse_group_by_clause()
{
    /* 'GROUP' 'BY' designator { ',' designator } */
    expect(TK_Group);
    expect(TK_By);
    do
        parse_designator();
    while (accept(TK_COMMA));
}

void Parser::parse_order_by_clause()
{
    /* 'ORDER' 'BY' designator [ 'ASC' | 'DESC' ] { ',' designator [ 'ASC' | 'DESC' ] } */
    expect(TK_Order);
    expect(TK_By);

    do {
        parse_designator();
        accept(TK_Ascending) or accept(TK_Descending);
    } while (accept(TK_COMMA));
}

void Parser::parse_limit_clause()
{
    /* 'LIMIT' integer-constant [ 'OFFSET' integer-constant ] */
    expect(TK_Limit);
    expect_integer();
    if (accept(TK_Offset))
        expect_integer();
}

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

Expr * Parser::parse_Expr(const int precedence_lhs, Expr *lhs)
{
    DEBUG('(' << precedence_lhs << ", " << (lhs ? "expr" : "NULL") << ')');

    /*
     * primary-expression::= designator | constant | '(' expression ')' ;
     * unary-expression ::= [ '+' | '-' | '~' ] postfix-expression ;
     * logical-not-expression ::= 'NOT' logical-not-expression | comparative-expression ;
     */
    switch (token().type) {
        /* primary-expression */
        case TK_IDENTIFIER:
            lhs = parse_designator();
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

        /* logical-NOT-expression */
        case TK_Not:
        /* unary-expression */
        case TK_PLUS:
        case TK_MINUS:
        case TK_TILDE: {
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
        DEBUG("potential binary operator " << token().text << " with precedence " << p);
        if (precedence_lhs > p) return lhs; // left operator has higher precedence_lhs
        DEBUG("binary operator with higher precedence " << token());
        consume();

        DEBUG("recursive call to parse_Expr(" << p+1 << ')');
        Expr *rhs = parse_Expr(p + 1);
        lhs = new BinaryExpr(op, lhs, rhs);
    }
}

#if 0
Expr * Parser::parse_Expr(void *lhs, const int precedence_lhs)
{
    for (;;) {
        Token op = token();
        int p = get_precedence(op);
        if (p < precedence_lhs) return;
        consume();

        /* TODO what to do about NOT, which has lower precedence than some binary operators?
         * Maybe we should do LR parsing for all expressions, and add a respective entry for each operator (binary and
         * unique) to `get_precedence()`. */

        void *rhs = nullptr;
        parse_Expr();
        parse_Expr(rhs, p + 1);
        /* TODO merge lhs/rhs */
    }
}
#endif

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
