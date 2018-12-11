#include "parse/Parser.hpp"


using namespace db;


/** Returns the precedence of an operator.  A higher value means the operator has higher precedence. */
int get_precedence(const TokenType tt)
{
    int p = 0;
    /* List all binary operators.  The higher up an operator is in the switch statement, the higher its precedence. */
    switch (tt) {
        default:                    return -1;
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

void Parser::parse_PrimaryExpr()
{
    /* designator | constant | '(' expression ')' */
    switch (token().type) {
        case TK_IDENTIFIER:
            return parse_designator();

        case TK_True:
        case TK_False:
        case TK_STRING_LITERAL:
        case TK_OCT_INT:
        case TK_DEC_INT:
        case TK_HEX_INT:
        case TK_DEC_FLOAT:
        case TK_HEX_FLOAT:
            consume();
            return;

        case TK_LPAR:
            consume();
            parse_Expr();
            expect(TK_LPAR);
            return;

        default:
            /* TODO: careful when not consuming a token, this can lead to unexpected divergence */
            diag.e(token().pos) << "expected an expression, got " << token().text << '\n';
            return;
    }
}

void Parser::parse_PostfixExpr()
{
    /* postfix-expression '(' [ expression { ',' expression } ] ')' | (* function call *)
     * primary-expression
     */
    parse_PrimaryExpr();

    for (;;) {
        switch (token().type) {
            default: return;

            case TK_LPAR: {
                consume();
                if (token() != TK_LPAR) {
                    do
                        parse_Expr();
                    while (accept(TK_COMMA));
                }
                expect(TK_RPAR);
                break;
            }
        }
    }
}

void Parser::parse_UnaryExpr()
{
    /* [ '+' | '-' ] postfix-expression */
    accept(TK_PLUS) or accept(TK_MINUS);
    parse_PostfixExpr();
}

void Parser::parse_BinaryExpr()
{
    parse_UnaryExpr();
    parse_BinaryExpr(nullptr);
}

void Parser::parse_BinaryExpr(void *lhs, const int precedence_lhs)
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
        parse_UnaryExpr();
        parse_BinaryExpr(rhs, p + 1);
        /* TODO merge lhs/rhs */
    }
}

void Parser::parse_Expr()
{
    unreachable("TODO: not implemented");
}

/*======================================================================================================================
 * Miscellaneous
 *====================================================================================================================*/

void Parser::parse_designator()
{
    expect(TK_IDENTIFIER);
    if (accept(TK_DOT))
        expect(TK_IDENTIFIER);
}

void Parser::expect_integer()
{
    switch (token().type) {
        case TK_OCT_INT:
        case TK_DEC_INT:
        case TK_HEX_INT:
            consume();
            break;

        default:
            diag.e(token().pos) << "expected integer constant, got " << token().text << '\n';
            break;
    }
}
