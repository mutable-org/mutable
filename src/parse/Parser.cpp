#include "parse/Parser.hpp"


using namespace db;


int get_precedence(const TokenType tt)
{
    int p = 0;
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

void Parser::parse() {
    unreachable("TODO: not implemented");
}

void Parser::parseBinaryExpr()
{
    unreachable("TODO: parse unary-expression");
    parseBinaryExpr(nullptr);
}

void Parser::parseBinaryExpr(void *lhs, const int precedence_lhs)
{
    for (;;) {
        Token op = token();
        int p = get_precedence(op.type);

        if (p < precedence_lhs) return;

        consume();
        void *rhs = nullptr;
        unreachable("TODO: parse unary-expression");
        parseBinaryExpr(rhs, p + 1);

        unreachable("TODO: merge lhs/rhs");
    }
}
