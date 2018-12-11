#pragma once


#include "lex/Lexer.hpp"
#include "lex/Token.hpp"
#include "lex/TokenType.hpp"
#include "util/Diagnostic.hpp"


namespace db {

struct Parser
{
    public:
    Lexer      &lexer;
    Diagnostic &diag;

    private:
    Token tok_;

    public:
    explicit Parser(Lexer &lexer)
        : lexer(lexer)
        , diag(lexer.diag)
        , tok_(Position(lexer.filename), "ERROR", TK_ERROR)
    {
        consume();
    }

    Token token() const { return tok_; }

    bool no(const TokenType tt) { return not token() != tt; }

    void consume() { tok_ = lexer.next(); }

    bool accept(const TokenType tt) {
        if (token() == tt or token() == TK_ERROR) {
            consume();
            return true;
        }
        return false;
    }

    bool expect(const TokenType tt) {
        if (accept(tt)) return true;
        diag.e(token().pos) << "expected " << tt << ", but got " << token().text << '\n';
        return false;
    }

    bool expect_consume(TokenType const tt) {
        if (expect(tt)) return true;
        consume();
        return false;
    }

    void parse();

    /* Statements */
    void parse_Stmt();
    void parse_SelectStmt();
    void parse_UpdateStmt();
    void parse_DeleteStmt();

    /* Clauses */
    void parse_select_clause();
    void parse_from_clause();
    void parse_where_clause();
    void parse_group_by_clause();
    void parse_order_by_clause();
    void parse_limit_clause();

    /* Expressions */
    void parse_PrimaryExpr();
    void parse_PostfixExpr();
    void parse_UnaryExpr();
    void parse_BinaryExpr();
    void parse_BinaryExpr(void *lhs, int const p_lhs = 0 );
    void parse_Expr();

    /* Miscellaneous */
    void parse_designator();
    void expect_integer();
};

}
