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

    bool no(const TokenType tt) { return token().type != tt; }

    void consume() { tok_ = lexer.next(); }

    bool accept(const TokenType tt) {
        if (tt == token().type or TK_ERROR == token().type) {
            consume();
            return true;
        }
        return false;
    }

    bool expect(const TokenType tt) {
        if (accept(tt)) return true;
        diag.e(tok_.pos) << "expected " << tt << ", but got " << tok_.text << "\n";
        return false;
    }

    bool expect_consume(TokenType const tt) {
        if (expect(tt)) return true;
        consume();
        return false;
    }

    void parse();

    /* Statements */
    void parseStmt();
    void parseSelectStmt();
    void parseUpdateStmt();
    void parseDeleteStmt();

    /* Expressions */
    void parseExpr();
    void parseBinaryExpr();
    void parseBinaryExpr(void *lhs, int const p_lhs = 0 );
};

}
