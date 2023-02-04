#pragma once


#include "lex/Lexer.hpp"
#include <array>
#include <mutable/lex/Token.hpp>
#include <mutable/lex/TokenType.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/Diagnostic.hpp>


namespace m {

namespace ast {

struct M_EXPORT Parser
{
    using follow_set_t = std::array<bool, unsigned(TokenType::TokenType_MAX) + 1U>;

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

    const Token & token() {
        if (not tok_)
            tok_ = lexer.next();
        return tok_;
    }

    public:
    bool is(const TokenType tt) { return token() == tt; }
    bool no(const TokenType tt) { return token() != tt; }

    Token consume() {
        auto old = tok_;
        if (lexer.has_next())
            tok_ = lexer.next();
        else
            tok_ = Token();
        return old;
    }

    bool accept(const TokenType tt) {
        if (token() == tt or token() == TK_ERROR) {
            consume();
            return true;
        }
        return false;
    }

    bool expect(const TokenType tt) {
        if (accept(tt)) return true;
        diag.e(token().pos) << "expected " << tt << ", got " << token().text << '\n';
        return false;
    }

    void recover(const follow_set_t &FS) { while (not FS[token().type]) consume(); }

    std::unique_ptr<Command> parse();
    std::unique_ptr<Instruction> parse_Instruction();
    std::unique_ptr<Stmt> parse_Stmt();

    /* Statements */
    std::unique_ptr<Stmt> parse_CreateDatabaseStmt();
    std::unique_ptr<Stmt> parse_UseDatabaseStmt();
    std::unique_ptr<Stmt> parse_CreateTableStmt();
    std::unique_ptr<Stmt> parse_SelectStmt();
    std::unique_ptr<Stmt> parse_InsertStmt();
    std::unique_ptr<Stmt> parse_UpdateStmt();
    std::unique_ptr<Stmt> parse_DeleteStmt();
    std::unique_ptr<Stmt> parse_ImportStmt();

    /* Clauses */
    std::unique_ptr<Clause> parse_SelectClause();
    std::unique_ptr<Clause> parse_FromClause();
    std::unique_ptr<Clause> parse_WhereClause();
    std::unique_ptr<Clause> parse_GroupByClause();
    std::unique_ptr<Clause> parse_HavingClause();
    std::unique_ptr<Clause> parse_OrderByClause();
    std::unique_ptr<Clause> parse_LimitClause();

    /* Expressions */
    std::unique_ptr<Expr> parse_Expr(int precedence_lhs = 0, std::unique_ptr<Expr> lhs = nullptr);
    std::unique_ptr<Expr> parse_designator();
    std::unique_ptr<Expr> expect_integer();

    /* Types */
    const Type * parse_data_type();
};

}

}
