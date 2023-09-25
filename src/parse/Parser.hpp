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
    using follow_set_t = std::array<bool, unsigned(TokenType::TokenType_MAX) + 1>;

    public:
    Lexer      &lexer;
    Diagnostic &diag;

    private:
    std::array<Token, 2> lookahead_;

    public:
    explicit Parser(Lexer &lexer)
        : lexer(lexer)
        , diag(lexer.diag)
        , lookahead_({Token(), Token()})
    {
        consume();
        consume();
    }

    template<unsigned Idx = 0>
    const Token & token() { return lookahead_[Idx]; }

    bool is(const TokenType tt) { return token() == tt; }
    bool no(const TokenType tt) { return token() != tt; }

    Token consume() {
        auto old = token();
        for (std::size_t i = 1; i != lookahead_.size(); ++i)
            lookahead_[i - 1] = lookahead_[i];
        lookahead_.back() = lexer.next();
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

    /** Consumes tokens until the first occurence of a token in the follow set \p FS is found. */
    void recover(const follow_set_t &FS) { while (token() and not FS[token().type]) consume(); }

    /** Consumes tokens until the first occurence of a token in the follow set \p FS is found. Constructs an object of
     * type `T` on \p start and returns a unique pointer to it.  Here, type `T` is either `ErrorStmt` or `ErrorClause`.
     */
    template<typename T>
    std::unique_ptr<T> recover(Token start, const follow_set_t &FS) {
        recover(FS);
        return std::make_unique<T>(start);
    }

    std::unique_ptr<Command> parse();
    std::unique_ptr<Instruction> parse_Instruction();
    std::unique_ptr<Stmt> parse_Stmt();

    /* Statements */
    std::unique_ptr<Stmt> parse_CreateDatabaseStmt();
    std::unique_ptr<Stmt> parse_DropDatabaseStmt();
    std::unique_ptr<Stmt> parse_UseDatabaseStmt();
    std::unique_ptr<Stmt> parse_CreateTableStmt();
    std::unique_ptr<Stmt> parse_DropTableStmt();
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
