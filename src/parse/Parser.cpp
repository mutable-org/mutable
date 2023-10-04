#include "parse/Parser.hpp"

#include <cerrno>
#include <cstdlib>
#include <initializer_list>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Schema.hpp>
#include <utility>


using namespace m;
using namespace m::ast;


namespace {

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
        /* string concat */
        case TK_DOTDOT:             ++p;
        /* comparison */
        case TK_LESS:
        case TK_GREATER:
        case TK_LESS_EQUAL:
        case TK_GREATER_EQUAL:
        case TK_EQUAL:
        case TK_BANG_EQUAL:
        case TK_Like:               ++p;
        /* logical NOT */
        case TK_Not:                ++p;
        /* logical AND */
        case TK_And:                ++p;
        /* logical OR */
        case TK_Or:                 ++p;
    }
    return p;
}

/** Returns `true` if \p tt is an integral `TokenType`. */
bool is_integer(TokenType tt)
{
    switch (tt) {
        case TK_OCT_INT:
        case TK_DEC_INT:
        case TK_HEX_INT:
            return true;

        default:
            return false;
    }
}

}


/*======================================================================================================================
 * Follow sets
 *====================================================================================================================*/

namespace {

constexpr Parser::follow_set_t make_follow_set(std::initializer_list<TokenType> tokens)
{
    Parser::follow_set_t F{};
    for (TokenType tk : tokens) {
        M_insist(tk < TokenType::TokenType_MAX);
        F[tk] = true;
    }
    return F;
}

#define M_FOLLOW(NAME, SET) \
const Parser::follow_set_t follow_set_##NAME = make_follow_set SET ;
#include "tables/FollowSet.tbl"
#undef M_FOLLOW

}


/*======================================================================================================================
 * Parser
 *====================================================================================================================*/

std::unique_ptr<Command> Parser::parse()
{
    if (token().type == TK_INSTRUCTION)
        return parse_Instruction();
    else
        return parse_Stmt();
}

std::unique_ptr<Instruction> Parser::parse_Instruction()
{
    auto &C = Catalog::Get();
    M_insist(is(TK_INSTRUCTION));

    Token instr = consume();
    std::string_view sv(instr.text);
    const char *delimiter = " \n";

    /*----- Isolate the instruction's name -----*/
    std::string::size_type end = sv.find_first_of(delimiter);
    const char *instruction_name = C.pool(sv.substr(1, end - 1)); // skip leading `\`

    /*----- Separate the arguments. -----*/
    std::vector<std::string> args;
    for (;;) {
        std::string::size_type start = sv.find_first_not_of(delimiter, end);
        if (start == std::string::npos)
            break;
        end = sv.find_first_of(delimiter, start);
        args.emplace_back(sv.substr(start, end - start));
    }

    expect(TK_SEMICOL);
    return std::make_unique<Instruction>(instr, instruction_name, std::move(args));
}

std::unique_ptr<Stmt> Parser::parse_Stmt()
{
    Token start = token();
    std::unique_ptr<Stmt> stmt = nullptr;
    switch (token().type) {
        default:
            stmt = std::make_unique<ErrorStmt>(token());
            diag.e(token().pos) << "expected a statement, got " << token().text << '\n';
            consume();
            return recover<ErrorStmt>(start, follow_set_STATEMENT);

        case TK_SEMICOL: return std::make_unique<EmptyStmt>(consume());

        case TK_Create:
            switch (token<1>().type) {
                default:
                    stmt = std::make_unique<ErrorStmt>(token());
                    diag.e(token<1>().pos) << "expected a create database statement or a create table statement, got "
                                           << token<1>().text << '\n';
                    consume();
                    return recover<ErrorStmt>(start, follow_set_STATEMENT);

                case TK_Database: stmt = parse_CreateDatabaseStmt(); break;
                case TK_Table:    stmt = parse_CreateTableStmt(); break;
                case TK_Unique:
                case TK_Index:    stmt = parse_CreateIndexStmt(); break;
            }
            break;

        case TK_Drop:
            switch (token<1>().type) {
                default:
                    stmt = std::make_unique<ErrorStmt>(token());
                    diag.e(token().pos) << "expected a drop database, table, or index statement, got "
                                        << token().text << '\n';
                    consume();
                    return recover<ErrorStmt>(start, follow_set_STATEMENT);

                case TK_Database: stmt = parse_DropDatabaseStmt(); break;
                case TK_Table:    stmt = parse_DropTableStmt(); break;
                case TK_Index:    stmt = parse_DropIndexStmt(); break;
            }
            break;

        case TK_Use:    stmt = parse_UseDatabaseStmt(); break;
        case TK_Select: stmt = parse_SelectStmt(); break;
        case TK_Insert: stmt = parse_InsertStmt(); break;
        case TK_Update: stmt = parse_UpdateStmt(); break;
        case TK_Delete: stmt = parse_DeleteStmt(); break;
        case TK_Import: stmt = parse_ImportStmt(); break;
    }
    expect(TK_SEMICOL);
    return stmt;
}

/*======================================================================================================================
 * statements
 *====================================================================================================================*/

std::unique_ptr<Stmt> Parser::parse_CreateDatabaseStmt()
{
    Token start = token();

    /* 'CREATE' 'DATABASE' identifier */
    if (not expect(TK_Create)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    if (not expect(TK_Database))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    Token database_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    return std::make_unique<CreateDatabaseStmt>(database_name);
}

std::unique_ptr<Stmt> Parser::parse_DropDatabaseStmt()
{
    Token start = token();

    /* 'DROP' 'DATABASE' */
    if (not expect(TK_Drop)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    if (not expect(TK_Database))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /* [ 'IF' 'EXISTS' ] */
    bool has_if_exists = false;
    if (accept(TK_If)) {
        if (not expect(TK_Exists))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
        has_if_exists = true;
    }

    /* identifier */
    Token database_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    return std::make_unique<DropDatabaseStmt>(database_name, has_if_exists);
}

std::unique_ptr<Stmt> Parser::parse_UseDatabaseStmt()
{
    Token start = token();

    /* 'USE' identifier */
    if (not expect(TK_Use)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    Token database_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    return std::make_unique<UseDatabaseStmt>(database_name);
}

std::unique_ptr<Stmt> Parser::parse_CreateTableStmt()
{
    Token start = token();
    std::vector<std::unique_ptr<CreateTableStmt::attribute_definition>> attrs;

    /* 'CREATE' 'TABLE' identifier '(' */
    if (not expect(TK_Create)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    if (not expect(TK_Table))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    Token table_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    if (not expect(TK_LPAR))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /* identifier data-type { constraint } [ ',' identifier data-type { constraint } ] */
    do {
        Token id = token();
        if (not expect(TK_IDENTIFIER))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);

        const Type *type = M_notnull(parse_data_type());

        /* Parse the list of constraints. */
        std::vector<std::unique_ptr<Constraint>> constraints;
        for (;;) {
            switch (token().type) {
                /* 'PRIMARY' 'KEY' */
                case TK_Primary: {
                    Token tok = consume();
                    if (not expect(TK_Key)) goto constraint_error_recovery;
                    constraints.push_back(std::make_unique<PrimaryKeyConstraint>(tok));
                    break;
                }

                /* 'NOT' 'NULL' */
                case TK_Not: {
                    Token tok = consume();
                    if (not expect(TK_Null)) goto constraint_error_recovery;
                    constraints.push_back(std::make_unique<NotNullConstraint>(tok));
                    break;
                }

                /* 'UNIQUE' */
                case TK_Unique: {
                    Token tok = consume();
                    constraints.push_back(std::make_unique<UniqueConstraint>(tok));
                    break;
                }

                /* 'CHECK' '(' expression ')' */
                case TK_Check: {
                    Token tok = consume();
                    if (not expect(TK_LPAR)) goto constraint_error_recovery;
                    std::unique_ptr<Expr> cond = parse_Expr();
                    if (not expect(TK_RPAR)) goto constraint_error_recovery;
                    constraints.push_back(std::make_unique<CheckConditionConstraint>(tok, std::move(cond)));
                    break;
                }

                /* 'REFERENCES' identifier '(' identifier ')' */
                case TK_References: {
                    Token tok = consume();
                    Token ref_table_name = token();
                    if (not expect(TK_IDENTIFIER)) goto constraint_error_recovery;
                    if (not expect(TK_LPAR)) goto constraint_error_recovery;
                    Token attr_name = token();
                    if (not expect(TK_IDENTIFIER)) goto constraint_error_recovery;
                    if (not expect(TK_RPAR)) goto constraint_error_recovery;
                    constraints.push_back(std::make_unique<ReferenceConstraint>(
                        tok,
                        ref_table_name,
                        attr_name,
                        ReferenceConstraint::ON_DELETE_RESTRICT)
                    );
                    break;
                }

                default:
                    goto exit_constraints;
            }

        }
constraint_error_recovery:
        recover(follow_set_CONSTRAINT);
exit_constraints:
        attrs.push_back(std::make_unique<CreateTableStmt::attribute_definition>(id,
                                                                                std::move(type),
                                                                                std::move(constraints)));
    } while (accept(TK_COMMA));

    /* ')' */
    if (not expect(TK_RPAR))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    return std::make_unique<CreateTableStmt>(table_name, std::move(attrs));
}

std::unique_ptr<Stmt> Parser::parse_DropTableStmt()
{
    Token start = token();

    /* 'DROP' 'TABLE' */
    if (not expect(TK_Drop)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    if (not expect(TK_Table))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /* [ 'IF' 'EXISTS' ] */
    bool has_if_exists = false;
    if (accept(TK_If)) {
        if (not expect(TK_Exists))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
        has_if_exists = true;
    }

    /* identifier { ',' identifier } */
    std::vector<std::unique_ptr<Token>> table_names;
    do {
        Token table_name = token();
        if (not expect(TK_IDENTIFIER))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
        table_names.emplace_back(std::make_unique<Token>(table_name));
    } while (accept(TK_COMMA));

    return std::make_unique<DropTableStmt>(std::move(table_names), has_if_exists);
}

std::unique_ptr<Stmt> Parser::parse_CreateIndexStmt()
{
    Token start = token();

    /* 'CREATE' [ 'UNIQUE' ] 'INDEX' */
    if (not expect(TK_Create))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    bool has_unique = false;
    if (accept(TK_Unique))
        has_unique = true;

    if (not expect(TK_Index))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /* [ [ 'IF' 'NOT' 'EXISTS' ] identifier ] */
    bool has_if_not_exists = false;
    Token index_name = Token();
    if (accept(TK_If)) {
        if (not expect(TK_Not))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
        if (not expect(TK_Exists))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
        has_if_not_exists = true;
        index_name = token();
        if (not expect(TK_IDENTIFIER))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
    } else if (token().type == TK_IDENTIFIER)
        index_name = consume();

    /* 'ON' identifier */
    if (not expect(TK_On))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    Token table_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /* [ 'USING' identifier */
    Token method = Token();
    if (accept(TK_Using)) {
        if (token().type != TK_IDENTIFIER and token().type != TK_Default) {
            diag.e(token().pos) << "expected an identifier or DEFAULT, got " << token().text << '\n';
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
        }
        method = consume();
    }

    /* '(' key_field { ',' key_field } ')' */
    if (not expect(TK_LPAR))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    std::vector<std::unique_ptr<Expr>> key_fields;
    do {
        switch (token().type) {
            /* identifier */
            case TK_IDENTIFIER: {
                auto id = std::make_unique<Designator>(consume());
                key_fields.emplace_back(std::move(id));
                break;
            }
            /* '(' expression ')' */
            case TK_LPAR: {
                auto expr = parse_Expr();
                key_fields.emplace_back(std::move(expr));
                break;
            }
            default: {
                diag.e(token().pos) << "expected an identifier or expression, got " << token().text << '\n';
                return recover<ErrorStmt>(start, follow_set_STATEMENT);
            }
        }
    } while(accept(TK_COMMA));
    if (not expect(TK_RPAR))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    return std::make_unique<CreateIndexStmt>(
        /* has_unique=        */ has_unique,
        /* has_if_not_exists= */ has_if_not_exists,
        /* index_name=        */ index_name,
        /* table_name=        */ table_name,
        /* method=            */ method,
        /* key_fields=        */ std::move(key_fields)
    );
}

std::unique_ptr<Stmt> Parser::parse_DropIndexStmt()
{
    Token start = token();

    /* 'DROP' 'INDEX' */
    if (not expect(TK_Drop)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    if (not expect(TK_Index))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /* [ 'IF' 'EXISTS' ] */
    bool has_if_exists = false;
    if (accept(TK_If)) {
        if (not expect(TK_Exists))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
        has_if_exists = true;
    }

    /* identifier { ',' identifier } */
    std::vector<std::unique_ptr<Token>> index_names;
    do {
        Token index_name = token();
        if (not expect(TK_IDENTIFIER))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
        index_names.emplace_back(std::make_unique<Token>(index_name));
    } while (accept(TK_COMMA));

    return std::make_unique<DropIndexStmt>(std::move(index_names), has_if_exists);
}

std::unique_ptr<Stmt> Parser::parse_SelectStmt()
{
    std::unique_ptr<Clause> select = parse_SelectClause();
    std::unique_ptr<Clause> from = nullptr;
    std::unique_ptr<Clause> where = nullptr;
    std::unique_ptr<Clause> group_by = nullptr;
    std::unique_ptr<Clause> having = nullptr;
    std::unique_ptr<Clause> order_by = nullptr;
    std::unique_ptr<Clause> limit = nullptr;

    if (token() == TK_From)
        from = parse_FromClause();
    if (token() == TK_Where)
        where = parse_WhereClause();
    if (token() == TK_Group)
        group_by = parse_GroupByClause();
    if (token() == TK_Having)
        having = parse_HavingClause();
    if (token() == TK_Order)
        order_by = parse_OrderByClause();
    if (token() == TK_Limit)
        limit = parse_LimitClause();

    return std::make_unique<SelectStmt>(std::move(select),
                                        std::move(from),
                                        std::move(where),
                                        std::move(group_by),
                                        std::move(having),
                                        std::move(order_by),
                                        std::move(limit));
}

std::unique_ptr<Stmt> Parser::parse_InsertStmt()
{
    Token start = token();

    /* 'INSERT' 'INTO' identifier 'VALUES' */
    if (not expect(TK_Insert)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    if (not expect(TK_Into))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    Token table_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    if (not expect(TK_Values))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /* tuple { ',' tuple } */
    std::vector<InsertStmt::tuple_t> tuples;
    do {
        /* '(' ( 'DEFAULT' | 'NULL' | expression ) { ',' ( 'DEFAULT' | 'NULL' | expression ) } ')' */
        InsertStmt::tuple_t tuple;
        if (not expect(TK_LPAR)) goto tuple_error_recovery;
        do {
            switch (token().type) {
                case TK_Default:
                    consume();
                    tuple.emplace_back(InsertStmt::I_Default, nullptr);
                    break;

                case TK_Null:
                    consume();
                    tuple.emplace_back(InsertStmt::I_Null, nullptr);
                    break;

                default: {
                    auto e = parse_Expr();
                    tuple.emplace_back(InsertStmt::I_Expr, std::move(e));
                    break;
                }
            }
        } while (accept(TK_COMMA));
        if (not expect(TK_RPAR)) goto tuple_error_recovery;
        tuples.emplace_back(std::move(tuple));
        continue;
tuple_error_recovery:
        recover(follow_set_TUPLE);
    } while (accept(TK_COMMA));

    return std::make_unique<InsertStmt>(table_name, std::move(tuples));
}

std::unique_ptr<Stmt> Parser::parse_UpdateStmt()
{
    Token start = token();

    /* 'UPDATE' identifier 'SET' */
    if (not expect(TK_Update)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    Token table_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    if (not expect(TK_Set))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /* identifier '=' expression { ',' identifier '=' expression } ; */
    std::vector<UpdateStmt::set_type> set;
    do {
        auto id = token();
        if (not expect(TK_IDENTIFIER))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);

        if (not expect(TK_EQUAL))
            return recover<ErrorStmt>(start, follow_set_STATEMENT);

        auto e = parse_Expr();
        set.emplace_back(id, std::move(e));
    } while (accept(TK_COMMA));

    /* [ where-clause ] */
    std::unique_ptr<Clause> where = nullptr;
    if (token() == TK_Where)
        where = parse_WhereClause();

    return std::make_unique<UpdateStmt>(table_name, std::move(set), std::move(where));
}

std::unique_ptr<Stmt> Parser::parse_DeleteStmt()
{
    Token start = token();

    /* 'DELETE' 'FROM' identifier */
    if (not expect(TK_Delete)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    if (not expect(TK_From))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    Token table_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    /*[ where-clause ] ; */
    std::unique_ptr<Clause> where = nullptr;
    if (token() == TK_Where)
        where = parse_WhereClause();

    return std::make_unique<DeleteStmt>(table_name, std::move(where));
}

std::unique_ptr<Stmt> Parser::parse_ImportStmt()
{
    Token start = token();

    /* 'IMPORT' 'INTO' identifier */
    if (not expect(TK_Import)) {
        consume();
        return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }

    if (not expect(TK_Into))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    Token table_name = token();
    if (not expect(TK_IDENTIFIER))
        return recover<ErrorStmt>(start, follow_set_STATEMENT);

    switch (token().type) {
        /* 'DSV' string-literal */
        case TK_Dsv: {
            consume();

            DSVImportStmt stmt;

            stmt.table_name = table_name;
            stmt.path = token();
            if (not expect(TK_STRING_LITERAL))
                return recover<ErrorStmt>(start, follow_set_STATEMENT);

            /* [ 'ROWS' integer-constant ] */
            if (accept(TK_Rows)) {
                stmt.rows = token();
                if (token() == TK_DEC_INT or token() == TK_OCT_INT) {
                    consume();
                } else {
                    diag.e(token().pos) << "expected a decimal integer, got " << token().text << '\n';
                    return recover<ErrorStmt>(start, follow_set_STATEMENT);
                }
            }

            /* [ 'DELIMITER' string-literal ] */
            if (accept(TK_Delimiter)) {
                stmt.delimiter = token();
                if (not expect(TK_STRING_LITERAL))
                    return recover<ErrorStmt>(start, follow_set_STATEMENT);
            }

            /* [ 'ESCAPE' string-literal ] */
            if (accept(TK_Escape)) {
                stmt.escape = token();
                if (not expect(TK_STRING_LITERAL))
                    return recover<ErrorStmt>(start, follow_set_STATEMENT);
            }

            /* [ 'QUOTE' string-literal ] */
            if (accept(TK_Quote)) {
                stmt.quote = token();
                if (not expect(TK_STRING_LITERAL))
                    return recover<ErrorStmt>(start, follow_set_STATEMENT);
            }

            /* [ 'HAS' 'HEADER' ] */
            if (accept(TK_Has)) {
                if (not expect(TK_Header))
                    return recover<ErrorStmt>(start, follow_set_STATEMENT);
                stmt.has_header = true;
            }

            /* [ 'SKIP' 'HEADER' ] */
            if (accept(TK_Skip)) {
                if (not expect(TK_Header))
                    return recover<ErrorStmt>(start, follow_set_STATEMENT);
                stmt.skip_header = true;
            }

            return std::make_unique<DSVImportStmt>(stmt);
        }

        default:
            diag.e(token().pos) << "Unrecognized input format \"" << token().text << "\".\n";
            return recover<ErrorStmt>(start, follow_set_STATEMENT);
    }
}

/*======================================================================================================================
 * Clauses
 *====================================================================================================================*/

std::unique_ptr<Clause> Parser::parse_SelectClause()
{
    Token start = token();

    /* 'SELECT' */
    if (not expect(TK_Select)) {
        consume();
        return recover<ErrorClause>(start, follow_set_SELECT_CLAUSE);
    }

    /* ( '*' | expression [ [ 'AS' ] identifier ] ) */
    Token select_all = Token();
    std::vector<SelectClause::select_type> select;
    if (token() == TK_ASTERISK) {
        select_all = token();
        consume();
    } else {
        auto e = parse_Expr();
        Token tok;
        if (accept(TK_As)) {
            tok = token();
            if (not expect(TK_IDENTIFIER))
                return recover<ErrorClause>(start, follow_set_SELECT_CLAUSE);
        } else if (token().type == TK_IDENTIFIER) {
            tok = token();
            consume();
        }
        select.emplace_back(std::move(e), tok);
    }

    /* { ',' expression [ [ 'AS' ] identifier ] } */
    while (accept(TK_COMMA)) {
        auto e = parse_Expr();
        Token tok;
        if (accept(TK_As)) {
            tok = token();
            if (not expect(TK_IDENTIFIER))
                return recover<ErrorClause>(start, follow_set_SELECT_CLAUSE);
        } else if (token().type == TK_IDENTIFIER) {
            tok = token();
            consume();
        }
        select.emplace_back(std::move(e), tok);
    }

    return std::make_unique<SelectClause>(start, std::move(select), select_all);
}

std::unique_ptr<Clause> Parser::parse_FromClause()
{
    Token start = token();

    /* 'FROM' */
    if (not expect(TK_From)) {
        consume();
        return recover<ErrorClause>(start, follow_set_FROM_CLAUSE);
    }

    /* table-or-select-statement { ',' table-or-select-statement } */
    std::vector<FromClause::from_type> from;
    do {
        Token alias;
        if (accept(TK_LPAR)) {
            std::unique_ptr<Stmt> S = parse_SelectStmt();
            if (not expect(TK_RPAR))
                return recover<ErrorClause>(start, follow_set_FROM_CLAUSE);
            accept(TK_As);
            alias = token();
            if (not expect(TK_IDENTIFIER))
                return recover<ErrorClause>(start, follow_set_FROM_CLAUSE);
            from.emplace_back(std::move(S), alias);
        } else {
            Token table = token();
            if (not expect(TK_IDENTIFIER))
                return recover<ErrorClause>(start, follow_set_FROM_CLAUSE);
            if (accept(TK_As)) {
                alias = token();
                if (not expect(TK_IDENTIFIER))
                    return recover<ErrorClause>(start, follow_set_FROM_CLAUSE);
            } else if (token().type == TK_IDENTIFIER) {
                alias = token();
                consume();
            }
            from.emplace_back(table, alias);
        }
    } while (accept(TK_COMMA));

    return std::make_unique<FromClause>(start, std::move(from));
}

std::unique_ptr<Clause> Parser::parse_WhereClause()
{
    Token start = token();

    /* 'WHERE' */
    if (not expect(TK_Where)) {
        consume();
        return recover<ErrorClause>(start, follow_set_WHERE_CLAUSE);
    }

    /* expression */
    std::unique_ptr<Expr> where = parse_Expr();

    return std::make_unique<WhereClause>(start, std::move(where));
}

std::unique_ptr<Clause> Parser::parse_GroupByClause()
{
    Token start = token();

    /* 'GROUP' 'BY' */
    if (not expect(TK_Group)) {
        consume();
        return recover<ErrorClause>(start, follow_set_GROUP_BY_CLAUSE);
    }
    if (not expect(TK_By))
        return recover<ErrorClause>(start, follow_set_GROUP_BY_CLAUSE);

    /* expr [ 'AS' identifier ] { ',' expr [ 'AS' identifier ] } */
    std::vector<GroupByClause::group_type> group_by;
    do {
        auto e = parse_Expr();
        Token tok;
        if (accept(TK_As)) {
            tok = token();
            if (not expect(TK_IDENTIFIER))
                return recover<ErrorClause>(start, follow_set_GROUP_BY_CLAUSE);
        } else if (token().type == TK_IDENTIFIER) {
            tok = token();
            consume();
        }
        group_by.emplace_back(std::move(e), tok);
    } while (accept(TK_COMMA));

    return std::make_unique<GroupByClause>(start, std::move(group_by));
}

std::unique_ptr<Clause> Parser::parse_HavingClause()
{
    Token start = token();

    /* 'HAVING' */
    if (not expect(TK_Having)) {
        consume();
        return recover<ErrorClause>(start, follow_set_HAVING_CLAUSE);
    }

    /* expression */
    std::unique_ptr<Expr> having = parse_Expr();

    return std::make_unique<HavingClause>(start, std::move(having));
}

std::unique_ptr<Clause> Parser::parse_OrderByClause()
{
    Token start = token();

    /* 'ORDER' 'BY' */
    if (not expect(TK_Order)) {
        consume();
        return recover<ErrorClause>(start, follow_set_ORDER_BY_CLAUSE);
    }
    if (not expect(TK_By))
        return recover<ErrorClause>(start, follow_set_ORDER_BY_CLAUSE);

    /* expression [ 'ASC' | 'DESC' ] { ',' expression [ 'ASC' | 'DESC' ] } */
    std::vector<OrderByClause::order_type> order_by;
    do {
        auto e = parse_Expr();
        if (accept(TK_Descending)) {
            order_by.emplace_back(std::move(e), false);
        } else {
            accept(TK_Ascending);
            order_by.emplace_back(std::move(e), true);
        }
    } while (accept(TK_COMMA));

    return std::make_unique<OrderByClause>(start, std::move(order_by));
}

std::unique_ptr<Clause> Parser::parse_LimitClause()
{
    Token start = token();

    /* 'LIMIT' integer-constant */
    if (not expect(TK_Limit)) {
        consume();
        return recover<ErrorClause>(start, follow_set_LIMIT_CLAUSE);
    }
    Token limit = token();
    if (limit.type == TK_DEC_INT or limit.type == TK_OCT_INT or limit.type == TK_HEX_INT) {
        consume();
    } else {
        diag.e(limit.pos) << "expected integer limit, got " << limit.text << '\n';
        return recover<ErrorClause>(start, follow_set_LIMIT_CLAUSE);
    }

    /* [ 'OFFSET' integer-constant ] */
    Token offset;
    if (accept(TK_Offset)) {
        offset = token();
        if (offset.type == TK_DEC_INT or offset.type == TK_OCT_INT or offset.type == TK_HEX_INT) {
            consume();
        } else {
            diag.e(offset.pos) << "expected integer offset, got " << offset.text << '\n';
            return recover<ErrorClause>(start, follow_set_LIMIT_CLAUSE);
        }
    }

    return std::make_unique<LimitClause>(start, limit, offset);
}

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

std::unique_ptr<Expr> Parser::parse_Expr(const int precedence_lhs, std::unique_ptr<Expr> lhs)
{
    /*
     * primary-expression ::= designator | constant | '(' expression ')' | '(' select-statement ')' ;
     * unary-expression ::= [ '+' | '-' | '~' ] postfix-expression ;
     * logical-not-expression ::= 'NOT' logical-not-expression | comparative-expression ;
     */
    switch (token().type) {
        /* primary-expression */
        case TK_IDENTIFIER:
            lhs = parse_designator(); // XXX For SUM(x), 'SUM' is parsed as designator; should be identifier.
            break;
        case TK_Null:
        case TK_True:
        case TK_False:
        case TK_STRING_LITERAL:
        case TK_DATE:
        case TK_DATE_TIME:
        case TK_OCT_INT:
        case TK_DEC_INT:
        case TK_HEX_INT:
        case TK_DEC_FLOAT:
        case TK_HEX_FLOAT:
            lhs = std::make_unique<Constant>(consume());
            break;
        case TK_LPAR:
            consume();
            if (token().type == TK_Select)
                lhs = std::make_unique<QueryExpr>(token(), parse_SelectStmt());
            else
                lhs = parse_Expr();
            if (not expect(TK_RPAR)) {
                recover(follow_set_PRIMARY_EXPRESSION);
            }
            break;

        /* unary-expression */
        case TK_PLUS:
        case TK_MINUS:
        case TK_TILDE: {
            auto tok = consume();
            int p = get_precedence(TK_TILDE); // the precedence of TK_TILDE equals that of unary plus and minus
            lhs = std::make_unique<UnaryExpr>(tok, parse_Expr(p));
            break;
        }

        /* logical-NOT-expression */
        case TK_Not: {
            auto tok = consume();
            int p = get_precedence(tok.type);
            lhs = std::make_unique<UnaryExpr>(tok, parse_Expr(p));
            break;
        }

        default:
            diag.e(token().pos) << "expected expression, got " << token().text << '\n';
            recover(follow_set_EXPRESSION);
            return std::make_unique<ErrorExpr>(token());
    }

    /* postfix-expression ::= postfix-expression '(' [ expression { ',' expression } ] ')' | primary-expression */
    while (token() == TK_LPAR) {
        Token lpar = consume();
        std::vector<std::unique_ptr<Expr>> args;
        if (token().type == TK_ASTERISK) {
            consume();
        } else if (token().type != TK_RPAR) {
            do
                args.push_back(parse_Expr());
            while (accept(TK_COMMA));
        }
        if (not expect(TK_RPAR)) {
            recover(follow_set_POSTFIX_EXPRESSION);
            lhs = std::make_unique<ErrorExpr>(token());
            continue;
        }
        lhs = std::make_unique<FnApplicationExpr>(lpar, std::move(lhs), std::move(args));
    }

    for (;;) {
        Token op = token();
        int p = get_precedence(op);
        if (precedence_lhs > p) return lhs; // left operator has higher precedence_lhs
        consume();

        auto rhs = parse_Expr(p + 1);
        lhs = std::make_unique<BinaryExpr>(op, std::move(lhs), std::move(rhs));
    }
}

std::unique_ptr<Expr> Parser::parse_designator()
{
    Token lhs = token();
    if (not expect(TK_IDENTIFIER)) {
        recover(follow_set_DESIGNATOR);
        return std::make_unique<ErrorExpr>(lhs);
    }
    if (token() == TK_DOT) {
        Token dot = consume();
        Token rhs = token();
        if (not expect(TK_IDENTIFIER)) {
            recover(follow_set_DESIGNATOR);
            return std::make_unique<ErrorExpr>(rhs);
        }
        return std::make_unique<Designator>(dot, lhs, rhs); // tbl.attr
    }
    return std::make_unique<Designator>(lhs); // attr
}

std::unique_ptr<Expr> Parser::expect_integer()
{
    if (is_integer(token().type)) {
        return std::make_unique<Constant>(consume());
    } else {
        diag.e(token().pos) << "expected integer constant, got " << token().text << '\n';
        return std::make_unique<ErrorExpr>(token());
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
            goto error_recovery;

        /* BOOL */
        case TK_Bool:
            consume();
            return Type::Get_Boolean(Type::TY_Scalar);

        /* 'CHAR' '(' decimal-constant ')' */
        case TK_Char:
        /* 'VARCHAR' '(' decimal-constant ')' */
        case TK_Varchar: {
            bool is_varying = token().type == TK_Varchar;
            consume();
            if (not expect(TK_LPAR)) goto error_recovery;
            Token tok = token();
            if (not expect(TK_DEC_INT)) goto error_recovery;
            if (not expect(TK_RPAR)) goto error_recovery;
            errno = 0;
            std::size_t length = strtoul(tok.text, nullptr, 10);
            if (errno) {
                diag.e(tok.pos) << tok.text << " is not a valid length\n";
                goto error_recovery;
            }
            return is_varying ? Type::Get_Varchar(Type::TY_Scalar, length) : Type::Get_Char(Type::TY_Scalar, length);
        }

        /* DATE */
        case TK_Date:
            consume();
            return Type::Get_Date(Type::TY_Scalar);

        /* DATETIME */
        case TK_Datetime:
            consume();
            return Type::Get_Datetime(Type::TY_Scalar);

        /* 'INT' '(' decimal-constant ')' */
        case TK_Int: {
            consume();
            if (not expect(TK_LPAR)) goto error_recovery;
            Token tok = token();
            if (not expect(TK_DEC_INT)) goto error_recovery;
            if (not expect(TK_RPAR)) goto error_recovery;
            errno = 0;
            std::size_t bytes = strtoul(tok.text, nullptr, 10);
            if (errno) {
                diag.e(tok.pos) << tok.text << " is not a valid size for an INT\n";
                goto error_recovery;
            }
            return Type::Get_Integer(Type::TY_Scalar, bytes);
        }

        /* 'FLOAT' */
        case TK_Float:
            consume();
            return Type::Get_Float(Type::TY_Scalar);

        /* 'DOUBLE' */
        case TK_Double:
            consume();
            return Type::Get_Double(Type::TY_Scalar);

        /* 'DECIMAL' '(' decimal-constant [ ',' decimal-constant ] ')' */
        case TK_Decimal: {
            consume();
            if (not expect(TK_LPAR)) goto error_recovery;
            Token precision = token();
            Token scale;
            if (not expect(TK_DEC_INT)) goto error_recovery;
            if (accept(TK_COMMA)) {
                scale = token();
                if (not expect(TK_DEC_INT)) goto error_recovery;
            }
            if (not expect(TK_RPAR)) goto error_recovery;
            errno = 0;
            std::size_t p = strtoul(precision.text, nullptr, 10);
            if (errno) {
                diag.e(precision.pos) << precision.text << " is not a valid precision for a DECIMAL\n";
                goto error_recovery;
            }
            errno = 0;
            std::size_t s = scale.text ? strtoul(scale.text, nullptr, 10) : 0;
            if (errno) {
                diag.e(scale.pos) << scale.text << " is not a valid scale for a DECIMAL\n";
                goto error_recovery;
            }
            return Type::Get_Decimal(Type::TY_Scalar, p, s);
        }
    }

error_recovery:
    recover(follow_set_DATA_TYPE);
    return Type::Get_Error();
}
