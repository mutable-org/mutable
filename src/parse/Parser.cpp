#include "parse/Parser.hpp"

#include <cerrno>
#include <cstdlib>
#include <initializer_list>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Schema.hpp>
#include <utility>


using namespace m;


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


/*======================================================================================================================
 * Follow sets
 *====================================================================================================================*/

constexpr Parser::follow_set_t make_follow_set(std::initializer_list<TokenType> tokens)
{
    Parser::follow_set_t F{{false}};
    for (TokenType tk : tokens)
        F[tk] = true;
    return F;
}

#define M_FOLLOW(NAME, SET) \
const Parser::follow_set_t follow_set_##NAME = make_follow_set SET ;
#include "tables/FollowSet.tbl"
#undef M_FOLLOW

}

Command * Parser::parse()
{
    if (token().type == TK_INSTRUCTION)
        return parse_Instruction();
    else
        return parse_Stmt();
}

Instruction * Parser::parse_Instruction()
{
    auto &C = Catalog::Get();
    Token tok = token();
    if (not expect(TK_INSTRUCTION)) {
        recover(follow_set_COMMAND);
    }

    std::string_view instruction_text(tok.text);
    const char *delimiter = " \n";

    const char *instruction_name;
    std::vector<const char*> args;

    std::string::size_type start = 0;
    std::string::size_type end = instruction_text.find_first_of(delimiter);
    instruction_name = C.pool(instruction_text.substr(start, end - start));
    start = instruction_text.find_first_not_of(delimiter, end);
    end = instruction_text.find_first_of(delimiter, start);
    while (start != std::string::npos) {
        if (end == std::string::npos) { end = instruction_text.length(); }

        args.emplace_back(C.pool(instruction_text.substr(start, end - start)));
        start = instruction_text.find_first_not_of(delimiter, end);
        end = instruction_text.find_first_of(delimiter, start);
    }

    if (not expect(TK_SEMICOL)) { recover(follow_set_COMMAND); };
    return new Instruction(tok, instruction_name, std::move(args));
}

Stmt * Parser::parse_Stmt()
{
    Stmt *stmt = nullptr;
    switch (token().type) {
        default:
            stmt = new ErrorStmt(token());
            diag.e(token().pos) << "expected a statement, got " << token().text << '\n';
            consume();
            break;

        case TK_SEMICOL: return new EmptyStmt(consume());

        case TK_Create: {
            consume();
            switch (token().type) {
                default:
                    stmt = new ErrorStmt(token());
                    diag.e(token().pos) << "expected a create database statement or a create table statement, got "
                                        << token().text << '\n';
                    recover(follow_set_CREATE_DATABASE_STATEMENT);
                    break;

                case TK_Database: stmt = parse_CreateDatabaseStmt(); break;
                case TK_Table:    stmt = parse_CreateTableStmt(); break;
            }
            break;
        }

        case TK_Use:    stmt = parse_UseDatabaseStmt(); break;
        case TK_Select: stmt = parse_SelectStmt(); break;
        case TK_Insert: stmt = parse_InsertStmt(); break;
        case TK_Update: stmt = parse_UpdateStmt(); break;
        case TK_Delete: stmt = parse_DeleteStmt(); break;
        case TK_Import: stmt = parse_ImportStmt(); break;
    }
    if (not expect(TK_SEMICOL)) recover(follow_set_STATEMENT);
    return stmt;
}

/*======================================================================================================================
 * statements
 *====================================================================================================================*/

Stmt * Parser::parse_CreateDatabaseStmt()
{
    bool ok = true;
    Token start = token();

    ok = ok and expect(TK_Database);
    Token database_name = token();
    if (ok)
        ok = expect(TK_IDENTIFIER);

    if (not ok) {
        recover(follow_set_CREATE_DATABASE_STATEMENT);
        return new ErrorStmt(start);
    }

    return new CreateDatabaseStmt(database_name);
}

Stmt * Parser::parse_UseDatabaseStmt()
{
    bool ok = true;
    Token start = token();

    ok = ok and expect(TK_Use);
    Token database_name = token();
    if (ok)
        ok = expect(TK_IDENTIFIER);

    if (not ok) {
        recover(follow_set_USE_DATABASE_STATEMENT);
        return new ErrorStmt(start);
    }

    return new UseDatabaseStmt(database_name);
}

Stmt * Parser::parse_CreateTableStmt()
{
    bool ok = true;
    Token start = token();
    std::vector<CreateTableStmt::attribute_definition*> attrs;

    /* 'TABLE' identifier '(' */
    ok = ok and expect(TK_Table);
    Token table_name = token();
    if (not ok) goto error_recovery;
    if (not expect(TK_IDENTIFIER)) goto error_recovery;
    if (not expect(TK_LPAR)) goto error_recovery;

    /* identifier data-type { constraint } [ ',' identifier data-type { constraint } ] */
    do {
        Token id = token();
        if (not expect(TK_IDENTIFIER)) goto error_recovery;
        const Type *type = parse_data_type();
        M_insist(type, "Must never be NULL");

        /* Parse the list of constraints. */
        std::vector<Constraint*> constraints;
        for (;;) {
            switch (token().type) {
                /* 'PRIMARY' 'KEY' */
                case TK_Primary: {
                    Token tok = consume();
                    if (not expect(TK_Key)) goto constraint_error_recovery;
                    constraints.push_back(new PrimaryKeyConstraint(tok));
                    break;
                }

                /* 'NOT' 'NULL' */
                case TK_Not: {
                    Token tok = consume();
                    if (not expect(TK_Null)) goto constraint_error_recovery;
                    constraints.push_back(new NotNullConstraint(tok));
                    break;
                }

                /* 'UNIQUE' */
                case TK_Unique: {
                    Token tok = consume();
                    constraints.push_back(new UniqueConstraint(tok));
                    break;
                }

                /* 'CHECK' '(' expression ')' */
                case TK_Check: {
                    Token tok = consume();
                    if (not expect(TK_LPAR)) goto constraint_error_recovery;
                    Expr *cond = parse_Expr();
                    if (not expect(TK_RPAR)) goto constraint_error_recovery;
                    constraints.push_back(new CheckConditionConstraint(tok, cond));
                    break;
                }

                /* 'REFERENCES' identifier '(' identifier ')' */
                case TK_References: {
                    Token tok = consume();
                    Token table_name = token();
                    if (not expect(TK_IDENTIFIER)) goto constraint_error_recovery;
                    if (not expect(TK_LPAR)) goto constraint_error_recovery;
                    Token attr_name = token();
                    if (not expect(TK_IDENTIFIER)) goto constraint_error_recovery;
                    if (not expect(TK_RPAR)) goto constraint_error_recovery;
                    constraints.push_back(new ReferenceConstraint(tok,
                                                                  table_name,
                                                                  attr_name,
                                                                  ReferenceConstraint::ON_DELETE_RESTRICT));
                    break;
                }

                default:
                    goto exit_constraints;
            }

        }
constraint_error_recovery:
        recover(follow_set_CONSTRAINT);
exit_constraints:
        attrs.push_back(new CreateTableStmt::attribute_definition(id, type, constraints));
    } while (accept(TK_COMMA));

    /* ')' */
    if (not expect(TK_RPAR)) goto error_recovery;

    return new CreateTableStmt(table_name, std::move(attrs));

error_recovery:
    recover(follow_set_CREATE_TABLE_STATEMENT);
    for (auto attr : attrs)
        delete attr;
    return new ErrorStmt(start);
}

Stmt * Parser::parse_SelectStmt()
{
    Clause *select = parse_SelectClause();
    Clause *from = nullptr;
    Clause *where = nullptr;
    Clause *group_by = nullptr;
    Clause *having = nullptr;
    Clause *order_by = nullptr;
    Clause *limit = nullptr;

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

    return new SelectStmt(select, from, where, group_by, having, order_by, limit);
}

Stmt * Parser::parse_InsertStmt()
{
    bool ok = true;
    Token start = token();
    std::vector<InsertStmt::tuple_t> tuples;

    /* 'INSERT' 'INTO' identifier 'VALUES' */
    ok = ok and expect(TK_Insert);
    if (ok)
        ok = expect(TK_Into);
    Token table_name = token();
    if (not ok) goto error_recovery;
    if (not expect(TK_IDENTIFIER)) goto error_recovery;
    if (not expect(TK_Values)) goto error_recovery;

    /* tuple { ',' tuple } */
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
                    tuple.emplace_back(InsertStmt::I_Expr, e);
                    break;
                }
            }
        } while (accept(TK_COMMA));
        if (not expect(TK_RPAR)) goto tuple_error_recovery;
        tuples.emplace_back(tuple);
        continue;
tuple_error_recovery:
        for (InsertStmt::element_type &e : tuple)
            delete e.second;
        recover(follow_set_TUPLE);
    } while (accept(TK_COMMA));

    return new InsertStmt(table_name, tuples);

error_recovery:
    recover(follow_set_INSERT_STATEMENT);
    for (InsertStmt::tuple_t &v : tuples) {
        for (InsertStmt::element_type &e : v)
            delete e.second;
    }
    return new ErrorStmt(start);
}

Stmt * Parser::parse_UpdateStmt()
{
    bool ok = true;
    Token start = token();
    std::vector<UpdateStmt::set_type> set;
    Clause *where = nullptr;

    /* update-clause ::= 'UPDATE' identifier 'SET' identifier '=' expression { ',' identifier '=' expression } ; */
    ok = ok and expect(TK_Update);
    Token table_name = token();
    if (not ok) goto error_recovery;
    if (not expect(TK_IDENTIFIER)) goto error_recovery;
    if (not expect(TK_Set)) goto error_recovery;

    do {
        auto id = token();
        if (not expect(TK_IDENTIFIER)) goto error_recovery;
        if (not expect(TK_EQUAL)) goto error_recovery;
        auto e = parse_Expr();
        set.emplace_back(id, e);
    } while (accept(TK_COMMA));

    if (token() == TK_Where)
        where = parse_WhereClause();

    return new UpdateStmt(table_name, set, where);

error_recovery:
    recover(follow_set_UPDATE_STATEMENT);
    for (auto &s : set)
        delete s.second;
    delete where;
    return new ErrorStmt(start);
}

Stmt * Parser::parse_DeleteStmt()
{
    bool ok = true;
    Token start = token();
    Clause *where = nullptr;

    /* delete-statement ::= 'DELETE' 'FROM' identifier [ where-clause ] ; */
    ok = ok and expect(TK_Delete);
    if (ok)
        ok = expect(TK_From);
    Token table_name = token();
    if (not ok) goto error_recovery;
    if (not expect(TK_IDENTIFIER)) goto error_recovery;

    if (token() == TK_Where)
        where = parse_WhereClause();

    return new DeleteStmt(table_name, where);

error_recovery:
    recover(follow_set_DELETE_STATEMENT);
    delete where;
    return new ErrorStmt(start);
}

Stmt * Parser::parse_ImportStmt()
{
    bool ok = true;
    Token start = token();
    ok = ok and expect(TK_Import);
    if (ok)
        ok = expect(TK_Into);
    Token table_name = token();
    if (not ok) goto error_recovery;
    if (not expect(TK_IDENTIFIER)) goto error_recovery;

    switch (token().type) {
        case TK_Dsv: {
            consume();

            DSVImportStmt stmt;

            stmt.table_name = table_name;
            stmt.path = token();
            if (not expect(TK_STRING_LITERAL)) goto error_recovery;

            /* Read the number of rows to read. */
            if (accept(TK_Rows)) {
                stmt.rows = token();
                if (token() == TK_DEC_INT or token() == TK_OCT_INT) {
                    consume();
                } else {
                    diag.e(token().pos) << "expected a decimal integer, got " << token().text << '\n';
                    goto error_recovery;
                }
            }

            /* Read the delimiter, escape character, and quote character. */
            if (accept(TK_Delimiter)) {
                stmt.delimiter = token();
                if (not expect(TK_STRING_LITERAL)) goto error_recovery;
            }
            if (accept(TK_Escape)) {
                stmt.escape = token();
                if (not expect(TK_STRING_LITERAL)) goto error_recovery;
            }
            if (accept(TK_Quote)) {
                stmt.quote = token();
                if (not expect(TK_STRING_LITERAL)) goto error_recovery;
            }

            if (accept(TK_Has)) {
                if (not expect(TK_Header)) goto error_recovery;
                stmt.has_header = true;
            }
            if (accept(TK_Skip)) {
                if (not expect(TK_Header)) goto error_recovery;
                stmt.skip_header = true;
            }

            return new DSVImportStmt(stmt);
        }

        default:
            diag.e(token().pos) << "Unrecognized input format \"" << token().text << "\".\n";
            goto error_recovery;
    }

error_recovery:
    recover(follow_set_IMPORT_STATEMENT);
    return new ErrorStmt(start);
}

/*======================================================================================================================
 * Clauses
 *====================================================================================================================*/

Clause * Parser::parse_SelectClause()
{
    Token start = token();
    Token select_all;
    std::vector<SelectClause::select_type> select;

    /* 'SELECT' */
    if (not expect(TK_Select)) goto error_recovery;

    /* ( '*' | expression [ [ 'AS' ] identifier ] ) */
    if (token() == TK_ASTERISK) {
        select_all = token();
        consume();
    } else {
        auto e = parse_Expr();
        Token tok;
        if (accept(TK_As)) {
            tok = token();
            if (not expect(TK_IDENTIFIER)) {
                delete e;
                goto error_recovery;
            }
        } else if (token().type == TK_IDENTIFIER) {
            tok = token();
            consume();
        }
        select.push_back(std::make_pair(e, tok));
    }

    /* { ',' expression [ [ 'AS' ] identifier ] } */
    while (accept(TK_COMMA)) {
        auto e = parse_Expr();
        Token tok;
        if (accept(TK_As)) {
            tok = token();
            if (not expect(TK_IDENTIFIER)) {
                delete e;
                goto error_recovery;
            }
        } else if (token().type == TK_IDENTIFIER) {
            tok = token();
            consume();
        }
        select.push_back(std::make_pair(e, tok));
    }

    return new SelectClause(start, select, select_all);

error_recovery:
    recover(follow_set_SELECT_CLAUSE);
    for (auto s : select)
        delete s.first;
    return new ErrorClause(start);
}

Clause * Parser::parse_FromClause()
{
    Token start = token();
    std::vector<FromClause::from_type> from;

    /* 'FROM' table-or-select-statement { ',' table-or-select-statement } */
    if (not expect(TK_From)) goto error_recovery;
    do {
        Token alias;
        if (accept(TK_LPAR)) {
            Stmt *S = parse_SelectStmt();
            if (not expect(TK_RPAR)) {
                delete S;
                goto error_recovery;
            }
            accept(TK_As);
            alias = token();
            if (not expect(TK_IDENTIFIER)) {
                delete S;
                goto error_recovery;
            }
            from.emplace_back(S, alias);
        } else {
            Token table = token();
            if (not expect(TK_IDENTIFIER)) goto error_recovery;
            if (accept(TK_As)) {
                alias = token();
                if (not expect(TK_IDENTIFIER)) goto error_recovery;
            } else if (token().type == TK_IDENTIFIER) {
                alias = token();
                consume();
            }
            from.emplace_back(table, alias);
        }
    } while (accept(TK_COMMA));

    return new FromClause(start, from);

error_recovery:
    recover(follow_set_FROM_CLAUSE);
    for (auto &f : from) {
        if (Stmt **stmt = std::get_if<Stmt*>(&f.source))
            delete (*stmt);
    }
    return new ErrorClause(start);
}

Clause * Parser::parse_WhereClause()
{
    Token start = token();

    /* 'WHERE' expression */
    if (not expect(TK_Where)) {
        recover(follow_set_WHERE_CLAUSE);
        return new ErrorClause(start);
    }
    Expr *where = parse_Expr();

    return new WhereClause(start, where);
}

Clause * Parser::parse_GroupByClause()
{
    Token start = token();
    std::vector<Expr*> group_by;

    /* 'GROUP' 'BY' designator { ',' designator } */
    if (not expect(TK_Group)) goto error_recovery;
    if (not expect(TK_By)) goto error_recovery;
    do
        group_by.push_back(parse_Expr());
    while (accept(TK_COMMA));
    return new GroupByClause(start, group_by);

error_recovery:
    recover(follow_set_GROUP_BY_CLAUSE);
    return new ErrorClause(start);
}

Clause * Parser::parse_HavingClause()
{
    Token start = token();

    /* 'HAVING' expression */
    if (not expect(TK_Having)) {
        recover(follow_set_HAVING_CLAUSE);
        return new ErrorClause(start);
    }
    Expr *having = parse_Expr();

    return new HavingClause(start, having);
}

Clause * Parser::parse_OrderByClause()
{
    Token start = token();
    std::vector<OrderByClause::order_type> order_by;

    /* 'ORDER' 'BY' expression [ 'ASC' | 'DESC' ] { ',' expression [ 'ASC' | 'DESC' ] } */
    if (not expect(TK_Order)) goto error_recovery;
    if (not expect(TK_By)) goto error_recovery;

    do {
        auto e = parse_Expr();
        if (accept(TK_Descending)) {
            order_by.push_back(std::make_pair(e, false));
        } else {
            accept(TK_Ascending);
            order_by.push_back(std::make_pair(e, true));
        }
    } while (accept(TK_COMMA));

    return new OrderByClause(start, order_by);

error_recovery:
    recover(follow_set_ORDER_BY_CLAUSE);
    return new ErrorClause(start);
}

Clause * Parser::parse_LimitClause()
{
    Token start = token();
    bool ok = true;

    /* 'LIMIT' integer constant */
    ok = ok and expect(TK_Limit);
    Token limit = token();
    if (ok and (limit.type == TK_DEC_INT or limit.type == TK_OCT_INT or limit.type == TK_HEX_INT)) {
        consume();
    } else if (ok) {
        diag.e(limit.pos) << "expected integer limit, got " << limit.text << '\n';
        ok = false;
    }

    /* 'OFFSET' integer constant */
    Token offset;
    if (not ok) goto error_recovery;
    if (accept(TK_Offset)) {
        offset = token();
        if (offset.type == TK_DEC_INT or offset.type == TK_OCT_INT or offset.type == TK_HEX_INT) {
            consume();
        } else {
            diag.e(offset.pos) << "expected integer offset, got " << offset.text << '\n';
            goto error_recovery;
        }
    }

    return new LimitClause(start, limit, offset);

error_recovery:
    recover(follow_set_LIMIT_CLAUSE);
    return new ErrorClause(start);
}

/*======================================================================================================================
 * Expressions
 *====================================================================================================================*/

Expr * Parser::parse_Expr(const int precedence_lhs, Expr *lhs)
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
            lhs = new Constant(consume());
            break;
        case TK_LPAR:
            consume();
            if (token().type == TK_Select)
                lhs = new QueryExpr(token(), parse_SelectStmt());
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
            recover(follow_set_EXPRESSION);
            return new ErrorExpr(token());
    }

    /* postfix-expression ::= postfix-expression '(' [ expression { ',' expression } ] ')' | primary-expression */
    while (token() == TK_LPAR) {
        Token lpar = consume();
        std::vector<Expr*> args;
        if (token().type == TK_ASTERISK) {
            consume();
        } else if (token().type != TK_RPAR) {
            do
                args.push_back(parse_Expr());
            while (accept(TK_COMMA));
        }
        if (not expect(TK_RPAR)) {
            recover(follow_set_POSTFIX_EXPRESSION);
            for (Expr* e : args)
                delete e;
            delete lhs;
            lhs = new ErrorExpr(token());
            continue;
        }
        lhs = new FnApplicationExpr(lpar, lhs, args);
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
    if (not expect(TK_IDENTIFIER)) {
        recover(follow_set_DESIGNATOR);
        return new ErrorExpr(lhs);
    }
    if (token() == TK_DOT) {
        Token dot = consume();
        Token rhs = token();
        if (not expect(TK_IDENTIFIER)) {
            recover(follow_set_DESIGNATOR);
            return new ErrorExpr(rhs);
        }
        return new Designator(dot, lhs, rhs); // tbl.attr
    }
    return new Designator(lhs); // attr
}

Expr * Parser::expect_integer()
{
    if (is_integer(token().type)) {
        return new Constant(consume());
    } else {
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
