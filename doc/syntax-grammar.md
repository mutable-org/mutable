This document defines the grammar for accepted SQL statements of the system.

The specification of the grammar is given in [extended Backus-Naur form](https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form).

The syntax of the terminal-tokens is given on the [Tokens](syntax-tokens.md) page.

🔥 Before editing the grammar below, read the disclaimer at the bottom of the page. 🔥

## Specification

### Commands
```
commands ::= command { command } ;

command ::= [ statement | INSTRUCTION ] ';' ;
```

---

### Statements
```
statement ::= create-statement |
              drop-statement |
              use_database-statement |
              select-statement |
              insert-statement |
              update-statement |
              delete-statement |
              import-statement ;
```

#### Create Statements
```
create-statement ::= create_database-statement | create_table-statement | create_index-statement ;
```

##### Create Database Statement
```
create_database-statement ::= 'CREATE' 'DATABASE' IDENTIFIER ;
```

##### Create Table Statement
```
create_table-statement ::= 'CREATE' 'TABLE' IDENTIFIER '(' IDENTIFIER data-type { constraint } { ',' IDENTIFIER data-type { constraint } } ')' ;

constraint ::= 'PRIMARY' 'KEY' |
               'NOT' 'NULL' |
               'UNIQUE' |
               'CHECK' '(' expression ')' |
               'REFERENCES' IDENTIFIER '(' IDENTIFIER ')' ;
```

##### Create Index Statement
```
create_index-statement ::= 'CREATE' [ 'UNIQUE' ] 'INDEX' [ [ 'IF' 'NOT' 'EXISTS' ] IDENTIFIER ] ON IDENTIFIER [ 'USING' ( 'DEFAULT' | IDENTIFIER ) ] '(' key_field { ',' key_field } ')' ;

key_field ::= IDENTIFIER | '(' expression ')' ;
```

#### Drop Statements
```
drop-statement ::= drop_database-statement | drop_table-statement | drop_index-statement ;
```

##### Drop Database Statement
```
drop_database-statement ::= 'DROP' 'DATABASE' [ 'IF' 'EXISTS' ] IDENTIFIER ;
```

##### Drop Table Statement
```
drop_table-statement ::= 'DROP' 'TABLE' [ 'IF' 'EXISTS' ] IDENTIFIER { ',' IDENTIFIER } ;
```

##### Drop Index Statement
```
drop_index-statement ::= 'DROP' 'INDEX' [ 'IF' 'EXISTS' ] IDENTIFIER { ',' IDENTIFIER } ;
```

#### Use Database Statement
```
use_database-statement ::= 'USE' IDENTIFIER ;
```

#### Select Statement
```
select-statement ::= select-clause
                     [ from-clause ]
                     [ where-clause ]
                     [ group_by-clause ]
                     [ having-clause ]
                     [ order_by-clause ]
                     [ limit-clause ] ;
```

#### Insert Statement
```
insert-statement ::= 'INSERT' 'INTO' IDENTIFIER 'VALUES' tuple { ',' tuple } ;

tuple ::= '(' ( 'DEFAULT' | 'NULL' | expression ) { ',' ( 'DEFAULT' | 'NULL' | expression ) } ')' ;
```

#### Update Statement
```
update-statement ::= update-clause [ where-clause ] ;
```

#### Delete Statement
```
delete-statement ::= 'DELETE' 'FROM' IDENTIFIER [ where-clause ] ;
```

#### Import Statement
```
import-statement ::= 'IMPORT' 'INTO' IDENTIFIER (
                         'DSV' STRING-LITERAL [ 'ROWS' INTEGER-CONSTANT ] [ 'DELIMITER' STRING-LITERAL ] [ 'ESCAPE' STRING-LITERAL ] [ 'QUOTE' STRING-LITERAL ] [ 'HAS' 'HEADER' ] [ 'SKIP' 'HEADER' ]
                     ) ;
```

---

### Clauses

##### Select Clause
```
select-clause ::= 'SELECT' ( '*' | expression [ [ 'AS' ] IDENTIFIER ] ) { ',' expression [ [ 'AS' ] IDENTIFIER ] } ;
```

##### From Clause
```
from-clause ::= 'FROM' table-or-select-statement { ',' table-or-select-statement } ;

table-or-select-statement ::= IDENTIFIER [ [ 'AS' ] IDENTIFIER ] | '(' select-statement ')' [ 'AS' ] IDENTIFIER ;
```

##### Where Clause
```
where-clause ::= 'WHERE' expression ;
```

##### Group By Clause
```
group_by-clause ::= 'GROUP' 'BY' expression [ 'AS' IDENTIFIER ] { ',' expression [ 'AS' IDENTIFIER ] } ;
```

##### Having Clause
```
having-clause ::= 'HAVING' expression ;
```

##### Order By Clause
```
order_by-clause ::= 'ORDER' 'BY' expression [ 'ASC' | 'DESC' ] { ',' expression [ 'ASC' | 'DESC' ] } ;
```

##### Limit Clause
```
limit-clause ::= 'LIMIT' INTEGER-CONSTANT [ 'OFFSET' INTEGER-CONSTANT ] ;
```

##### Update Clause
```
update-clause ::= 'UPDATE' IDENTIFIER 'SET' IDENTIFIER '=' expression { ',' IDENTIFIER '=' expression } ;
```

---

### Expressions

##### Designator
```
designator ::= IDENTIFIER [ '.' IDENTIFIER ] ;
```

#### Unary Expressions
```
primary-expression ::= designator | CONSTANT | '(' expression ')'  | '(' select-statement ')' ;

postfix-expression ::= postfix-expression '(' ( '*' | [ expression { ',' expression } ] ) ')' | (* function call *)
                       primary-expression ;

unary-expression ::= [ '+' | '-' | '~' ] postfix-expression ;
```

#### Binary Expressions
```
multiplicative-expression ::= multiplicative-expression '*' unary-expression |
                              multiplicative-expression '/' unary-expression |
                              multiplicative-expression '%' unary-expression |
                              unary-expression ;

additive-expression ::= additive-expression '+' multiplicative-expression |
                        additive-expression '-' multiplicative-expression |
                        multiplicative-expression ;

comparative-expression ::= additive-expression comparison-operator additive-expression |
                           additive-expression ;

comparison-operator ::= '=' | '!=' | '<' | '>' | '<=' | '>=' ;
```

##### Logical Expressions
```
logical-not-expression ::= 'NOT' logical-not-expression | comparative-expression ;

logical-and-expression ::= logical-and-expression 'AND' logical-not-expression |
                           logical-not-expression ;

logical-or-expression ::= logical-or-expression 'OR' logical-and-expression |
                          logical-and-expression ;
```

##### Expression
```
expression ::= logical-or-expression ;
```

---

### Types

##### Data Types
```
data-type ::= 'BOOL' |
              'CHAR' '(' DECIMAL-CONSTANT ')' |
              'VARCHAR' '(' DECIMAL-CONSTANT ')' |
              'DATE' |
              'DATETIME' |
              'INT' '(' DECIMAL-CONSTANT ')' |
              'FLOAT' |
              'DOUBLE' |
              'DECIMAL' '(' DECIMAL-CONSTANT [ ',' DECIMAL-CONSTANT ] ')' ;
```

## 🔥 DISCLAIMER 🔥
When editing this wiki page, make sure to keep the syntax within the code blocks intact.

The python script `resources/first_follow.py` processes the content of the code blocks on this page to generate follow sets for the parser to use during error recovery.

Any changes to the code blocks on this page require the script to be executed to update the follow sets in `src/tables/FollowSet.tbl`.

Major changes to the grammar may require adjustments in `resources/first_follow.py` and the `Parser`.
