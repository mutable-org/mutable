# SQL Syntax

This file defines the accepted SQL syntax of the system.

The specification of the language is given in [extended Backus-Naur
form](https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form).

## Specification

### Statements

##### Statement
```ebnf
statement ::= [
                select-statement |
                update-statement |
                delete-statement
              ] ';' ;
```

##### Select Statement
```ebnf
select-statement ::= select-clause
                     [where-clause]
                     [group_by-clause]
                     [order_by-clause]
                     [limit-clause] ;
```

##### Update Statement
```ebnf
update-statement ::= update-clause
                     set-clause
                     [where-clause]
                     [returning-clause] ;
```

##### Delete Statement
```ebnf
delete-statement ::= delete-clause
                     [using-clause] (* TODO: remove this? *)
                     [where-clause]
                     [returning-clause] ;
```

### Clauses

##### Select Clause
```ebnf
select-clause ::= 'SELECT' ( '*' | expression [ 'AS' identifier ] ) { ',' expression [ 'AS' identifier ] }
                  'FROM' identifier [ 'AS' identifier ] { ',' identifier [ 'AS' identifier ] } ;
```

##### Where Clause
```ebnf
where-clause ::= 'WHERE' expression ;
```

##### Group By Clause
```ebnf
group_by-clause ::= 'GROUP' 'BY' designator { ',' designator } ;
```

##### Order By Clause
```ebnf
order_by-clause ::= 'ORDER' 'BY' designator [ 'ASC' | 'DESC' ] { ',' designator [ 'ASC' | 'DESC' ] } ;
```

##### Limit Clause
```ebnf
limit-clause ::= 'LIMIT' integer-constant [ 'OFFSET' integer-constant ] ;
```

##### Update Clause
```ebnf
update-clause ::= 'UPDATE' identifier 'SET' identifier '=' expression { ',' identifier '=' expression } ;
```

##### Returning Clause
```ebnf
returning-clause ::= 'RETURNING' '*' | expression [ 'AS' identifier ] { ',' expression [ 'AS' identifier ] } ;
```

##### Delete Clause
```ebnf
delete-clause ::= 'DELETE FROM' identifier
```

##### Using Clause
```ebnf
using-clause ::= 'USING' identifier [ 'AS' identifier ] { ',' identifier [ 'AS' identifier ] } ;
```


### Expressions

##### Identifier
```ebnf
character ::= 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' |
              'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' | ;

identifier ::= ( character | '_' ) { character | digit | '_' }
```

##### Designator
```ebnf
designator ::= identifier [ '.' identifier ] ;
```

##### Digits
```ebnf
digit ::= '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' ;

octal-digit ::= '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' ;

hexadecimal-digit ::= digit | 'A' | 'B' | 'C' | 'D' | 'E' ;
```

##### String Literals
```ebnf
string-literal ::= '"' { any character except double quotes, backslash, and newline | escape-sequence } '"' ;

escape-sequence ::= '\\' | '\n' | '\"'
```

#### Numeric Constants

##### Integer Constant
```ebnf
integer-constant ::= decimal-constant | octal-constant | hexadecimal-constant ;

decimal-constant ::= (digit - '0') { digit } ;

octal-constant ::= '0' { octal-digit } ;

hexadecimal-constant ::= '0x' hexadecimal-digit { hexadecimal-digit } ;
```

#### Unary Expressions
```ebnf
primary-expression::= designator | constant | '(' expression ')' ;

postfix-expression ::= postfix-expression '(' [ expression { ',' expression } ] ')' | (* function call *)
                       primary-expression ;

unary-expression ::= [ '+' | '-' | '~' ] postfix-expression ;
```

#### Binary Expressions
```ebnf
multiplicative-expression ::= multiplicative-expression '*' unary-expression |
                              multiplicative-expression '/' unary-expression |
                              multiplicative-expression '%' unary-expression |
                              unary-expression ;

additive-expression ::= additive-expression '+' multiplicative-expression |
                        additive-expression '-' multiplicative-expression |
                        multiplicative-expression ;

comparative-expression ::= additive-expression comparison-operator additive-expression ;

comparison-operator ::= '=' | '!=' | '<' | '>' | '<=' | '>=' ;
```

##### Logical Expressions
```ebnf
logical-not-expression ::= 'NOT' logical-not-expression | comparative-expression ;

logical-and-expression ::= logical-and-expression 'AND' logical-not-expression |
                           logical-not-expression ;

logical-or-expression ::= logical-or-expression 'OR' logical-and-expression |
                          logical-and-expression ;
```
