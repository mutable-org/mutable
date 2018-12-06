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
                     from-clause
                     [where-clause]
                     [group_by-clause]
                     [order_by-clause]
                     [limit-clause] ;
```

### Clauses

##### Select Clause
```ebnf
select-clause ::= "SELECT" ( '*' | expression [ "AS" identifier ] ) { ',' expression [ "AS" identifier ] } ;
```

##### From Clause
```ebnf
from-clause ::= "FROM" identifier [ "AS" identifier ] { ',' identifier [ "AS" identifier ] } ;
```

##### Where Clause
```ebnf
where-clause ::= "WHERE" expression ;
```

##### Group By Clause
```ebnf
group_by-clause ::= "GROUP" "BY" designator { ',' designator } ;
```

##### Order By Clause
```ebnf
order_by-clause ::= "ORDER" "BY" designator [ "ASC" | "DESC" ] { ',' designator [ "ASC" | "DESC" ] } ;
```

##### Limit Clause
```ebnf
limit-clause ::= "LIMIT" integer-constant [ "OFFSET" integer-constant ] ;
```

### Expressions

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

escape-sequence ::= "\\" | "\n" | "\""
```

#### Numeric Constants

##### Integer Constant
```ebnf
integer-constant ::= decimal-constant | octal-constant | hexadecimal-constant ;

decimal-constant ::= (digit - '0') { digit } ;

octal-constant ::= '0' { octal-digit } ;

hexadecimal-constant ::= "0x" hexadecimal-digit { hexadecimal-digit } ;
```

#### Unary Expressions
```ebnf
unary-expression ::= '+' expression |
                     '-' expression |
                     '!' expression |
                     '~' expression ;
```

#### Binary Expressions

#### Function Application
```ebnf
function-application ::= identifier '(' { expression } ')' ;
```
