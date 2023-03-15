This document defines the accepted token syntax. It maps the terminal tokens given on the [Grammar](syntax-grammar.md) page to the concrete syntax.

The specification of the grammar is given in [extended Backus-Naur form](https://en.wikipedia.org/wiki/Extended_Backus%E2%80%93Naur_form).

## Specification

### Constants

##### Identifier
```
CHARACTER ::= 'a' | 'b' | 'c' | 'd' | 'e' | 'f' | 'g' | 'h' | 'i' | 'j' | 'k' | 'l' | 'm' | 'n' | 'o' | 'p' | 'q' | 'r' | 's' | 't' | 'u' | 'v' | 'w' | 'x' | 'y' | 'z' |
              'A' | 'B' | 'C' | 'D' | 'E' | 'F' | 'G' | 'H' | 'I' | 'J' | 'K' | 'L' | 'M' | 'N' | 'O' | 'P' | 'Q' | 'R' | 'S' | 'T' | 'U' | 'V' | 'W' | 'X' | 'Y' | 'Z' ;

IDENTIFIER ::= ( CHARACTER | '_' ) { CHARACTER | DIGIT | '_' } ;
```

##### Digits
```
DIGIT ::= '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' ;

OCTAL-DIGIT ::= '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' ;

HEXADECIMAL-DIGIT ::= DIGIT |
                      'A' | 'B' | 'C' | 'D' | 'E' | 'F' |
                      'a' | 'b' | 'c' | 'd' | 'e' | 'f' ;

DECIMAL-DIGIT-SEQUENCE ::= DIGIT { DIGIT } ;

HEXADECIMAL-DIGIT-SEQUENCE ::= HEXADECIMAL-DIGIT { HEXADECIMAL-DIGIT } ;
```

##### String Literals
```
STRING-LITERAL ::= '"' { PRINTABLE-CHARACTER | ESCAPE-SEQUENCE } '"' ;

PRINTABLE-CHARACTER ::= ' ' | '!' | '"' | '#' | '$' | '%' | '&' | ''' | '(' | ')' | '*' | '+' | ',' | '-' | '.' | '/' | DIGIT | ':' | ';' | '<' | '=' | '>' | '?' | '@' | CHARACTER | '[' | '\' | ']' | '^' | '_' | '`' | '{' | '|' | '}' | '~' ;

ESCAPE-SEQUENCE ::= '\\' | '\n' | '\"' ;
```

##### Instructions
```
INSTRUCTION ::= '\' PRINTABLE-CHARACTER {PRINTABLE-CHARACTER} { ' ' PRINTABLE-CHARACTER {PRINTABLE-CHARACTER} } ';' ;
```

#### Numeric Constants

##### Integer Constants
```
INTEGER-CONSTANT ::= DECIMAL-CONSTANT | OCTAL-CONSTANT | HEXADECIMAL-CONSTANT ;

DECIMAL-CONSTANT ::= ( '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' ) { DIGIT } ;

OCTAL-CONSTANT ::= '0' { OCTAL-DIGIT } ;

HEXADECIMAL-CONSTANT ::= ( '0x' | '0X' ) HEXADECIMAL-DIGIT { HEXADECIMAL-DIGIT } ;
```

##### Floating-point Constants
```
FLOATINGPOINT-CONSTANT ::= DECIMAL-FLOATINGPOINT-CONSTANT | HEXADECIMAL-FLOATINGPOINT-CONSTANT ;

DECIMAL-FLOATINGPOINT-CONSTANT ::= DECIMAL-FRACTIONAL-CONSTANT [ EXPONENT ] |
                              DECIMAL-DIGIT-SEQUENCE EXPONENT ;

HEXADECIMAL-FLOATINGPOINT-CONSTANT ::= ( '0x' | '0X' ) HEXADECIMAL-FRACTIONAL-CONSTANT [ BINARY-EXPONENT ] |
                                  ( '0x' | '0X' ) HEXADECIMAL-DIGIT-SEQUENCE BINARY-EXPONENT ;

DECIMAL-FRACTIONAL-CONSTANT ::= [ DECIMAL-DIGIT-SEQUENCE ] '.' DECIMAL-DIGIT-SEQUENCE |
                                DECIMAL-DIGIT-SEQUENCE '.' ;

HEXADECIMAL-FRACTIONAL-CONSTANT ::= [ HEXADECIMAL-DIGIT-SEQUENCE ] '.' HEXADECIMAL-DIGIT-SEQUENCE |
                                    HEXADECIMAL-DIGIT-SEQUENCE '.' ;

EXPONENT ::= ( 'e' | 'E' ) [ '+' | '-' ] DECIMAL-DIGIT-SEQUENCE ;

BINARY-EXPONENT ::= ( 'p' | 'P' ) [ '+' | '-' ] DECIMAL-DIGIT-SEQUENCE ;
```

#### Boolean Constants
```
BOOLEAN-CONSTANT ::= 'TRUE' | 'FALSE' ;
```

#### Date Constants
```
DATE-CONSTANT ::= 'd\'' [ '-' ] DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT '\'' ;
```

#### Datetime Constants
```
DATETIME-CONSTANT ::= 'd\'' [ '-' ] DIGIT DIGIT DIGIT DIGIT '-' DIGIT DIGIT '-' DIGIT DIGIT ' ' DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT '\'' ;
```

#### Constant
```
CONSTANT ::= STRING-LITERAL | INTEGER-CONSTANT | FLOATINGPOINT-CONSTANT | BOOLEAN-CONSTANT | DATE-CONSTANT | DATETIME-CONSTANT ;
```
