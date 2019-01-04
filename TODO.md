# TODOs

## Roadmap - A List of Things to be Done

### Lexer

* [x] define regular expression of tokens (as part of the grammar)
* [x] implement tokens
* [x] implement lexical analysis and tokenization
* [x] provide lexer tests
    * [x] unit tests
    * [x] integration tests

### Parser

* [x] define grammar
    * [x] expressions
    * [x] clauses
    * [x] statements
        * [x] `CREATE`
        * [x] `SELECT`
        * [x] `INSERT`
        * [x] `UPDATE`
        * [x] `DELETE`
* [x] implement *abstract syntax tree* (AST)
* [ ] provide parser tests
    * [x] unit tests tests
    * [ ] integration tests

### Semantic Analysis

* [x] SQL types
    * boolean type `BOOL`
    * character strings `CHAR(N)` and `VARCHAR(N)`
    * numeric types
        * integers `INT(1)` to `INT(8)`
        * floating point `FLOAT` (32 bit) and `DOUBLE` (64 bit)
        * `DECIMAL(p, s)` type with *p* decimal digits and a decimal dot left of the *s* rightmost digits
* [ ] define rules of the SQL type system
* [ ] implement SQL type system
* [ ] implement semantic analysis

### Catalog

* [ ] define internal type system
* [ ] define and implement Schema
* [ ] define core components of a catalog
    * [ ] schema
    * [ ] tables
    * [ ] index structures
    * [ ] statistics
        * [ ] workload
        * [ ] data

### Store

* which kind of storage to provide
    * [ ] memory-only non-persistent storage
    * [ ] disk-based persistent storage
* which data layouts to provide
    * [ ] row-major layout
    * [ ] column-major layout
    * [ ] *partition across* (PAX)
* [ ] define interface for access
    * [ ] by query interpreter
    * [ ] by query compiler

### Query Planner



### Query Interpreter



### Query Compiler



### Statistics

* [ ] gather statistics to aid query planning/compilation
    * [ ] statistics about data
    * [ ] statistics about workload
