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
* [x] provide parser tests
    * [x] unit tests tests
    * [x] integration tests

### Semantic Analysis

* [x] SQL types
    * boolean type `BOOL`
    * character strings `CHAR(N)` and `VARCHAR(N)`
    * numeric types
        * integers `INT(1)` to `INT(8)`
        * floating point `FLOAT` (32 bit) and `DOUBLE` (64 bit)
        * `DECIMAL(p, s)` type with *p* decimal digits and a decimal dot left of the *s* rightmost digits
* [x] define rules of the SQL type system
* [x] implement SQL type system
* [x] implement semantic analysis

### Intermediate Representation

* [x] conjunctive normal form
    * [x] implement CNF representation and operations
    * [x] CNF unit tests
* [x] design IR
* [x] implement IR
    * [x] translate expression AST to CNF
    * [ ] test IR
* [x] implement join graph representation
* [ ] rewrite rules on the join graph
    * [ ] push-down rules
    * [ ] de-correlation
    * [ ] unnesting

### Catalog

* [x] define internal type system
* [x] define and implement Schema
* [ ] define core components of a catalog
    * [x] schema
    * [x] tables
    * [ ] index structures
    * [ ] statistics
        * [ ] workload
        * [ ] data

### Store

* which kind of storage to provide
    * [ ] memory-only non-persistent storage
        * [x] in-memory row store
        * [ ] in-memory columnar store
    * [ ] disk-based persistent storage
* which data layouts to provide
    * [x] row-major layout
    * [ ] column-major layout
    * [ ] *partition across* (PAX)
* [ ] define interface for access
    * [x] by query interpreter
    * [ ] by query compiler

### Query Planner

* [x] AI Planning
    * [x] beam search
        * [x] track in `StateTracker` whether a state made it to the `beam_Q`.  if not, and it is part of a beam, add it
          to the `beam_Q` no matter its cost


### Query Interpreter



### Query Compiler



### Statistics

* [ ] gather statistics to aid query planning/compilation
    * [ ] statistics about data
    * [ ] statistics about workload

### Documentation

* document every class with at least 1-3 sentences
* document interface functions (most public's)
* document non-trivial functions/code

### Testing

* automatically run tests locally before every push (to master) (commit?)
