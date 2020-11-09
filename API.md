# mu*t*able API

## `mutable` Namespace

### Classes

##### Schema

- `Type`
    - `ErrorType`, `PrimitiveType`, `FnType`, ...
- `Attribute`
- `Table`
- `Schema`
    - `Schema::Identifier`

##### AST

- `Position`
- `Token`
- `Expr` and subclasses

##### Storage

- `Store`
    - `Store::Row` (?)

##### CNF

- `cnf` namespace
- `cnf::CNF`, `cnf::Clause`, `cnf::Predicate`

##### Query Graph

- `QueryGraph`
    - `DataSource`, `BaseTable`, `Query`
    - `Join`
- `AdjacencyMatrix`

##### Optimizer

- `PlanEnumerator`
- `CostFunction`
- `PlanTable`
- `SmallBitset`

##### Plan

- `Operator`, `Consumer`, `Producer`
    - `CallbackOperator`
    - `PrintOperator`
    - `NoOpOperator`
    - `ScanOperator`
    - `FilterOperator`
    - `JoinOperator`
    - `ProjectionOperator`
    - `LimitOperator`
    - `GroupingOperator`
    - `SortingOperator`
- `OperatorData`
- `OperatorVisitor`, `ConstOperatorVisitor`

##### Backend

- `Backend`


### Functions

- `type_check()` (?)
    - only required by the `Store::Row` interface

- `cnf` operators (`operator&&`, `operator||`, `operator!`)
- `to_CNF(Expr&)`, `get_CNF(Clause&)`


## Projects Requirements

### Project 1: Implement own store

- Create subclass of `Store` with suitable fields
- Create nested subclass of `Store::Row`
- Implement c'tor and d'tor for your store (allocate `Memory` and initialize `Linearization`)
- Implement virtual methods for your store
- Implement virtual methods for your row
- Register your store in the `Catalog` and set it as default store

#### Questions

- `Linearization`: offsets relative to start of virtual address space of allocated memory?
- `Store::accept()`: visitor not needed anymore



```cpp
struct Foo
{
    void bark(int);
};

struct Bar : Foo
{
    void bark(int i) { static_cast<Foo*>(this)->bark(i); }
    void bark(float) { ... }
};



struct Foo
{
    virtual void bark(int);
};

struct Bar : Foo
{
    void bark(int i);
    void bark(float) { ... }
};
```
