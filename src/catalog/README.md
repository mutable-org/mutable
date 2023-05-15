# Catalog

[[_TOC_]]

## SpnWrapper

This is a database specific wrapper around *sum-product networks* (SPNs). An `SpnWrapper` can be seen as a *relational
sum-product network* (RSPN) but the terms SPN and RSPN are often used interchangeably. The documentation of SPNs can be
found [here](src/util/README.md). Most methods are the same with the difference that some access the attributes (random
variables) via attribute name of the table instead of attribute id. This is done via the `AttrFilter` typename.
The `AttrFilter` typename is defined as follows:
```cpp
std::unordered_map<const char*, std::pair<Spn::SpnOperator, float>>
```
The difference to `Filter` is that we map from a `const char*` representing the attribute name.

The following method can be used to learn an RSPN directly on a table:
```cpp
static SpnWrapper learn_spn_table(
    const char *name_of_database,
    const char *name_of_table,
    std::vector<Spn::LeafType> leaf_types = decltype(leaf_types)()
);
```
You can specify the `leaf_types` or leave them out for automatic detection (`DISCRETE` for integer types, `CONTINUOUS`
for floats).

The following method can be used to learn an RSPN on each table in a database:
```cpp
static std::unordered_map<const char*, SpnWrapper*> learn_spn_database(
    const char *name_of_database,
    std::unordered_map<const char*, std::vector<Spn::LeafType>> leaf_types = decltype(leaf_types)()
);
```
You can specify the `leaf_types` for each table in a map from table name to `leaf_types` vector. If a table is not in
the map, the method uses automatic detection (leave map out for automatic detection for all tables). It returns a map
from table name to the respective RSPN.
