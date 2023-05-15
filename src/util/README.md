# Util

In this directory we have useful general tools.

[[_TOC_]]

## Spn

`util/Spn.hpp` and `util/Spn.cpp` implement *sum-product networks* (SPNs). An SPN is a learned data structure that
captures the joint probability distribution $P(X_1,X_2,\dots,X_n)$ of the variables
$X_1,X_2,\dots,X_n$.

SPNs were first proposed by Hoifung Poon and Pedro Domingos [[PDF]](https://homes.cs.washington.edu/~pedrod/papers/uai11a.pdf).
Our implementation of SPNs is more close to the work
of Hilprecht et al. [[PDF]](http://www.vldb.org/pvldb/vol13/p992-hilprecht.pdf) who use SPNs in a database context for
cardinality estimation and call them *relational sum-product networks* (RSPNs). Still `util/Spn.hpp` and `util/Spn.cpp` here only represent the
general concept of *sum-product networks* while the database specific extension making them *relational* lies in `catalog/SpnWrapper.hpp` and
`catalog/SpnWrapper.cpp` (more on SpnWrapper can be found [here](src/catalog/README.md)).

### Learning an SPN

Since SPNs are a learned data structure, we first need to train our SPN on some data. For this we can use the method

```cpp
static Spn learn_spn(
    Eigen::MatrixXf &data,
    Eigen::MatrixXi &null_matrix,
    std::vector<LeafType> &leaf_types
);
```

Each column in the `data` matrix represents a random variable and the rows represent the different values of the random
variable. The `null_matrix` represents possible `NULL` values in the dataset. `LeafType` is an enum representing the
different possible kinds of variables. Possible values are:

- `DISCRETE` for random variables with discrete values, like integers
- `CONTINUOUS` for random variables with continuous domains, like floats
- `AUTO` to assign `DISCRETE` to integer domains and `CONTINUOUS` to floating point domains

The vector `leaf_types` should contain the type for each random variable in order.

### Querying an SPN

We can compute likelihoods of predicates and the expectation of attributes with SPNs with the following methods:

```cpp
float likelihood(const Filter &filter) const;
float upper_bound(const Filter &filter) const;
float lower_bound(const Filter &filter) const;
float expectation(unsigned attribute_id, const Filter &filter) const;
std::size_t estimate_number_distinct_values(unsigned attribute_id) const;
```

`Filter` is a typename to pass SQL predicates to SPNs. It is a
```cpp
std::unordered_map<unsigned, std::pair<SpnOperator, float>>
```
where unsigned is the attribute id, `SpnOperator` is the
operator of the predicate and the float is the value to be compared to. This also implies that we only support
[Sargable](https://en.wikipedia.org/wiki/Sargable) clauses as this is a general limitation of SPNs.

`likelihood` returns the likelihood of a predicate to be true.

The methods `upper_bound` and `lower_bound` are only useful on continuous domains. Continuous domains compute the
likelihood with histograms and bins. On a bin we use linear interpolation to compute the likelihood. For instance, we
have a bin on the interval ]0;1] with 30% of the values of a continuous random variable in this bin. If we want to
know the likelihood of this variable being < 0.5, we can say that it is 15%, assuming the values are evenly distributed
across the bin. In general, this works well, but the upper bound of the likelihood can be interesting for e.g.
worst case computations. With the `upper_bound` method we would get 30% instead of 15% and with `lower_bound` we would
get 0%. On `DISCRETE` domains it returns the same value as the method `likelihood`.

The method `expectation` returns the expectation of an attribute (random variable), optionally with a filter predicate
to get the conditional expectation.

The method `estimate_number_distinct_values` does as the method says, it estimates the number of distinct values of an
attribute (random variable). This method only makes sense on `DISCRETE` domains. On `CONTINUOUS` domains the method
returns the number of rows in the dataset.

### Updating an SPN

We can update an SPN model with the following methods:

```cpp
void update_row(Eigen::VectorXf &old_row, Eigen::VectorXf &updated_row);
void insert_row(Eigen::VectorXf &row);
void delete_row(Eigen::VectorXf &row);
```

Each value in the `row` vector represents a value of the respective random variable by index.

Disclaimer: Automated updates are not implemented yet. This is a future task.

