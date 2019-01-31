# SQL Semantics

This file defines the semantics of the SQL language accepted by this system.

The semantics are defined such that potential pitfalls are prohibited by the type system and further static analysis.
On the other hand, it tries to be permissive whenever possible to provide functionality and ease of use.

##### Example

```sql
SELECT i4 + i8 FROM T; -- OK: adding INT(4) and INT(8) yields INT(8)

SELECT b + i4 FROM T; -- ERROR: addition is only allowed for numeric types

SELECT INT(b) + i4 FROM T; -- OK: explicit cast from boolean to numeric type is allowed

SELECT (CASE WHEN b THEN 1 ELSE 0 END) + i4 FROM T; -- OK: explicit interpretation of `b` as integer
```

## Types

The following tree-like structure shows all types and their relation.

```plain
Type
|- ErrorType
|- FnType
 ` PrimitiveType
   |- Boolean
   |- CharacterSequence
    ` Numeric
```

`FnType` defines a function type, e.g. the type of an instance of an aggregate function `SUM(): INT(4) -> INT(8)`

`PrimitiveType` is the base for all types of values.  An object with a type dereived from `PrimitiveType` holds a value,
e.g. a character sequence `CHAR(42)`.

#### Scalar and Vectorial

Primitive types can be either scalar or vectorial.  Objects of scalar type represent a single value, e.g. the number
`42`.  Objects of vectorial type represent a vector of values, e.g. an entire column `T.x`.

Objects of scalar type can be broadcasted to a vector by replicating their value.  Vectors cannot be converted
implicitly to scalars.  To convert a vector to a scalar value, one must explicitly use aggregate functions.


## AST

### Expressions

#### Arithmetic Operations

`~, +, -, *, /, %`

There are only unary and binary arithmetic operations.  Their operands must be of numeric type.

Unary operations preserve the type of their argument, e.g. `-x` has the same type as `x`.

Binary operations assume the type of the argument with higher precision, e.g. `13.37 + 42` yields a `DOUBLE`.
The rules for type deduction are stated below:

* Both arguments must be of numeric type.
* If both argument types are equal, the result type is equal to that of the arguments.
* If both arguments are of integer type, the result type equals that of the argument with the wider integer type.
  **Example:** `INT(4)` *op* `INT(8)` yields `INT(8)` because `8 > 4`.
* If one of the arguments is of floating point type (`FLOAT`, `DOUBLE`, or `DECIMAL`), so is the result type.
    * If one of the arguments is of type `DECIMAL`, so is the result type.  The precision and the scale, respectively,
      is the maximum of the precision and scale of the arguments.
    * If one of the arguments is of type `DOUBLE`, the result type is `DOUBLE`.
    * If one of the arguments is of type `FLOAT`, the result type is `FLOAT`, except if the precision of the other
      argument is higher than that of `FLOAT`, in which case the result type is `DOUBLE`.
