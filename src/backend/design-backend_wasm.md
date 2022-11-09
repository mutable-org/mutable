# WebAssembly Code Generation
**Design Document**

**Authors:** Immanuel Haffner

[[_TOC_]]


## Code Structure

- `WasmDSL.*` implements our deeply-embedded DSL
- `WasmMacro.hpp` provides macros to enable C-style DSL syntax
- `WasmUtil2.*` implements standard-library functionality
- `WasmAlgo.*` implements algorithms and abstract data types
- `WasmOperator.*` implements physical operators from `PhysicalOperator.hpp` with our Wasm DSL


## Namespaces, Classes, and Functions

All WebAssembly related code remains in namespace `m::wasm`.


### `m::wasm::PrimitiveExpr<T>`

A `PrimitiveExpr<T>` represents a *typed* Wasm *abstract syntax tree* (AST) of a compiled expression.
This expression has simple-value logic, i.e. it cannot be `NULL`.
The `PrimitiveExpr` is template-typed with a compile-time type, e.g. `PrimitiveExpr<bool>` or `PrimitiveExpr<const char*>`.
The template type allows for semantic analysis of the DSL code at compile-time.

---

It is crucial to understand that `PrimitiveExpr` represents a compiled AST and not a value.
Only the *evaluation* of the expression implemented by the AST yields a value.
The point (location in the code) where an AST is composed *is not necessarily* the point where the expression is evaluated!
This is metaprogramming.

---

`PrimitiveExpr` is not meant to be created directly, through the DSL, but is created internally by `Expr` (see below).
A `PrimitiveExpr` implements a special *move-on-copy* semantics.
This is necessary because an AST can only be used at most once in the underlying IR.
Therefore, invoking the copy c'tor of `PrimitiveExpr` actually *moves* the AST to the new instance.
Use `PrimitiveExpr::clone()` to create a deep copy.
```cpp
PrimitiveExpr<int> b = a; // AST of `a` moved to `b`
PrimitiveExpr<int> c = b.clone(); // deep copy of `b`'s AST
use(a); // illegal
use(b); // OK
```

---

To prevent generation of dead code, every `PrimitiveExpr` must be used (exactly once).
However, this would prevent calling a non-`void` function solely for its side effect: the returned `PrimitiveExpr` would remain unused and cause an error.
Therefore, `PrimitiveExpr` provides a method `PrimitiveExpr::discard()` to *explicitly* mark an unused AST as used.
The method also immediately emits the `PrimitiveExpr`s AST to ensure evaluation.
```cpp
foo_with_sideeffect().discard();
// or
auto res = foo_with_sideeffect();
res.discard();
```

---

A `PrimitiveExpr<T>` can be converted to a `PrimitiveExpr<U>`, either implicitly or explicitly.
Implicit conversion of a `PrimitiveExpr<T>` to a `PrimitiveExpr<U>` is applicable if
- `T` and `U` have same signed-ness
- neither or both `T` and `U` are integers
- `T` can be *trivially* converted to `U` (e.g. `int` to `long` but not `long` to `int`)
- `U` is not `bool`

Explicit conversion of a `PrimitiveExpr<T>` to a `PrimitiveExpr<U>` is applicable if
- `T` and `U` have same signed-ness except `T` or `U` is `bool` or `char`
- `T` can be converted to `U` (e.g. `int` to `long`, `long` to `int`, `float` to `int`)

Implicit conversion is performed implicitly (who would have thunk) and when explicitly calling the conversion operator, e.g. `static_cast<PrimitiveExpr<U>>(e)`.
Explicit conversion is accessible through method `PrimitiveExpr<T>::to<U>()`.
```cpp
PrimitiveExpr<int> a(42);
PrimitiveExpr<long> b = a; // implicit, OK
PrimitiveExpr<char> c = b; // implicit, illegal
PrimitiveExpr<char> d = b.to<char>(); // explicit, OK
```

---

`PrimitiveExpr` overloads many arithmetical and logical operators (e.g. `+`, `-`, `==`, `&&`) to enable non-verbose, human-readable creation of complex expressions.
Behind the scenes, these operations construct an AST of the compiled operations.
Operations not supported by a particular type, e.g. `PrimitiveExpr<bool>::operator+()`, are disabled via [*substitution failure is not an error* (SFINAE)](https://en.cppreference.com/w/cpp/language/sfinae).

---

`PrimitiveExpr` is also specialized for pointer types, e.g. `PrimitiveExpr<int*>`.  For pointer types, many operations are unavailable but pointer arithmetic (addition of pointer and offset, subtraction of two pointers) is supported.
Pointers can be dereferenced with `PrimitiveExpr::operator*()` and access into the referenced entity is possible with `PrimitiveExpr::operator->()`.
Pointers are necessary to reference dynamically allocated objects, that are located in *Wasm linear memory*.


### `m::wasm::Expr<T>`

`Expr<T>` extends `PrimitiveExpr<T>` by [*three-valued logic* (3VL)](https://en.wikipedia.org/wiki/Three-valued_logic): an `Expr` can evaluate to `NULL`.

To implement 3VL, `Expr<T>` composes *two* `PrimitiveExpr`:
- The first is a `PrimitiveExpr<T>` that represents the AST of the actual value.
- The second is a `PrimitiveExpr<bool>` that represents an AST that evaluates to `true` if the `Expr` is `NULL`, to `false` otherwise.
  We call this the `NULL` information.
  The `NULL` information may be absent, in which case the `Expr` is strictly `NOT NULL`.

To check whether an `Expr` can be `NULL`, use method `Expr::can_be_null()`.

---

`Expr` overloads the same operators overloaded by `PrimitiveExpr` and augments them by 3VL.
These operators, e.g. `Expr<int>::operator+()`, call to `PrimitiveExpr`s overloaded operator, e.g. `PrimitiveExpr<int>::operator+()`, to compute the value and additionally compute the `NULL` information of the result by composing the `NULL` information of both operands.
Similarly, conversion operations carry over the `NULL` information.

The exception to the rule are the logical operations `and` and `or`, that are implemented according to [Kleene's and Priest's logics](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics).
`Expr<bool>` provides additional methods that are useful in implementing conditional control flow: `Expr<bool>::is_true_and_not_null()` and `Expr<bool>::is_false_and_not_null()`.

---

Note, that any kind of evaluation of an `Expr` is a use, and every `Expr` and `PrimitiveExpr` must be used exactly once.
This means, in particular, that evaluating an `Expr`'s `NULL` information, e.g. with `Expr::is_null()`, is a use of the `Expr` and evaluating the value of the `Expr` is impossible afterwards.
```cpp
Expr<bool> e = ...;
IF (not e.is_null()) {
    use(e.value()); // error
}
```
To work around this limitation, `Expr<T>` provides method `Expr<T>::split()`, that dissects an `Expr<T>` into an `Expr<T>` and an `Expr<bool>`.
The returned `Expr<T>` holds only the value of the original `Expr<T>` without `NULL` information.
The returned `Expr<bool>` contains the `NULL` information of the original `Expr<T>` as a boolean value.
Both returned `Expr`s cannot be `NULL`.
The following example demonstrates a common use case:
```cpp
auto [val, is_null] = e.split();
IF (not is_null) {
    use(val);
}
```


### `m::wasm:Variable<T, IsGlobal>`

A `Variable` represents a *typed* runtime object in Wasm *static* storage.
A `Variable<T>` has a template type reflecting the runtime type of the `Variable`, similar to `PrimitiveExpr<T>` and `Expr<T>`.

---

`Variable`s come in two flavors: local and global `Variable`s:

##### Local: `Variable<T, false>`

A local `Variable`'s storage is located on the currently active function's stack.
Creating an instance of a local `Variable` allocates storage space on the currently active function's stack.
The location on the stack is identified by a numeric index.
<br/>
Local `Variable`s may be `NULL`.
We distinguish between strictly `NOT NULL` and *potentially* `NULL` `Variable`s.
A `Variable` that is potentially `NULL` is assigned a single runtime bit on the function's stack, that signals whether the `Variable` is `NULL` at runtime.
A `Variable` that is strictly `NOT NULL` has no such bit.
<br/>
The lifetime of a `Variable`'s storage is limited by the enclosing function.
This means, instances of `Variable`s can only be created and used within a `m::wasm::Function` definition.
This happens naturally, when `Variable`s are only created as local values (avoid  `new Variable<T, false>(...)`).

##### Global: `Variable<T, true>`

A global `Variable` has *global* storage.
Its lifetime is not limited by any function and a global `Variable` may be accessed from any function, at any time.
Global `Variable`s cannot be `NULL`.
A global `Variable` is identified by a *unique* name (in contrast to an index for local `Variable`s).

---

`Variable<T>` overloads the same operators as `Expr<T>`.
However, the overloads are more permissive in the sense that the left and right operands need only be convertible to `Expr<T>`.

---

To obtain the value of a `Variable` use method `Variable<T>::value()`, returning an instance of `Expr<T>`.
The returned `Expr<T>` holds both the value and the `NULL` information of the `Variable<T>`.
It is only necessary to invoke this method if the DSL does not implicitly convert a `Variable<T, IsGlobal>` to an `Expr<T>`.
Avoid it when possible.

---

To assign a value to a `Variable`, the class overloads assignment operators `operator=`, `operator+=`, etc.
Assigning to a `Variable` *immediately* emits IR code that *evaluates* the right-hand side and writes the value to the `Variable`'s storage.
This is one mechanism of enforcing the *immediate* evaluation of an `Expr`.
```cpp
Expr<int> e0(42);
Variable<int> v;
v = e0; // evaluates `e0`, assigns 42 to `v`
Expr<int> e1 = v + e0; // new `Expr<int>` of `v + 42`
v = 7;
print(e1); // prints 49: e1 is evaluated *after* assignment to `v`
```

### `m::wasm::Reference<T>` and `m::wasm::ConstReference<T>`

The classes `Reference<T>` and `ConstReference<T>` are helper classes to provide simpler access to dynamically allocated objects in Wasm linear memory.
Internally, they hold a `PrimitiveExpr<T*>`.
Consequently, referenced objects are strictly `NOT NULL`.

---

`Reference<T>` and `ConstReference<T>` provide implicit conversion to `Expr<T>`, thereby dereferencing the underlying `PrimitiveExpr<T*>`.

---

`Reference<T>` provides overloads of `PrimitiveExpr<T>::operator=()` to write to the referenced value.
`ConstReference<T>` does not provide any means of writing to the referenced object to enforce constness.


### `m::wasm::LocalBitmap` and `m::wasm::LocalBit`

To minimize space requirements to store `NULL` information and boolean values, we decided to implement a means of allocating individual bits on the current function's stack.
As single bits cannot directly be allocated on the Wasm stack, we decided for allocating 64-bit integers that are used as bitmaps of 64 bits.
The DSL internally tracks which bits of a bitmap are currently in use and manages allocation and deallocation of individual bits.

---

A `LocalBitmap` combines a 64-bit integer runtime value, allocated on the current function's stack, with a 64-bit integer compile-time value, that is used as a mask that tracks which bits of the runtime value are in use during code generation.
The `LocalBitmap` is not used directly but through `LocalBit` and methods to allocate/deallocate `LocalBit`s.

---

A `LocalBit` represents a single runtime bit allocated from a bitmap that is located on the current function's stack.
Therefore, a `LocalBit` references the `LocalBitmap` that the bit belongs to and stores internally the offset (in bits) within the bitmap.
`LocalBit` cannot be constructed directly but must instead be allocated via `Module::allocate_bit()`.
Destruction of a `LocalBit` returns the bit to the owning `LocalBitmap` such that it may be reused.

---

`LocalBit` provides methods to set a bit's value.
<br/>
**TODO:** `LocalBit` should provide a means to read a bit's value as `PrimitiveExpr<bool>` and potentially overload `operator=` to allow for assigning a `PrimitiveExpr<bool>` to the `LocalBit`.


### `m::wasm::Parameter<T>`

A `Parameter` is a specialization of a local `Variable`, i.e. `Variable<T, false>`, that is not allocated on the current function's stack, but its storage is provided by a function's parameter.

The operator overloadings for `Variable<T>` apply to `Parameter<T>`, too.


### `m::wasm::Module`

The `Module` is the central entity that provides all information necessary to enable *contextual* code generation.
By contextual, we mean that the DSL code has access to context information that is necessary to generate code.
For example, creating a local `Variable` requires knowing the current function.
The `Module` provides this information.

---

To make the information stored inside the `Module` available to any DSL code from anywhere in the program, the `Module` is implemented as a `thread_local` singleton.
This means, there can be at most **one** `Module` per thread at any point in time.  (However, the same thread may very well generate multiple `Module`s -- one after the other.)

---

The `Module`s thread-local instance can be accessed with `Module::Get()`.
However, many static methods are provided that call to `Get()` internally.
For example, `Module::Block()` returns the currently active block of code.
The method internally calls `Get()` to get a handle on the thread-local instance, then obtains and returns the currently active block stored within the instance.

---

Imports and exports must be declared at the `Module` level.
Functions can be imported and exported with `Module::emit_function_import()` and `Module::add_function_export()`, respectively.
Values can be imported with `Module::emit_import()`.

---

`Module` provides methods to generate control flow.
These methods are `Module::emit_return()`, `Module::emit_break()`, `Module::emit_continue()`, and `Module::emit_exception()`.


### `m::wasm::Block`

A `Block` represents a linear sequence of instructions.
`Block`s can be nested arbitrarily.
`Block`s can optionally be named.
The name is used to specify the target of a `break`, which proceeds execution immediately *after* the `Block`.

---

When a `Block` is created, it registers itself as active `Block` in the `Module`.
Code generated afterwards is added to the active `Block`.
When a `Block` is destroyed, it unregisters itself at the `Module` and sets the previously active `Block` as newly active `Block` in the `Module`.

---

Use our macro `BLOCK()` to easily construct, destroy, and nest `Block`s in natural C/C++ control flow:

```cpp
BLOCK() {
    BLOCK("hello") {
    }
    BLOCK("world") {
        BLOCK() { ... }
    }
}
```

The `BLOCK()` macro can only be used within a `FUNCTION() { }`.

The macro `BLOCK_OPEN()` allows (re-)opening a `m::wasm::Block` to generate code into that block:

```cpp
m::wasm::Block a, b, c;
BLOCK_OPEN(a) // generate code into block `a`
{
    // print "Hello, "
    BLOCK_OPEN(b) // generate code into block `b`
    {
        // print "World!"
    } // `b` is not nested inside of `a`!
}
BLOCK_OPEN(c)
{
    b.attach_to_current(); // attach block b to current block c at the current location
    a.attach_to_current(); // attach block b to current block c at the current location, i.e. after `b`
} // c prints "World!Hello, "
```

---

Note, that blocks are not the same as scopes (as, e.g., in C/C++): `Variable`s and `Expr`s defined within a `Block` may be accessed outside the `Block`.
However, this is discouraged and cannot occur when `Expr`, `Variable`, and `Block` are only created as local values (avoid `new`).
The braces `{ }` after our `BLOCK` macro are C++ scopes, thereby mimicking scope semantics in the DSL.


### `m::wasm::Function<ReturnType, ParamTypes...>`

A `Function` represents a callable Wasm function.
The function is templated on the return type and the parameter types.
A `Function` is a `Block` and creating a `Function` implicitly creates a `Block`, that is the body of the function.

---

Every `Function` has a unique name by that it can be called.

---

A `Function`'s parameters can be accessed with methods `Function<R, P...>::parameter<Idx>()` for a single parameter at index `Idx` or `Function<R, P...>::parameters()` to obtain a `std::tuple<P...>` of all parameters.

---

To return from a `Function` use method `Function<R, P...>::emit_return<T>()`.
This function uses SFINAE and is only available if the argument is implicitly convertible to type `R`.

---

A `Function` cannot be created directly but is created through `FunctionProxy`, descibed next.


### `m::wasm::FunctionProxy<ReturnType, ParamTypes...>`

A `FunctionProxy<R, P...>` is a thin proxy that can create a `Function<R, P...>` through method `FunctionProxy<R, P...>::make_function()`.
The `FunctionProxy` dissects a function definition from its references and call sites.

---

`FunctionProxy` overloads `operator()()` to implement C/C++-style calls that are compiled to Wasm function calls.
The template types of `FunctionProxy` enable us to perform static type checking of calls and implicit conversion of arguments.


```cpp
FUNCTION(foo, void(int)) // creates FunctionProxy `foo` in current scope
{                        // then creates a function named "foo" and enters the function's body
    // do something
}

FUNCTION(bar)
{
    foo(42); // call foo, OK
    foo();   // call foo, illegal
    FUNCTION(baz, int(void))
    {
        // do something else
    }
    baz();           // call baz, OK, but result unused
    baz().discard(); // call baz, OK, result used
    baz(42);         // call baz, illegal
}
```


### `m::wasm::Struct`

- `Struct::Declare<FieldTypes...>()`, `Struct::Declare<FieldTypes...>(Names...)`, `Struct::Declare(m::Type...)` as factory
- as a buffer between two operators
  - configurable in the number of tuples (finite)
  - consequently configurable in the data layout

```cpp
void WasmFilter::execute(const Match<WasmFilter> &M, callback_t Return)
{
    M.child.execute([Return=std::move(Return), &M](){
        IF (WasmCGContext::Get().env().compile(M.filter.filter())) THEN {
            Return(); // tuple-at-a-time
        };
    });
}
```

```cpp
void WasmFilter::execute(const Match<WasmFilter> &M, callback_t Return)
{
    Buffer buf(M.child.schema(), /* num_tuples= */ 16, /* layout= */ ...);

    M.child.execute([Return=std::move(Return), &M](){
        IF (WasmCGContext::Get().env().compile(M.filter.filter())) THEN {
            buf.append(...) // append current tuple to buffer
            IF (buf.full() or M.filter.child().exhausted()) THEN {
                Return(); // buffered tuple-at-a-time
            }
        };
    });
}
```

```cpp
void WasmFilter::execute(const Match<WasmFilter> &M, callback_t Return)
{
    Buffer buf(M.child.schema(), /* num_tuples= */ 16, /* layout= */ ...);

    M.child.execute([Return=std::move(Return), &M](){
        IF (WasmCGContext::Get().env().compile(M.filter.filter())) THEN {
            buf.for_each(std::move(Return)); // consume handles tuple-at-a-time processing as special case
        };
    });
}
```

```cpp
void WasmFilter::execute(const Match<WasmFilter> &M, callback_t Return)
{
    M.child.execute([Return=std::move(Return), &M](){
        IF (WasmCGContext::Get().env().compile(M.filter.filter())) THEN {
            M.consume(std::move(Return));
        };
    });
}

FilterMatch::consume(callback_t Return)
{
    buf.for_each(std::move(Return));
}

struct FilterMatch
{
    ...
    std::function<void(callback_t)> consume;
};
```


## Tasks

- [X] Implement `PrimitiveExpr<T*>::to<uint32_t>()` with implicitly adding pointer and offset (Luca)
- [X] Adapt `Variable` c'tor for `Expr<T*>` to use `PrimitiveExpr<T*>::to<uint32_t>()`
- [X] separate environment from expression compilation in `WasmEnvironment`; provide one class for each purpose
- [ ] Merge `WasmUtil2.*` and `WasmAlgo.*` and rename to `WasmLib.*`.
- [X] `WasmScan` physical operator should not implement interpretation of `Linearization`; we should move most of
  the logic into the `Linearization` class.
- [X] Provide generic support for POD structs.  This should replace `WasmStruct` and potentially `RuntimeStruct`.
- [X] `WasmStruct` should no longer inherit from `WasmExprCompiler`.
  Instead, there should be simple means to load struct fields, either into the current `WasmEnvironment` or a new, temporary `WasmEnvironment`.
  Conceptually, a struct is not responsible for compiling expressions.
- [ ] Use the more generic `Condition` class in `include/IR/Condition.hpp` to implement pre- and post-conditions of physical operators.
