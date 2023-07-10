#pragma once

#include <algorithm>
#include "backend/WebAssembly.hpp"
#include <concepts>
#include <cstdlib>
#include <deque>
#include <experimental/type_traits>
#include <functional>
#include <iostream>
#include <list>
#include <memory>
#include <mutable/util/concepts.hpp>
#include <mutable/util/fn.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/tag.hpp>
#include <mutable/util/type_traits.hpp>
#include <numeric>
#include <tuple>
#include <type_traits>
#include <utility>

// Binaryen
#include <wasm-binary.h>
#include <wasm-builder.h>
#include <wasm-interpreter.h>
#include <wasm-validator.h>
#include <wasm.h>


namespace m {

namespace options {

#if !defined(NDEBUG) && defined(M_ENABLE_SANITY_FIELDS)
/** Whether there must not be any ternary logic, i.e. NULL value computation.  Note that NULL values have different
 * origins, e.g. NULL values stored in a table or default aggregate values in an aggregation operator. */
extern bool insist_no_ternary_logic;

#define M_insist_no_ternary_logic() M_insist(not options::insist_no_ternary_logic, "ternary logic must not occur")

#else
#define M_insist_no_ternary_logic()

#endif

}

// forward declarations
struct Table;

namespace wasm {

/*======================================================================================================================
 * Concepts needed for forward declarations
 *====================================================================================================================*/

/** Check whether \tparam T is a primitive type and not decayable. */
template<typename T>
concept dsl_primitive = primitive<T> and not decayable<T>;

/** Check whether \tparam T is a pointer to primitive type and not decayable. */
template<typename T>
concept dsl_pointer_to_primitive = pointer_to_primitive<T> and not decayable<T>;


/*======================================================================================================================
 * Type forward declarations
 *====================================================================================================================*/

/** Declares the kind of a variable: local, parameter, or global. */
enum class VariableKind {
    Local,
    Param,
    Global,
};

struct Allocator; // for use in Module
struct Bit; // for use in PrimitiveExpr
template<std::size_t = 1> struct LocalBit; // for use in Module
struct LocalBitmap; // for use in Module
struct LocalBitvector; // for use in Module
template<typename> struct invoke_interpreter; // for unittests only
template<typename> struct invoke_v8; // for unittests only
template<typename, std::size_t = 1> struct PrimitiveExpr;
template<typename, std::size_t = 1> struct Expr;
template<typename, VariableKind, bool, std::size_t = 1> struct Variable;
template<typename, std::size_t = 1> struct Parameter;

namespace detail {

template<typename, VariableKind, bool, std::size_t> class variable_storage;

template<dsl_primitive, std::size_t, bool> struct the_reference;

}

template<typename T, std::size_t L = 1>
using Reference = detail::the_reference<T, L, false>;
template<typename T, std::size_t L = 1>
using ConstReference = detail::the_reference<T, L, true>;


/*======================================================================================================================
 * Concepts and meta types
 *====================================================================================================================*/

/** Helper type to deduce the `PrimitiveExpr<U>` type given a type \tparam T. */
template<typename T>
struct primitive_expr;

/** Specialization for decayable \tparam T. */
template<decayable T>
requires requires { typename primitive_expr<std::decay_t<T>>::type; }
struct primitive_expr<T>
{ using type = typename primitive_expr<std::decay_t<T>>::type; };

/** Specialization for primitive type \tparam T. */
template<dsl_primitive T>
struct primitive_expr<T>
{ using type = PrimitiveExpr<T, 1>; };

/** Specialization for pointer to primitive type \tparam T. */
template<dsl_pointer_to_primitive T>
struct primitive_expr<T>
{ using type = PrimitiveExpr<T, 1>; };

/** Specialization for \tparam T being `PrimitiveExpr<T, L>` already. */
template<typename T, std::size_t L>
struct primitive_expr<PrimitiveExpr<T, L>>
{ using type = PrimitiveExpr<T, L>; };

/** Specialization for \tparam T being a `Variable<T, Kind, false, L>` (i.e. a `Variable` that *cannot* be `NULL`). */
template<typename T, VariableKind Kind, std::size_t L>
struct primitive_expr<Variable<T, Kind, false, L>>
{ using type = PrimitiveExpr<T, L>; };

/** Specialization for \tparam T being a `Parameter` (i.e. a local `Variable` that *cannot* be `NULL`). */
template<typename T, std::size_t L>
struct primitive_expr<Parameter<T, L>>
{ using type = PrimitiveExpr<T, L>; };

/** Specialization for \tparam T being a `the_reference`. */
template<typename T, std::size_t L, bool IsConst>
struct primitive_expr<detail::the_reference<T, L, IsConst>>
{ using type = PrimitiveExpr<T, L>; };

/** Convenience alias for `primitive_expr`. */
template<typename T>
using primitive_expr_t = typename primitive_expr<T>::type;


/** Detects whether a type \tparam T is convertible to `PrimitiveExpr<U>`. */
template<typename T>
concept primitive_convertible = not pointer<T> and requires { typename primitive_expr_t<T>; };


/** Helper type to deduce the `Expr<U>` type given a \tparam T. */
template<typename T>
struct expr;

/** Specialization for decayable \tparam T. */
template<decayable T>
requires requires { typename expr<std::decay_t<T>>::type; }
struct expr<T>
{ using type = typename expr<std::decay_t<T>>::type; };

/** Specialization for primitive type \tparam T. */
template<dsl_primitive T>
struct expr<T>
{ using type = Expr<T, 1>; };

/** Specialization for pointer to primitive type \tparam T. */
template<dsl_pointer_to_primitive T>
struct expr<T>
{ using type = Expr<T, 1>; };

/** Specialization for \tparam T being a `PrimitiveExpr<T, L>`. */
template<typename T, std::size_t L>
struct expr<PrimitiveExpr<T, L>>
{ using type = Expr<T, L>; };

/** Specialization for \tparam T being `Expr<T, L>` already. */
template<typename T, std::size_t L>
struct expr<Expr<T, L>>
{ using type = Expr<T, L>; };

/** Specialization for \tparam T being a `Variable<T, Kind, CanBeNull, L>`. */
template<typename T, VariableKind Kind, bool CanBeNull, std::size_t L>
struct expr<Variable<T, Kind, CanBeNull, L>>
{ using type = Expr<T, L>; };

/** Specialization for \tparam T being a `Parameter` (i.e. a local `Variable` that *cannot* be `NULL`). */
template<typename T, std::size_t L>
struct expr<Parameter<T, L>>
{ using type = Expr<T, L>; };

/** Specialization for \tparam T being a `the_reference`. */
template<typename T, std::size_t L, bool IsConst>
struct expr<detail::the_reference<T, L, IsConst>>
{ using type = Expr<T, L>; };

/** Convenience alias for `expr`. */
template<typename T>
using expr_t = typename expr<T>::type;


/** Detect whether a type \tparam T is convertible to `Expr<U>`. */
template<typename T>
concept expr_convertible = not pointer<T> and requires { typename expr_t<T>; };


namespace detail {

/** Converts a compile-time type into a runtime-type `::wasm::Type`. */
template<typename, std::size_t>
struct wasm_type_helper;

/** Specialization for \tparam T being void. */
template<typename T>
requires (std::is_void_v<T>)
struct wasm_type_helper<T, 1>
{
    ::wasm::Type operator()() const { return ::wasm::Type(::wasm::Type::none); }
};

/** Specialization for \tparam T being integral. */
template<std::integral T>
struct wasm_type_helper<T, 1>
{
    ::wasm::Type operator()() const {
        /* NOTE: there are no unsigned types, only unsigned operations */
        if constexpr (sizeof(T) <= 4)
            return ::wasm::Type(::wasm::Type::i32);
        if constexpr (sizeof(T) == 8)
            return ::wasm::Type(::wasm::Type::i64);
        M_unreachable("illegal type");
    };
};

/** Specialization for \tparam T being floating point. */
template<std::floating_point T>
struct wasm_type_helper<T, 1>
{
    ::wasm::Type operator()()
    {
        if constexpr (sizeof(T) <= 4)
            return ::wasm::Type(::wasm::Type::f32);
        if constexpr (sizeof(T) == 8)
            return ::wasm::Type(::wasm::Type::f64);
        M_unreachable("illegal type");
    };
};

/** Specialization for \tparam T being pointer to primitive. */
template<dsl_pointer_to_primitive T, std::size_t L>
struct wasm_type_helper<T, L>
{
    ::wasm::Type operator()() { return ::wasm::Type(::wasm::Type::i32); };
};

/** Specialization for \tparam T being vectorial. */
template<dsl_primitive T, std::size_t L>
requires (L > 1)
struct wasm_type_helper<T, L>
{
    ::wasm::Type operator()() const { return ::wasm::Type(::wasm::Type::v128); }
};

/** Specialization for \tparam T being a `PrimitiveExpr`.  E.g. used for `Module::emit_function_import()` to enable
 * imports of functions with vectorial return or parameter types. */
template<typename T, std::size_t L>
struct wasm_type_helper<PrimitiveExpr<T, L>, 1>
{
    ::wasm::Type operator()() { return wasm_type_helper<T, L>{}(); };
};

/** Specialization for \tparam T being function with parameters. */
template<typename ReturnType, typename... ParamTypes>
struct wasm_type_helper<ReturnType(ParamTypes...), 1>
{
    ::wasm::Signature operator()()
    {
        return ::wasm::Signature(
            /* params= */ { wasm_type_helper<ParamTypes, 1>{}()... },
            /* result= */ wasm_type_helper<ReturnType, 1>{}());
    };
};

}

template<typename T, std::size_t L>
auto wasm_type() { return detail::wasm_type_helper<T, L>{}(); }


/** Helper type to deduce the signed integral type with a given byte width \tparam W. */
template<std::size_t W>
struct _int;

template<>
struct _int<1>
{ using type = int8_t; };

template<>
struct _int<2>
{ using type = int16_t; };

template<>
struct _int<4>
{ using type = int32_t; };

template<>
struct _int<8>
{ using type = int64_t; };

template<std::size_t W>
using int_t = typename _int<W>::type;

/** Helper type to deduce the unsigned integral type with a given byte width \tparam W. */
template<std::size_t W>
struct uint;

template<>
struct uint<1>
{ using type = uint8_t; };

template<>
struct uint<2>
{ using type = uint16_t; };

template<>
struct uint<4>
{ using type = uint32_t; };

template<>
struct uint<8>
{ using type = uint64_t; };

template<std::size_t W>
using uint_t = typename uint<W>::type;


/*======================================================================================================================
 * Wasm_insist(COND [, MSG])
 *
 * Similarly to `M_insist()`, checks a condition in debug build and prints location information and an optional
 * message if it evaluates to `false`.  However, the condition is checked at runtime inside the Wasm code.
 *====================================================================================================================*/

#ifndef NDEBUG

#ifdef M_ENABLE_SANITY_FIELDS
#define WASM_INSIST2_(COND, MSG) ({ \
    auto old = std::exchange(options::insist_no_ternary_logic, false); \
    m::wasm::Module::Get().emit_insist((COND), __FILE__, __LINE__, (MSG)); \
    options::insist_no_ternary_logic = old; \
})
#else
#define WASM_INSIST2_(COND, MSG) ({ \
    m::wasm::Module::Get().emit_insist((COND), __FILE__, __LINE__, (MSG)); \
})
#endif

#define WASM_INSIST1_(COND) WASM_INSIST2_((COND), nullptr)

#else
#define WASM_INSIST2_(COND, MSG) while (0) { ((void) (COND), (void) (MSG)); }
#define WASM_INSIST1_(COND) while (0) { ((void) (COND)); }

#endif

#define WASM_GET_INSIST_(XXX, _1, _2, NAME, ...) NAME
#define Wasm_insist(...) WASM_GET_INSIST_(XXX, ##__VA_ARGS__, WASM_INSIST2_, WASM_INSIST1_)(__VA_ARGS__)


/*######################################################################################################################
 * TYPE DEFINITIONS
 *####################################################################################################################*/

/*======================================================================================================================
 * Boxing types
 *====================================================================================================================*/

/** Stores the "branch targets" introduced by control flow structures, i.e. loops.
 *
 * The "break" target identifies the parent `::wasm::Block` of the loop to break out of. The "continue" target
 * identifies the `::wasm::Loop` to reiterate. */
struct branch_target_t
{
    ///> the break target
    ::wasm::Name brk;
    ///> the continue target
    ::wasm::Name continu;
    ///> the continue condition (may be `nullptr` if there is no condition)
    ::wasm::Expression *condition = nullptr;

    branch_target_t(::wasm::Name brk, ::wasm::Name continu, ::wasm::Expression *condition)
        : brk(brk), continu(continu), condition(condition)
    { }
};


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

/** A helper type to print Wasm types.  Use the `param_pack_t` helper since `template<typename..., std::size_t...>`
 * would not be allowed. */
template<param_pack Ts, std::size_t... Ls>
struct print_types;

/** Prints the Wasm type for \tparam L values of type \tparam T, recurse to print \tparam Ls values of
 * types \tparam Ts. */
template<typename T, typename... Ts, std::size_t L, std::size_t... Ls>
requires (sizeof...(Ts) == sizeof...(Ls))
struct print_types<param_pack_t<T, Ts...>, L, Ls...>
{
    friend std::ostream & operator<<(std::ostream &out, print_types) {
        return out << wasm_type<T, L>() << ", " << print_types<Ts..., Ls...>{};
    }
};

/** Prints the Wasm type for \tparam L values of type \tparam T. */
template<typename T, std::size_t L>
struct print_types<param_pack_t<T>, L>
{
    friend std::ostream & operator<<(std::ostream &out, print_types) {
        return out << wasm_type<T, L>();
    }
};

/** Creates a unique name from a given \p prefix and a \p counter.  Increments `counter`. */
inline std::string unique(std::string prefix, unsigned &counter)
{
    static thread_local std::ostringstream oss;
    oss.str("");
    oss << prefix << '<' << counter++ << '>';
    return oss.str();
}

/** Creates a `::wasm::Literal` of type \tparam T from a given \p value.  Used to solve macOS ambiguity. */
template<typename T, std::size_t L, bool = false, typename U>
requires (L == 1) and std::floating_point<T> and std::floating_point<U>
inline ::wasm::Literal make_literal(U value)
{
    return ::wasm::Literal(T(value));
}

/** Creates a `::wasm::Literal` of type \tparam T from a given \p value.  Used to solve macOS ambiguity. */
template<typename T, std::size_t L, bool = false, typename U>
requires (L == 1) and signed_integral<T> and integral<U>
inline ::wasm::Literal make_literal(U value)
{
    return sizeof(T) <= 4 ? ::wasm::Literal(int32_t(value))
                          : ::wasm::Literal(int64_t(value));
}

/** Creates a `::wasm::Literal` of type \tparam T from a given \p value.  Used to solve macOS ambiguity. */
template<typename T, std::size_t L, bool = false, typename U>
requires (L == 1) and unsigned_integral<T> and integral<U>
inline ::wasm::Literal make_literal(U value)
{
    return sizeof(T) <= 4 ? ::wasm::Literal(uint32_t(value))
                          : ::wasm::Literal(uint64_t(value));
}

/** Creates a `::wasm::Literal` of type \tparam T from a given \p value.  Used to solve macOS ambiguity. */
template<typename T, std::size_t L, bool VecRepr = false, typename U>
requires (L == 1) and boolean<T> and boolean<U>
inline ::wasm::Literal make_literal(U value)
{
    return M_CONSTEXPR_COND(VecRepr, ::wasm::Literal(0U - uint32_t(value)), ::wasm::Literal(uint32_t(value)));
}

/** Creates a `::wasm::Literal` of type \tparam T from a given \p value.  Used to solve macOS ambiguity. */
template<typename T, std::size_t L, bool = false>
requires (L == 1) and std::is_pointer_v<T>
inline ::wasm::Literal make_literal(uint32_t value)
{
    return ::wasm::Literal(uint32_t(value));
}

/** Creates a `::wasm::Literal` for \tparam L values of type \tparam T from a given \p value.  Used to solve macOS
 * ambiguity. */
template<typename T, std::size_t L, bool = false, typename U>
requires (L > 1) and (L * sizeof(T) <= 16) and
requires (U value) { make_literal<T, 1>(value); }
inline ::wasm::Literal make_literal(U value)
{
    std::array<::wasm::Literal, 16 / sizeof(T)> vec; // must be fully utilized vector
    auto it = std::fill_n(vec.begin(), L, make_literal<T, 1, true>(value)); // fill range [0, L) with value
    std::fill(it, vec.end(), make_literal<T, 1>(T(0))); // fill range [L, end) with 0
    return ::wasm::Literal(vec);
}

/** Creates a `::wasm::Literal` for \tparam L values of type \tparam T from a given \p value.  Used to solve macOS
 * ambiguity. */
template<typename T, std::size_t L, bool = false, typename U>
requires (L > 1) and (L * sizeof(T) > 16) and ((L * sizeof(T)) % 16 == 0) and
requires (U value) { make_literal<T, 1>(value); }
inline std::array<::wasm::Literal, (L * sizeof(T)) / 16> make_literal(U value)
{
    std::array<::wasm::Literal, 16 / sizeof(T)> vec;
    vec.fill(make_literal<T, 1, true>(value)); // fill single fully utilized vector with value
    ::wasm::Literal elem(vec);

    std::array<::wasm::Literal, (L * sizeof(T)) / 16> literals;
    literals.fill(elem); // fill each vector with single vectorial literal
    return literals;
}

/** Creates a `::wasm::Literal` for \tparam L values of type \tparam T from given \p values.  Used to solve macOS
 * ambiguity. */
template<typename T, std::size_t L, bool = false, typename... Us>
requires (L > 1) and (L * sizeof(T) <= 16) and (L == sizeof...(Us)) and
requires (Us... values) { (make_literal<T, 1>(values), ...); }
inline ::wasm::Literal make_literal(Us... values)
{
    std::array<::wasm::Literal, 16 / sizeof(T)> vec; // must be fully utilized vector
    auto it = vec.begin();
    ((*(it++) = make_literal<T, 1, true>(values)), ...); // fill range [0, L) with values
    std::fill(it, vec.end(), make_literal<T, 1>(T(0))); // fill range [L, end) with 0
    return ::wasm::Literal(vec);
}

/** Creates a `::wasm::Literal` for \tparam L values of type \tparam T from given \p values.  Used to solve macOS
 * ambiguity. */
template<typename T, std::size_t L, bool = false, typename... Us>
requires (L > 1) and (L * sizeof(T) > 16) and ((L * sizeof(T)) % 16 == 0) and (L == sizeof...(Us)) and
requires (Us... values) { (make_literal<T, 1>(values), ...); }
inline std::array<::wasm::Literal, (L * sizeof(T)) / 16> make_literal(Us... values)
{
    ::wasm::Literal vectors[L] = { make_literal<T, 1, true>(values)... }; // fill multiple vectors with values

    std::array<::wasm::Literal, (L * sizeof(T)) / 16> literals;
    for (std::size_t idx = 0; idx < (L * sizeof(T)) / 16; ++idx) {
        auto vec = std::to_array(*reinterpret_cast<::wasm::Literal(*)[16 / sizeof(T)]>(vectors + idx * (16 / sizeof(T))));
        literals[idx] = ::wasm::Literal(vec);
    }
    return literals;
}


/*======================================================================================================================
 * Exceptions
 *====================================================================================================================*/

#define M_EXCEPTION_LIST(X) \
    X(invalid_escape_sequence) \
    X(unreachable) \
    X(failed_unittest_check)

struct exception : backend_exception
{
#define DECLARE_ENUM(TYPE) TYPE,
    enum exception_t : uint64_t {
        M_EXCEPTION_LIST(DECLARE_ENUM)
    };
#undef DECLARE_ENUM

#define DECLARE_NAMES(TYPE) #TYPE,
    static constexpr const char * const names_[] = {
        M_EXCEPTION_LIST(DECLARE_NAMES)
    };
#undef DECLARE_NAMES

    private:
    exception_t type_;

    public:
    explicit exception(exception_t type, std::string message) : backend_exception(std::move(message)), type_(type) { }
};


/*======================================================================================================================
 * Callback functions
 *====================================================================================================================*/

/** Reports a runtime error.  The index to the filename, the line, and an optional message stored by the host is given
 * by `args`. */
::wasm::Literals insist_interpreter(::wasm::Literals &args);

/** Throws an exception.  The exception type id and the index to the filename, the line, and an optional message stored
 * by the host is given by `args`. */
::wasm::Literals throw_interpreter(::wasm::Literals &args);

const std::map<::wasm::Name, std::function<::wasm::Literals(::wasm::Literals&)>> callback_functions = {
#define CALLBACK(NAME, FUNC) { NAME, FUNC },
    CALLBACK("insist", insist_interpreter)
    CALLBACK("throw", throw_interpreter)
#undef CALLBACK
};


/*======================================================================================================================
 * GarbageCollectedData
 *====================================================================================================================*/

/** Helper struct for garbage collection done by the `Module`.  Inherit from this struct, provide a c`tor expecting
 * a `GarbageCollectedData&&` instance, and register the created struct in the module to garbage collect it
 * automatically when the module is destroyed. */
struct GarbageCollectedData
{
    friend struct Module;

    private:
    GarbageCollectedData() = default;

    public:
    GarbageCollectedData(GarbageCollectedData&&) = default;

    virtual ~GarbageCollectedData() { }
};


/*======================================================================================================================
 * ConstantFolding
 *====================================================================================================================*/

/** Helper struct to perform constant folding at compile time. */
struct ConstantFolding
{
    enum boolean_result_t {
        UNDEF,
        TRUE,
        FALSE
    };

    /** Tries to evaluate the given boolean expression \p expr using constant folding.  Returns `UNDEF` if the
     * expression cannot be evaluated at compile time, `TRUE` if the expression evaluates to `true` at compile time,
     * and `FALSE` otherwise.
     * Currently supported are only expressions consisting of a single boolean constant, a negation (tested using
     * `eqZ` instruction) of a constant boolean expression, and conjunctions or disjunctions of constant boolean
     * expressions.  Expressions like `x == x` are currently not supported. */
    static boolean_result_t EvalBoolean(const ::wasm::Expression *expr);
};


/*======================================================================================================================
 * Module
 *====================================================================================================================*/

struct Module final
{
    /*----- Friends --------------------------------------------------------------------------------------------------*/
    friend struct Block;
    friend struct BlockUser;
    template<typename> friend struct Function;
    template<typename, std::size_t> friend struct PrimitiveExpr;
    template<typename, VariableKind, bool, std::size_t> friend class detail::variable_storage;
    template<std::size_t> friend struct LocalBit;
    friend struct Allocator;
    template<typename> friend struct invoke_v8;

    private:
    ///> counter to make block names unique
    static inline std::atomic_uint NEXT_MODULE_ID_ = 0;

    ///> the unique ID for this `Module`
    unsigned id_;
    ///> counter to make block names unique
    unsigned next_block_id_ = 0;
    ///> counter to make function names unique
    unsigned next_function_id_ = 0;
    ///> counter to make global variable names unique
    unsigned next_global_id_ = 0;
    ///> counter to make if names unique
    unsigned next_if_id_ = 0;
    ///> counter to make loop names unique
    unsigned next_loop_id_ = 0;
    ///> the Binaryen Wasm module
    ::wasm::Module module_;
    ///> the Binaryen expression builder for the `module_`
    ::wasm::Builder builder_;
    ///> the currently active Binaryen block
    ::wasm::Block *active_block_ = nullptr;
    ///> the currently active Binaryen function
    ::wasm::Function *active_function_ = nullptr;
    ///> the main memory of the module
    ::wasm::Memory *memory_ = nullptr;
    ///> the allocator
    std::unique_ptr<Allocator> allocator_;
    ///> stack of Binaryen branch targets
    std::vector<branch_target_t> branch_target_stack_;
    ///> filename, line, and an optional message for each emitted insist or exception throw
    std::vector<std::tuple<const char*, unsigned, const char*>> messages_;
    ///> this module's interface, if any
    std::unique_ptr<::wasm::ModuleRunner::ExternalInterface> interface_;
    ///> the per-function stacks of local bitmaps; used for local scalar boolean variables and NULL bits
    std::vector<std::vector<LocalBitmap*>> local_bitmaps_stack_;
    ///> the per-function stacks of local bitvectors; used for local vectorial boolean variables and NULL bits
    std::vector<std::vector<LocalBitvector*>> local_bitvectors_stack_;
    ///> mapping from handles to garbage collected data
    std::unordered_map<void*, std::unique_ptr<GarbageCollectedData>> garbage_collected_data_;

    /*----- Thread-local instance ------------------------------------------------------------------------------------*/
    private:
    static thread_local std::unique_ptr<Module> the_module_;

    Module();
    Module(const Module&) = delete;

    public:
    static void Init() {
        M_insist(not the_module_, "must not have a module yet");
        the_module_ = std::unique_ptr<Module>(new Module());
    }
    static void Dispose() {
        M_insist(bool(the_module_), "must have a module");
        the_module_ = nullptr;
    }
    static Module & Get() {
        M_insist(bool(the_module_), "must have a module");
        return *the_module_;
    }

    /*----- Access methods -------------------------------------------------------------------------------------------*/
    /** Returns the ID of the current module. */
    static unsigned ID() { return Get().id_; }

    /** Returns a unique block name in the current module. */
    static std::string Unique_Block_Name(std::string prefix = "block") { return unique(prefix, Get().next_block_id_); }
    /** Returns a unique function name in the current module. */
    static std::string Unique_Function_Name(std::string prefix = "function") {
        return unique(prefix, Get().next_function_id_);
    }
    /** Returns a unique global name in the current module. */
    static std::string Unique_Global_Name(std::string prefix = "global") {
        return unique(prefix, Get().next_global_id_);
    }
    /** Returns a unique if name in the current module. */
    static std::string Unique_If_Name(std::string prefix = "if") { return unique(prefix, Get().next_if_id_); }
    /** Returns a unique loop name in the current module. */
    static std::string Unique_Loop_Name(std::string prefix = "loop") { return unique(prefix, Get().next_loop_id_); }

    /** Returns the expression builder of the current module. */
    static ::wasm::Builder & Builder() { return Get().builder_; }

    /** Returns the currently active block. */
    static ::wasm::Block & Block() { return *M_notnull(Get().active_block_); }

    /** Returns the currently active function. */
    static ::wasm::Function & Function() { return *M_notnull(Get().active_function_); }

    /** Returns the allocator. */
    static Allocator & Allocator();

    /** Validates that the module is well-formed. */
    static bool Validate(bool verbose = true, bool global = true);

    /** Optimizes the module with the optimization level set to `level`. */
    static void Optimize(int optimization_level);

    /** Sets the new active `::wasm::Block` and returns the previously active `::wasm::Block`. */
    ::wasm::Block * set_active_block(::wasm::Block *block) { return std::exchange(active_block_, block); }
    /** Sets the new active `::wasm::Function` and returns the previously active `::wasm::Function`. */
    ::wasm::Function * set_active_function(::wasm::Function *fn) { return std::exchange(active_function_, fn); }

    /*----- Control flow ---------------------------------------------------------------------------------------------*/
    /** An unsafe, i.e. statically-**un**typed, version of `Function::emit_return()`. */
    void emit_return();
    /** An unsafe, i.e. statically-**un**typed, version of `Function::emit_return(T&&)`. */
    template<typename T, std::size_t L>
    void emit_return(PrimitiveExpr<T, L> expr);
    /** An unsafe, i.e. statically-**un**typed, version of `Function::emit_return(T&&)`. */
    template<typename T, std::size_t L>
    void emit_return(Expr<T, L> expr);

    void emit_break(std::size_t level = 1);
    void emit_break(PrimitiveExpr<bool, 1> cond, std::size_t level = 1);

    void emit_continue(std::size_t level = 1);
    void emit_continue(PrimitiveExpr<bool, 1> cond, std::size_t level = 1);

    template<typename T, std::size_t L>
    PrimitiveExpr<T, L> emit_select(PrimitiveExpr<bool, 1> cond, PrimitiveExpr<T, L> tru, PrimitiveExpr<T, L> fals);
    template<typename T, std::size_t L>
    Expr<T, L> emit_select(PrimitiveExpr<bool, 1> cond, Expr<T, L> tru, Expr<T, L> fals);
    template<typename T, std::size_t L>
    requires (L > 1) and requires (PrimitiveExpr<int8_t, L> e) { e.template to<int_t<sizeof(T)>, L>(); }
    PrimitiveExpr<T, L> emit_select(PrimitiveExpr<bool, L> cond, PrimitiveExpr<T, L> tru, PrimitiveExpr<T, L> fals);
    template<typename T, std::size_t L>
    requires (L > 1) and requires (PrimitiveExpr<int8_t, L> e) { e.template to<int_t<sizeof(T)>, L>(); }
    Expr<T, L> emit_select(PrimitiveExpr<bool, L> cond, Expr<T, L> tru, Expr<T, L> fals);

    /*----- Shuffle --------------------------------------------------------------------------------------------------*/
    /** Selects lanes of \p first and \p second in byte granularity depending on the indices specified by \p indices.
     * Indices `i` in the range [0, L * sizeof(T)) select the `i`-th lane of `first`, indices `i` in the
     * range [L * sizeof(T), 2 * L * sizeof(T)) select the `i-(L * sizeof(T))`-th lane of `second`, indices outside of
     * these ranges result in undefined values. */
    template<typename T, std::size_t L, std::size_t M>
    requires (L > 1) and (L * sizeof(T) <= 16) and (M > 0) and (M <= 16) and (M % sizeof(T) == 0)
    PrimitiveExpr<T, M / sizeof(T)> emit_shuffle_bytes(PrimitiveExpr<T, L> first, PrimitiveExpr<T, L> second,
                                                       const std::array<uint8_t, M> &indices);
    /** Selects lanes of \p first and \p second in lane granularity depending on the indices specified by \p indices.
     * Indices `i` in the range [0, L) select the `i`-th lane of `first`, indices `i` in the range [L, 2 * L) select
     * the `i-L`-th lane of `second`, indices outside of these ranges result in undefined values. */
    template<typename T, std::size_t L, std::size_t M>
    requires (L > 1) and (L * sizeof(T) <= 16) and (M > 0) and (is_pow_2(M)) and (M * sizeof(T) <= 16)
    PrimitiveExpr<T, M> emit_shuffle_lanes(PrimitiveExpr<T, L> first, PrimitiveExpr<T, L> second,
                                           const std::array<uint8_t, M> &indices);
    /** Selects lanes of \p first and \p second in byte granularity depending on the indices specified by \p indices.
     * Indices `i` in the range [0, L * sizeof(T)) select the `i`-th lane of `first`, indices `i` in the
     * range [L * sizeof(T), 2 * L * sizeof(T)) select the `i-(L * sizeof(T))`-th lane of `second`, indices outside of
     * these ranges result in undefined values. */
    template<typename T, std::size_t L, std::size_t M>
    requires (L > 1) and (L * sizeof(T) <= 16) and (M > 0) and (M <= 16) and (M % sizeof(T) == 0) and (sizeof(T) == 1)
    Expr<T, M / sizeof(T)> emit_shuffle_bytes(Expr<T, L> first, Expr<T, L> second, const std::array<uint8_t, M> &indices);
    /** Selects lanes of \p first and \p second in lane granularity depending on the indices specified by \p indices.
     * Indices `i` in the range [0, L) select the `i`-th lane of `first`, indices `i` in the range [L, 2 * L) select
     * the `i-L`-th lane of `second`, indices outside of these ranges result in undefined values. */
    template<typename T, std::size_t L, std::size_t M>
    requires (L > 1) and (L * sizeof(T) <= 16) and (M > 0) and (is_pow_2(M)) and (M * sizeof(T) <= 16)
    Expr<T, M> emit_shuffle_lanes(Expr<T, L> first, Expr<T, L> second, const std::array<uint8_t, M> &indices);

    /*----- Globals. -------------------------------------------------------------------------------------------------*/
    template<dsl_primitive T, std::size_t L = 1, dsl_primitive... Us>
    requires (L * sizeof(T) <= 16) and requires (Us... us) { make_literal<T, L>(us...); }
    void emit_global(::wasm::Name name, bool is_mutable, Us... inits) {
        ::wasm::Builder::Mutability mut = is_mutable ? ::wasm::Builder::Mutability::Mutable
                                                     : ::wasm::Builder::Mutability::Immutable;
        ::wasm::Const *_init = builder_.makeConst(make_literal<T, L>(inits...));
        auto global = builder_.makeGlobal(name, wasm_type<T, L>(), _init, mut);
        module_.addGlobal(std::move(global));
    }
    template<dsl_primitive T, std::size_t L = 1, dsl_primitive... Us>
    requires (L * sizeof(T) <= 16) and requires (Us... us) { make_literal<T, L>(us...); }
    void emit_global(const std::array<::wasm::Name, 1> &names, bool is_mutable, Us... inits) {
        emit_global<T, L>(names[0], is_mutable, inits...);
    }
    template<dsl_primitive T, std::size_t L = 1, std::size_t N, dsl_primitive... Us>
    requires requires (Us... us) { { make_literal<T, L>(us...) } -> std::same_as<std::array<::wasm::Literal, N>>; }
    void emit_global(const std::array<::wasm::Name, N> &names, bool is_mutable, Us... inits) {
        ::wasm::Builder::Mutability mut = is_mutable ? ::wasm::Builder::Mutability::Mutable
                                                     : ::wasm::Builder::Mutability::Immutable;
        auto literals = make_literal<T, L>(inits...);
        for (std::size_t idx = 0; idx < N; ++idx) {
            ::wasm::Const *_init = builder_.makeConst(literals[idx]);
            auto global = builder_.makeGlobal(names[idx], wasm_type<T, L>(), _init, mut);
            module_.addGlobal(std::move(global));
        }
    }
    template<dsl_pointer_to_primitive T, std::size_t L = 1>
    void emit_global(::wasm::Name name, bool is_mutable, uint32_t init) {
        ::wasm::Builder::Mutability mut = is_mutable ? ::wasm::Builder::Mutability::Mutable
                                                     : ::wasm::Builder::Mutability::Immutable;
        ::wasm::Const *_init = builder_.makeConst(::wasm::Literal(init));
        auto global = builder_.makeGlobal(name, wasm_type<T, L>(), _init, mut);
        module_.addGlobal(std::move(global));
    }

    template<typename T, std::size_t L = 1>
    requires (L * sizeof(T) <= 16)
    PrimitiveExpr<T, L> get_global(const char *name);

    /*----- Imports & Exports ----------------------------------------------------------------------------------------*/
    template<typename T, std::size_t L = 1>
    requires (dsl_primitive<T> or dsl_pointer_to_primitive<T>) and
    requires { M_CONSTEXPR_COND_UNCAPTURED(std::is_pointer_v<T>, (make_literal<T, L>(0)), (make_literal<T, L>(T()))); }
    void emit_import(const char *extern_name, const char *intern_name = nullptr)
    {
        ::wasm::Const *value = M_CONSTEXPR_COND(std::is_pointer_v<T>, builder_.makeConst(make_literal<T, L>(0)),
                                                                      builder_.makeConst(make_literal<T, L>(T())));
        auto global = builder_.makeGlobal(intern_name ? intern_name : extern_name, wasm_type<T, L>(), M_notnull(value),
                                          ::wasm::Builder::Mutability::Immutable);
        global->module = "imports";
        global->base = extern_name;
        module_.addGlobal(std::move(global));
    }

    /** Add function `name` with type `T` as import. */
    template<typename T>
    requires std::is_function_v<T> and requires { wasm_type<T, 1>(); }
    void emit_function_import(const char *name) {
        auto func = module_.addFunction(builder_.makeFunction(name, wasm_type<T, 1>(), {}));
        func->module = "imports";
        func->base = name;
    }

    /** Add function `name` as export. */
    void emit_function_export(const char *name) {
        module_.addExport(builder_.makeExport(name, name, ::wasm::ExternalKind::Function));
    }

    /*----- Function calls -------------------------------------------------------------------------------------------*/
    template<typename ReturnType, typename... ParamTypes, std::size_t... ParamLs>
    requires std::is_void_v<ReturnType>
    void emit_call(const char *fn, PrimitiveExpr<ParamTypes, ParamLs>... args);

    template<typename ReturnType, std::size_t ReturnL = 1, typename... ParamTypes, std::size_t... ParamLs>
    requires dsl_primitive<ReturnType> or dsl_pointer_to_primitive<ReturnType>
    PrimitiveExpr<ReturnType, ReturnL> emit_call(const char *fn, PrimitiveExpr<ParamTypes, ParamLs>... args);

    /*----- Runtime checks and throwing exceptions -------------------------------------------------------------------*/
    template<std::size_t L>
    void emit_insist(PrimitiveExpr<bool, L> cond, const char *filename, unsigned line, const char *msg);
    template<>
    void emit_insist<1>(PrimitiveExpr<bool, 1> cond, const char *filename, unsigned line, const char *msg);

    void emit_throw(exception::exception_t type, const char *filename, unsigned line, const char *msg);

    const std::tuple<const char*, unsigned, const char*> & get_message(std::size_t idx) const {
        return messages_.at(idx);
    }

    /*----- Garbage collected data -----------------------------------------------------------------------------------*/
    /** Adds and returns an instance of \tparam C, which will be created by calling its c`tor with an
     * `GarbageCollectedData&&` instance and the forwarded \p args, to `this` `Module`s garbage collection using the
     * unique caller handle \p handle. */
    template<class C, typename... Args>
    C & add_garbage_collected_data(void *handle, Args... args) {
        auto it = garbage_collected_data_.template try_emplace(
            /* key=   */ handle,
            /* value= */ std::make_unique<C>(GarbageCollectedData(), std::forward<Args>(args)...)
        ).first;
        return as<C>(*it->second);
    }

    /*----- Interpretation & Debugging -------------------------------------------------------------------------------*/
    ::wasm::ModuleRunner::ExternalInterface * get_mock_interface();

    /** Create an instance of this module.  Can be used for interpretation and debugging. */
    ::wasm::ModuleRunner instantiate() { return ::wasm::ModuleRunner(module_, get_mock_interface()); }

    /*----- Module settings ------------------------------------------------------------------------------------------*/
    void set_feature(::wasm::FeatureSet feature, bool value) { module_.features.set(feature, value); }

    /** Returns the binary representation of `module_` in a freshly allocated memory.  The caller must dispose of this
     * memory. */
    std::pair<uint8_t*, std::size_t> binary();

    private:
    void create_local_bitmap_stack();
    void create_local_bitvector_stack();
    void dispose_local_bitmap_stack();
    void dispose_local_bitvector_stack();
    public:
    template<std::size_t L = 1>
    requires (L > 0) and (L <= 16)
    LocalBit<L> allocate_bit();

    void push_branch_targets(::wasm::Name brk, ::wasm::Name continu) {
        branch_target_stack_.emplace_back(brk, continu, nullptr);
    }

    void push_branch_targets(::wasm::Name brk, ::wasm::Name continu, PrimitiveExpr<bool, 1> condition);

    branch_target_t pop_branch_targets() {
        auto top = branch_target_stack_.back();
        branch_target_stack_.pop_back();
        return top;
    }

    const branch_target_t & current_branch_targets() const { return branch_target_stack_.back(); }

    /*----- Printing -------------------------------------------------------------------------------------------------*/
    public:
    friend std::ostream & operator<<(std::ostream &out, const Module &M) {
        out << "Module\n";

        out << "  currently active block: ";
        if (M.active_block_) {
            if (M.active_block_->name.is())
                out << '"' << M.active_block_->name << '"';
            else
                out << "<anonymous block>";
        } else {
            out << "none";
        }
        out << '\n';

        // out << "  currently active function: ";
        // if (M.active_function_) {
        //     out << '"' << M.active_function_->name << '"';
        // } else {
        //     out << "none";
        // }
        // out << '\n';

        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }

    void dump_all(std::ostream &out) { out << module_ << std::endl; }
    void dump_all() { dump_all(std::cerr); }
};


/*======================================================================================================================
 * Block
 *====================================================================================================================*/

/** Represents a code block, i.e. a sequential sequence of code.  Necessary to compose conditional control flow and
 * useful for simultaneous code generation at several locations.  */
struct Block final
{
    template<typename T> friend struct Function; // to get ::wasm::Block of body
    friend struct BlockUser; // to access `Block` internals
    friend struct If; // to get ::wasm::Block for *then* and *else* part
    friend struct Loop; // to get ::wasm::Block for loop body
    friend struct DoWhile; // to get ::wasm::Block for loop body

    private:
    ///> this block, can be `nullptr` if default-constructed or the block has already been attached
    ::wasm::Block *this_block_ = nullptr;
    ///> the parent block, before this block was created
    ::wasm::Block *parent_block_ = nullptr;
    ///> whether this block attaches itself to its parent block
    bool attach_to_parent_ = false;

    public:
    friend void swap(Block &first, Block &second) {
        using std::swap;
        swap(first.this_block_,       second.this_block_);
        swap(first.parent_block_,     second.parent_block_);
        swap(first.attach_to_parent_, second.attach_to_parent_);
    }

    private:
    Block() = default;

    /** Create a new `Block` for a given `::wasm::Block`. */
    Block(::wasm::Block *block, bool attach_to_parent)
        : this_block_(M_notnull(block))
        , attach_to_parent_(attach_to_parent)
    {
        if (attach_to_parent_) {
            parent_block_ = Module::Get().active_block_;
            M_insist(not attach_to_parent_ or parent_block_, "can only attach to parent if there is a parent block");
        }
    }

    public:
    /** Create an anonymous `Block`. */
    explicit Block(bool attach_to_parent) : Block(Module::Builder().makeBlock(), attach_to_parent) { }
    /** Create a named `Block` and set it *active* in the current `Module`. */
    explicit Block(std::string name, bool attach_to_parent)
        : Block(Module::Builder().makeBlock(Module::Unique_Block_Name(name)), attach_to_parent)
    { }
    /** Create a named `Block` and set it *active* in the current `Module`. */
    explicit Block(const char *name, bool attach_to_parent) : Block(std::string(name), attach_to_parent) { }

    Block(const Block&) = delete;
    Block(Block &&other) : Block() { swap(*this, other); }

    ~Block() {
        if (this_block_ and attach_to_parent_)
            attach_to(*M_notnull(parent_block_));
    }

    Block & operator=(Block &&other) { swap(*this, other); return *this; }

    private:
    ::wasm::Block & get() const { return *M_notnull(this_block_); }
    ::wasm::Block & previous() const { return *M_notnull(parent_block_); }

    void attach_to(::wasm::Block &other) {
        other.list.push_back(this_block_);
        this_block_ = nullptr;
    }

    public:
    bool has_name() const { return bool(get().name); }
    std::string name() const { M_insist(has_name()); return get().name.toString(); }

    /** Returns whether this `Block` is empty, i.e. contains to expressions. */
    bool empty() const { return get().list.empty(); }

    /** Attaches this `Block` to the given `Block` \p other. */
    void attach_to(Block &other) {
        M_insist(not attach_to_parent_, "cannot explicitly attach if attach_to_parent is true");
        attach_to(*M_notnull(other.this_block_));
    }

    /** Attaches this `Block` to the `wasm::Block` currently active in the `Module`. */
    void attach_to_current() {
        M_insist(not attach_to_parent_, "cannot explicitly attach if attach_to_parent is true");
        attach_to(Module::Block());
    }

    /** Emits a jump to the end of this `Block`. */
    void go_to() const { Module::Block().list.push_back(Module::Builder().makeBreak(get().name)); }
    /** Emits a jump to the end of this `Block` iff `cond` is fulfilled. */
    void go_to(PrimitiveExpr<bool, 1> cond) const;

    friend std::ostream & operator<<(std::ostream &out, const Block &B) {
        out << "vvvvvvvvvv block";
        if (B.has_name())
            out << " \"" << B.name() << '"';
        out << " starts here vvvvvvvvvv\n";

        for (auto expr : B.get().list)
            out << *expr << '\n';

        out << "^^^^^^^^^^^ block";
        if (B.has_name())
            out << " \"" << B.name() << '"';
        out << " ends here ^^^^^^^^^^^\n";

        return out;
    }

    void dump(std::ostream &out) const { out << *this; out.flush(); }
    void dump() const { dump(std::cerr); }
};

/** A helper class to *use* a `Block`, thereby setting the `Block` active for code generation.  When the `BlockUser` is
 * destructed, restores the previously active block for code generation. */
struct BlockUser
{
    private:
    const Block &block_; ///< the block to use (for code gen)
    ::wasm::Block *old_block_ = nullptr; ///< the previously active, now old block

    public:
    BlockUser(const Block &block) : block_(block) {
        old_block_ = Module::Get().set_active_block(block_.this_block_); // set active block
    }

    ~BlockUser() { Module::Get().set_active_block(old_block_); } // restore previously active block
};


/*======================================================================================================================
 * Function
 *====================================================================================================================*/

/** Represents a Wasm function.  It is templated with return type and parameter types.  This enables us to access
 * parameters with their proper types.  */
template<typename>
struct Function;

template<typename ReturnType, typename... ParamTypes, std::size_t ReturnL, std::size_t... ParamLs>
requires ((std::is_void_v<ReturnType> and (ReturnL == 1)) or
           requires { typename PrimitiveExpr<ReturnType, ReturnL>; }) and
         (not dsl_primitive<ReturnType> or (ReturnL * sizeof(ReturnType) <= 16)) and       // must fit in single register
         (requires { typename PrimitiveExpr<ParamTypes, ParamLs>; } and ...) and
         ((not dsl_primitive<ParamTypes> or (ParamLs * sizeof(ParamTypes) <= 16)) and ...) // must fit in single register
 struct Function<PrimitiveExpr<ReturnType, ReturnL>(PrimitiveExpr<ParamTypes, ParamLs>...)>
{
    template<typename> friend struct FunctionProxy;

    ///> the type of the function
    using type = ReturnType(ParamTypes...);
    ///> the DSL type of the function
    using dsl_type = PrimitiveExpr<ReturnType, ReturnL>(PrimitiveExpr<ParamTypes, ParamLs>...);
    ///> the return type of the function
    using return_type = ReturnType;
    ///> the amount of parameters of the function
    static constexpr std::size_t PARAMETER_COUNT = sizeof...(ParamTypes);

    /*------------------------------------------------------------------------------------------------------------------
     * Parameter helper types
     *----------------------------------------------------------------------------------------------------------------*/
    private:
    template<typename... Ts, std::size_t... Ls, std::size_t... Is>
    requires (sizeof...(Ts) == sizeof...(Ls)) and (sizeof...(Ts) == sizeof...(Is))
    std::tuple<Parameter<Ts, Ls>...> make_parameters_helper(std::index_sequence<Is...>) {
        return std::make_tuple<Parameter<Ts, Ls>...>(
            (Parameter<Ts, Ls>(Is), ...)
        );
    }

    /** Creates a `std::tuple` with statically typed fields, one per parameter. */
    template<typename... Ts, std::size_t... Ls, typename Indices = std::make_index_sequence<sizeof...(Ts)>>
    requires (sizeof...(Ts) == sizeof...(Ls))
    std::tuple<Parameter<Ts, Ls>...> make_parameters() { return make_parameters_helper<Ts..., Ls...>(Indices{}); }

    /** Provides an alias `type` with the type of the \tparam I -th parameter. */
    template<std::size_t I, typename... Ts>
    struct parameter_type;
    template<std::size_t I, typename T, typename... Ts>
    struct parameter_type<I, T, Ts...>
    {
        static_assert(I <= sizeof...(Ts), "parameter index out of range");
        using type = typename parameter_type<I - 1, Ts...>::type;
    };
    template<typename T, typename... Ts>
    struct parameter_type<0, T, Ts...>
    {
        using type = T;
    };
    /** Convenience alias for `parameter_type::type`. */
    template<std::size_t I>
    using parameter_type_t = typename parameter_type<I, ParamTypes...>::type;

    /** Provides a value `value` with the number of SIMD lanes of the \tparam I -th parameter. */
    template<std::size_t I, std::size_t... Ls>
    struct parameter_num_simd_lanes;
    template<std::size_t I, std::size_t L, std::size_t... Ls>
    struct parameter_num_simd_lanes<I, L, Ls...>
    {
        static_assert(I <= sizeof...(Ls), "parameter index out of range");
        static constexpr std::size_t value = parameter_num_simd_lanes<I - 1, Ls...>::value;
    };
    template<std::size_t L, std::size_t... Ls>
    struct parameter_num_simd_lanes<0, L, Ls...>
    {
        static constexpr std::size_t value = L;
    };
    /** Convenience alias for `parameter_num_simd_lanes::value`. */
    template<std::size_t I>
    static constexpr std::size_t parameter_num_simd_lanes_v = parameter_num_simd_lanes<I, ParamLs...>::value;

    private:
    ::wasm::Name name_; ///< the *unique* name of this function
    Block body_; ///< the function body
    ///> the `::wasm::Function` implementing this function
    ::wasm::Function *this_function_ = nullptr;
    ///> the previously active `::wasm::Function` (may be `nullptr`)
    ::wasm::Function *previous_function_ = nullptr;

    public:
    friend void swap(Function &first, Function &second) {
        using std::swap;
        swap(first.name_,              second.name_);
        swap(first.body_,              second.body_);
        swap(first.this_function_,     second.this_function_);
        swap(first.previous_function_, second.previous_function_);
    }

    private:
    Function() = default;

    public:
    /** Constructs a fresh `Function` and expects a unique \p name.  To be called by `FunctionProxy`. */
    Function(const std::string &name)
        : name_(name)
        , body_(name + ".body", /* attach_to_parent= */ false)
    {
        /*----- Set block return type for non-`void` functions. -----*/
        if constexpr (not std::is_void_v<ReturnType>)
            body_.get().type = wasm_type<ReturnType, ReturnL>();

        /*----- Create Binaryen function. -----*/
        auto fn = Module::Builder().makeFunction(
            /* name= */ name,
            /* type= */ wasm_type<dsl_type, 1>(),
            /* vars= */ std::vector<::wasm::Type>{}
        );
        fn->body = &body_.get(); // set function body
        this_function_ = Module::Get().module_.addFunction(std::move(fn));
        M_insist(this_function_->getNumParams() == PARAMETER_COUNT);
        Module::Get().create_local_bitmap_stack();
        Module::Get().create_local_bitvector_stack();

        /*----- Set this function active in the `Module`. -----*/
        previous_function_ = Module::Get().set_active_function(this_function_);
    }

    Function(const Function&) = delete;
    Function(Function &&other) : Function() { swap(*this, other); }

    ~Function() {
        if constexpr (not std::is_void_v<ReturnType>)
            body_.get().list.push_back(Module::Builder().makeUnreachable());
        Module::Get().dispose_local_bitmap_stack();
        Module::Get().dispose_local_bitvector_stack();
        /*----- Restore previously active function in the `Module`. -----*/
        Module::Get().set_active_function(previous_function_);
    }

    Function & operator=(Function &&other) { swap(*this, other); return *this; }

    public:
    /** Returns the body of this function. */
    Block & body() { return body_; }
    /** Returns the body of this function. */
    const Block & body() const { return body_; }

    /** Returns the name of this function. */
    std::string name() const { return name_.toString(); }

    /** Returns all parameters of this function. */
    std::tuple<Parameter<ParamTypes, ParamLs>...> parameters() { return make_parameters<ParamTypes..., ParamLs...>(); }

    /** Returns the \tparam I -th parameter, statically typed via `parameter_type_t`. */
    template<std::size_t I>
    Parameter<parameter_type_t<I>, parameter_num_simd_lanes_v<I>> parameter() {
        return Parameter<parameter_type_t<I>, parameter_num_simd_lanes_v<I>>(I);
    }

    /** Emits a return instruction returning `void`. */
    void emit_return() requires std::is_void_v<ReturnType> {
        Module::Block().list.push_back(Module::Builder().makeReturn());
    }

    /** Emits a return instruction returning `PrimitiveExpr` constructed from \p t of type \tparam T. */
    template<primitive_convertible T>
    requires (not std::is_void_v<ReturnType>) and
    requires (T &&t) { PrimitiveExpr<ReturnType, ReturnL>(primitive_expr_t<T>(std::forward<T>(t))); }
    void emit_return(T &&t) {
        PrimitiveExpr<ReturnType, ReturnL> value(primitive_expr_t<T>(std::forward<T>(t)));
        Module::Get().emit_return(value);
    }

    /** Emits a return instruction returning `PrimitiveExpr` constructed from \p t of type \tparam T. Checks that
     * \p t is `NOT NULL`. */
    template<expr_convertible T>
    requires (not std::is_void_v<ReturnType>) and (not primitive_convertible<T>) and
    requires (T t) { Expr<ReturnType, ReturnL>(expr_t<T>(std::forward<T>(t))); }
    void emit_return(T &&t)
    {
        Expr<ReturnType, ReturnL> expr(expr_t<T>(std::forward<T>(t)));
        Module::Get().emit_return(expr);
    }

    private:
    ::wasm::Function & get() const { return *M_notnull(this_function_); }

    public:
    friend std::ostream & operator<<(std::ostream &out, const Function &Fn) {
        out << "function \"" << Fn.name() << "\" : ";
        if constexpr (PARAMETER_COUNT)
            out << print_types<param_pack_t<ParamTypes...>, ParamLs...>{};
        else
            out << typeid(void).name();
        out << " -> " << print_types<param_pack_t<ReturnType>, ReturnL>{} << '\n';

        if (not Fn.get().vars.empty()) {
            out << "  " << Fn.get().getNumVars() << " local variables:";
            for (::wasm::Index i = 0, end = Fn.get().getNumVars(); i != end; ++i)
                out << " [" << i << "] " << Fn.get().vars[i];
            out << '\n';
        }

        out << Fn.body();
        return out;
    }

    void dump(std::ostream &out) const { out << *this; out.flush(); }
    void dump() const { dump(std::cerr); }
};

template<typename ReturnType, typename... ParamTypes>
requires (std::is_void_v<ReturnType> or dsl_primitive<ReturnType> or dsl_pointer_to_primitive<ReturnType>) and
         ((dsl_primitive<ParamTypes> or dsl_pointer_to_primitive<ParamTypes>) and ...)
struct Function<ReturnType(ParamTypes...)> : Function<PrimitiveExpr<ReturnType, 1>(PrimitiveExpr<ParamTypes, 1>...)>
{
    using Function<PrimitiveExpr<ReturnType, 1>(PrimitiveExpr<ParamTypes, 1>...)>::Function;
};

template<typename... ParamTypes, std::size_t... ParamLs>
struct Function<void(PrimitiveExpr<ParamTypes, ParamLs>...)>
    : Function<PrimitiveExpr<void, 1>(PrimitiveExpr<ParamTypes, ParamLs>...)>
{
    using Function<PrimitiveExpr<void, 1>(PrimitiveExpr<ParamTypes, ParamLs>...)>::Function;
};


/*======================================================================================================================
 * FunctionProxy
 *====================================================================================================================*/

/** A handle to create a `Function` and to create invocations of that function. Provides `operator()()` to emit a
 * function call by issuing a C-style call.  The class is template typed with the function signature, allowing us to
 * perform static type checking of arguments and the returned value at call sites. */
template<typename>
struct FunctionProxy;

template<typename ReturnType, typename... ParamTypes, std::size_t ReturnL, std::size_t... ParamLs>
requires ((std::is_void_v<ReturnType> and (ReturnL == 1)) or
           requires { typename PrimitiveExpr<ReturnType, ReturnL>; }) and
         (not dsl_primitive<ReturnType> or (ReturnL * sizeof(ReturnType) <= 16)) and       // must fit in single register
         (requires { typename PrimitiveExpr<ParamTypes, ParamLs>; } and ...) and
         ((not dsl_primitive<ParamTypes> or (ParamLs * sizeof(ParamTypes) <= 16)) and ...) // must fit in single register
struct FunctionProxy<PrimitiveExpr<ReturnType, ReturnL>(PrimitiveExpr<ParamTypes, ParamLs>...)>
{
    using type = ReturnType(ParamTypes...);
    using dsl_type = PrimitiveExpr<ReturnType, ReturnL>(PrimitiveExpr<ParamTypes, ParamLs>...);

    private:
    std::string name_; ///< the unique name of the `Function`

    public:
    FunctionProxy() = delete;
    FunctionProxy(std::string name) : name_(Module::Unique_Function_Name(name)) { }
    FunctionProxy(const char *name) : FunctionProxy(std::string(name)) { }

    FunctionProxy(FunctionProxy&&) = default;

    FunctionProxy & operator=(FunctionProxy&&) = default;

    const std::string & name() const { return name_; }
    const char * c_name() const { return name_.c_str(); }

    Function<dsl_type> make_function() const { return Function<dsl_type>(name_); }

    /*----- Overload operator() to emit function calls ---------------------------------------------------------------*/
    /** Call function returning `void` with parameters \p args of types \tparam Args. */
    template<typename... Args>
    requires std::is_void_v<ReturnType> and
    requires (Args&&... args) { (PrimitiveExpr<ParamTypes, ParamLs>(std::forward<Args>(args)), ...); }
    void operator()(Args&&... args) const {
        operator()(PrimitiveExpr<ParamTypes, ParamLs>(std::forward<Args>(args))...);
    }

    /** Call function returning `void` with parameters \p args of `PrimitiveExpr` type. */
    void operator()(PrimitiveExpr<ParamTypes, ParamLs>... args) const requires std::is_void_v<ReturnType> {
        Module::Block().list.push_back(
            Module::Builder().makeCall(name_, { args.expr()... }, wasm_type<ReturnType, ReturnL>())
        );
    }

    /** Call function returning non-`void` with parameters \p args of types \tparam Args. */
    template<typename... Args>
    requires (not std::is_void_v<ReturnType>) and
    requires (Args&&... args) { (PrimitiveExpr<ParamTypes, ParamLs>(std::forward<Args>(args)), ...); }
    PrimitiveExpr<ReturnType, ReturnL> operator()(Args&&... args) const {
        return operator()(PrimitiveExpr<ParamTypes, ParamLs>(std::forward<Args>(args))...);
    }

    /** Call function returning non-`void` with parameters \p args of `PrimitiveExpr` type. */
    PrimitiveExpr<ReturnType, ReturnL>
    operator()(PrimitiveExpr<ParamTypes, ParamLs>... args) const requires (not std::is_void_v<ReturnType>) {
        return PrimitiveExpr<ReturnType, ReturnL>(
            Module::Builder().makeCall(name_, { args.expr()... }, wasm_type<ReturnType, ReturnL>())
        );
    }
};

template<typename ReturnType, typename... ParamTypes>
requires (std::is_void_v<ReturnType> or dsl_primitive<ReturnType> or dsl_pointer_to_primitive<ReturnType>) and
         ((dsl_primitive<ParamTypes> or dsl_pointer_to_primitive<ParamTypes>) and ...)
struct FunctionProxy<ReturnType(ParamTypes...)>
    : FunctionProxy<PrimitiveExpr<ReturnType, 1>(PrimitiveExpr<ParamTypes, 1>...)>
{
    using FunctionProxy<PrimitiveExpr<ReturnType, 1>(PrimitiveExpr<ParamTypes, 1>...)>::FunctionProxy;
};

template<typename... ParamTypes, std::size_t... ParamLs>
struct FunctionProxy<void(PrimitiveExpr<ParamTypes, ParamLs>...)>
    : FunctionProxy<PrimitiveExpr<void, 1>(PrimitiveExpr<ParamTypes, ParamLs>...)>
{
    using FunctionProxy<PrimitiveExpr<void, 1>(PrimitiveExpr<ParamTypes, ParamLs>...)>::FunctionProxy;
};


/*======================================================================================================================
 * PrimitiveExpr
 *====================================================================================================================*/

template<typename T, typename U, std::size_t L>
concept arithmetically_combinable = dsl_primitive<T> and dsl_primitive<U> and have_common_type<T, U> and
requires (PrimitiveExpr<T, L> e) { PrimitiveExpr<common_type_t<T, U>, L>(e); } and
requires (PrimitiveExpr<U, L> e) { PrimitiveExpr<common_type_t<T, U>, L>(e); };

/** Specialization of `PrimitiveExpr<T, L>` for primitive type \tparam T and either scalar values or vectorial ones
 * fitting in a single SIMD vector.  Represents an expression (AST) evaluating to \tparam L runtime values of
 * primitive type \tparam T. */
template<dsl_primitive T, std::size_t L>
requires (L > 0) and (is_pow_2(L)) and (L * sizeof(T) <= 16) // since Wasm currently only supports 128 bit SIMD vectors
struct PrimitiveExpr<T, L>
{
    ///> the primitive type of the represented expression
    using type = T;
    ///> the number of SIMD lanes of the represented expression, i.e. 1 for scalar and at least 2 for vectorial ones
    static constexpr std::size_t num_simd_lanes = L;

    /*----- Friends --------------------------------------------------------------------------------------------------*/
    template<typename, std::size_t> friend struct PrimitiveExpr; // to convert U to T and U* to uint32_t
    template<typename, std::size_t>
    friend struct Expr; // to construct an empty `PrimitiveExpr<bool>` for the NULL information
    template<typename, VariableKind, bool, std::size_t>
    friend class detail::variable_storage; // to construct from `::wasm::Expression` and access private `expr()`
    friend struct Module; // to access internal `::wasm::Expression`, e.g. in `emit_return()`
    friend struct Block; // to access internal `::wasm::Expression`, e.g. in `go_to()`
    template<typename> friend struct FunctionProxy; // to access internal `::wasm::Expr` to construct function calls
    template<std::size_t> friend struct LocalBit; // to access private `move()`
    friend struct If; // to use PrimitiveExpr<bool> as condition
    friend struct While; // to use PrimitiveExpr<bool> as condition
    template<typename> friend struct invoke_interpreter; // to access private `expr()`
    template<typename, std::size_t> friend struct std::array; // to be able to default construct vectors array

    private:
    ///> the referenced Binaryen expression (AST)
    ::wasm::Expression *expr_ = nullptr;
    ///> a list of referenced `Bit`s
    std::list<std::shared_ptr<Bit>> referenced_bits_;

    private:
    ///> Constructs an empty `PrimitiveExpr`, for which `operator bool()` returns `false`.
    explicit PrimitiveExpr() = default;

    ///> Constructs a `PrimitiveExpr` from a Binaryen `::wasm::Expression` \p expr and the \p referenced_bits.
    explicit PrimitiveExpr(::wasm::Expression *expr, std::list<std::shared_ptr<Bit>> referenced_bits = {})
        : expr_(expr)
        , referenced_bits_(std::move(referenced_bits))
    { }
    /** Constructs a `PrimitiveExpr` from a `std::pair` of a Binaryen `::wasm::Expression` \p expr and the \p
     * referenced_bits. */
    explicit PrimitiveExpr(std::pair<::wasm::Expression*, std::list<std::shared_ptr<Bit>>> expr)
        : PrimitiveExpr(std::move(expr.first), std::move(expr.second))
    { }

    /** Constructs a `PrimitiveExpr` from a byte array \p bytes. */
    explicit PrimitiveExpr(const std::array<uint8_t, 16> &bytes)
    requires (L > 1)
        : PrimitiveExpr(Module::Builder().makeConst(::wasm::Literal(bytes.data())))
    { }

    /** Constructs a `PrimitiveExpr` from a vector array \p vectors containing a single `PrimitiveExpr`. */
    explicit PrimitiveExpr(std::array<PrimitiveExpr, 1> vectors)
    requires (L > 1)
        : PrimitiveExpr(vectors[0])
    { }

    public:
    /** Constructs a new `PrimitiveExpr` from a constant \p value. */
    template<dsl_primitive... Us>
    requires (sizeof...(Us) > 0) and requires (Us... us) { make_literal<T, L>(us...); }
    explicit PrimitiveExpr(Us... value)
        : PrimitiveExpr(Module::Builder().makeConst(make_literal<T, L>(value...)))
    { }

    /** Constructs a new `PrimitiveExpr` from a decayable constant \p value. */
    template<decayable... Us>
    requires (sizeof...(Us) > 0) and (dsl_primitive<std::decay_t<Us>> and ...) and
    requires (Us... us) { PrimitiveExpr(std::decay_t<Us>(us)...); }
    explicit PrimitiveExpr(Us... value)
        : PrimitiveExpr(std::decay_t<Us>(value)...)
    { }

    PrimitiveExpr(const PrimitiveExpr&) = delete;
    /** Constructs a new `PrimitiveExpr` by **moving** the underlying `expr_` and `referenced_bits_` of `other`
     * to `this`. */
    PrimitiveExpr(PrimitiveExpr &other)
        : PrimitiveExpr(std::exchange(other.expr_, nullptr), std::move(other.referenced_bits_))
    { /* move, not copy */ }
    /** Constructs a new `PrimitiveExpr` by **moving** the underlying `expr_` and `referenced_bits_` of `other`
     * to `this`. */
    PrimitiveExpr(PrimitiveExpr &&other)
        : PrimitiveExpr(std::exchange(other.expr_, nullptr), std::move(other.referenced_bits_))
    { }

    private:
    /** Move assigns `this` to `other`.  Only necessary to assign vectors array for double pumping. XXX: use vector instead of array? */
    PrimitiveExpr & operator=(PrimitiveExpr &&other) {
        using std::swap;
        swap(this->expr_, other.expr_);
        swap(this->referenced_bits_, other.referenced_bits_);
        return *this;
    }

    public:
    ~PrimitiveExpr() { M_insist(not expr_, "expression must be used or explicitly discarded"); }

    private:
    /** **Moves** the underlying Binaryen `::wasm::Expression` out of `this`. */
    ::wasm::Expression * expr() {
        M_insist(expr_, "cannot access an already moved or discarded expression of a `PrimitiveExpr`");
        return std::exchange(expr_, nullptr);
    }
    /** **Moves** the referenced bits out of `this`. */
    std::list<std::shared_ptr<Bit>> referenced_bits() { return std::move(referenced_bits_); }
    /** **Moves** the underlying Binaryen `::wasm::Expression` and the referenced bits out of `this`. */
    std::pair<::wasm::Expression*, std::list<std::shared_ptr<Bit>>> move() {
        return { expr(), referenced_bits() };
    }

    public:
    /** Returns `true` if this `PrimitiveExpr` actually holds a value (Binaryen AST), `false` otherwise. Can be used to
     * test whether this `PrimitiveExpr` has already been used. */
    explicit operator bool() const { return expr_ != nullptr; }

    /** Creates and returns a *deep copy* of `this`. */
    PrimitiveExpr clone() const {
        M_insist(expr_, "cannot clone an already moved or discarded `PrimitiveExpr`");
        return PrimitiveExpr(
            /* expr=            */ ::wasm::ExpressionManipulator::copy(expr_, Module::Get().module_),
            /* referenced_bits= */ referenced_bits_ // copy
        );
    }

    /** Discards `this`.  This is necessary to signal in our DSL that a value is *expectedly* unused (and not dead
     * code). For example, the return value of a function that was invoked because of its side effects may remain
     * unused.  One **must** discard the returned value to signal that the value is expectedly left unused. */
    void discard() {
        M_insist(expr_, "cannot discard an already moved or discarded `PrimitiveExpr`");
        if (expr_->is<::wasm::Call>())
            Module::Block().list.push_back(Module::Builder().makeDrop(expr_)); // keep the function call
#ifndef NDEBUG
        expr_ = nullptr;
#endif
        referenced_bits_.clear();
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Operation helper
     *----------------------------------------------------------------------------------------------------------------*/

    private:
    /** Helper function to implement *unary* operations.  Applies `::wasm::UnaryOp` \p op to `this` and returns the
     * result. */
    template<dsl_primitive ResultType, std::size_t ResultL>
    PrimitiveExpr<ResultType, ResultL> unary(::wasm::UnaryOp op) {
        return PrimitiveExpr<ResultType, ResultL>(
            /* expr=            */ Module::Builder().makeUnary(op, expr()),
            /* referenced_bits= */ referenced_bits() // moved
        );
    }

    /** Helper function to implement *binary* operations.  Applies `::wasm::BinaryOp` \p op to `this` and \p other and
     * returns the result.  Note, that we require `this` and \p other to be of same type. */
    template<dsl_primitive ResultType, std::size_t ResultL, dsl_primitive OperandType, std::size_t OperandL>
    PrimitiveExpr<ResultType, ResultL> binary(::wasm::BinaryOp op, PrimitiveExpr<OperandType, OperandL> other) {
        auto referenced_bits = this->referenced_bits(); // moved
        referenced_bits.splice(referenced_bits.end(), other.referenced_bits());
        return PrimitiveExpr<ResultType, ResultL>(
            /* expr=            */ Module::Builder().makeBinary(op,
                                                                this->template to<OperandType, OperandL>().expr(),
                                                                other.expr()),
            /* referenced_bits= */ std::move(referenced_bits)
        );
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Conversion operations
     *----------------------------------------------------------------------------------------------------------------*/

    private:
    template<dsl_primitive U, std::size_t M>
    PrimitiveExpr<U, M> convert() {
        using From = T;
        using To = U;
        constexpr std::size_t FromL = L;
        constexpr std::size_t ToL = M;

        if constexpr (std::same_as<From, To> and FromL == ToL)
            return *this;
        if constexpr (integral<From> and integral<To> and std::is_signed_v<From> == std::is_signed_v<To> and
                      sizeof(From) == sizeof(To) and FromL == ToL)
            return PrimitiveExpr<To, ToL>(move());

        if constexpr (boolean<From>) {                                                                  // from boolean
            if constexpr (integral<To>) {                                                               //  to integer
                if constexpr (FromL == 1 and ToL == 1) {                                                //   scalar
                    if constexpr (sizeof(To) <= 4)                                                      //    bool -> i32
                        return PrimitiveExpr<To, ToL>(move());
                    if constexpr (sizeof(To) == 8)                                                      //    bool -> i64
                        return unary<To, ToL>(::wasm::ExtendUInt32);
                }
                if constexpr (FromL > 1 and FromL == ToL) {                                             //   vectorial
                    if constexpr (sizeof(To) == 1)                                                      //    bool -> i8/u8
                        return -PrimitiveExpr<To, ToL>(move()); // negate to convert 0xff to 1
                    if constexpr (std::is_signed_v<To>) {
                        if constexpr (sizeof(To) == 2)                                                  //    bool -> i16
                            return to<int8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(To) == 4)                                                  //    bool -> i32
                            return to<int8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(To) == 8)                                                  //    bool -> i64
                            return to<int8_t, ToL>().template to<To, ToL>();
                    } else {
                        if constexpr (sizeof(To) == 2)                                                  //    bool -> u16
                            return to<uint8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(To) == 4)                                                  //    bool -> u32
                            return to<uint8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(To) == 8)                                                  //    bool -> u64
                            return to<uint8_t, ToL>().template to<To, ToL>();
                    }
                }
            }
            if constexpr (std::floating_point<To>) {                                                    //  to floating point
                if constexpr (FromL == 1 and ToL == 1) {                                                //   scalar
                    if constexpr (sizeof(To) == 4)                                                      //    bool -> f32
                        return unary<To, ToL>(::wasm::ConvertUInt32ToFloat32);
                    if constexpr (sizeof(To) == 8)                                                      //    bool -> f64
                        return unary<To, ToL>(::wasm::ConvertUInt32ToFloat64);
                }
                if constexpr (FromL > 1 and FromL == ToL) {                                             //   vectorial
                    if constexpr (sizeof(To) == 4)                                                      //    bool -> f32
                        return to<uint32_t, ToL>().template convert<To, ToL>();
                    if constexpr (sizeof(To) == 8)                                                      //    bool -> f64
                        return to<uint32_t, ToL>().template convert<To, ToL>();
                }
            }
        }

        if constexpr (boolean<To>)                                                                      // to boolean
            return *this != PrimitiveExpr(static_cast<From>(0));

        if constexpr (integral<From>) {                                                                 // from integer
            if constexpr (integral<To>) {                                                               //  to integer
                if constexpr (FromL == 1 and ToL == 1) {                                                //   scalar
                    if constexpr (std::is_signed_v<From>) {                                             //    signed
                        if constexpr (sizeof(From) <= 4 and sizeof(To) == 8)                            //     i32 -> i64
                            return unary<To, ToL>(::wasm::ExtendSInt32);
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                            //     i64 -> i32
                            return unary<To, ToL>(::wasm::WrapInt64);
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) <= 4 and sizeof(To) == 8)                            //     u32 -> u64
                            return unary<To, ToL>(::wasm::ExtendUInt32);
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                            //     u64 -> u32
                            return unary<To, ToL>(::wasm::WrapInt64);
                    }
                    if constexpr (sizeof(To) <= 4 and sizeof(From) < sizeof(To))                        //    From less precise than To
                        return PrimitiveExpr<To, ToL>(move());                                          //     extend integer
                    if constexpr (sizeof(From) <= 4 and sizeof(To) < sizeof(From)) {                    //    To less precise than From
                        constexpr From MASK = (uint64_t(1) << (8 * sizeof(To))) - uint64_t(1);
                        return PrimitiveExpr<To, ToL>((*this bitand PrimitiveExpr(MASK)).move());       //     truncate integer
                    }
                    if constexpr (sizeof(From) == 8 and sizeof(To) < 4) {                               //    To less precise than From
                        if constexpr (std::is_signed_v<To>) {
                            auto wrapped = unary<int32_t, ToL>(::wasm::WrapInt64);                      //     wrap integer
                            constexpr int32_t MASK = (int64_t(1) << (8 * sizeof(To))) - int64_t(1);
                            return PrimitiveExpr<To, ToL>(
                                (wrapped bitand PrimitiveExpr<int32_t, ToL>(MASK)).move()               //     truncate integer
                            );
                        } else {
                            auto wrapped = unary<uint32_t, ToL>(::wasm::WrapInt64);                     //     wrap integer
                            constexpr uint32_t MASK = (uint64_t(1) << (8 * sizeof(To))) - uint64_t(1);
                            return PrimitiveExpr<To, ToL>(
                                (wrapped bitand PrimitiveExpr<uint32_t, ToL>(MASK)).move()              //     truncate integer
                            );
                        }
                    }
                }
                if constexpr (FromL > 1 and FromL == ToL) {                                             //   vectorial
                    if constexpr (std::is_signed_v<From>) {                                             //    signed
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 2 and FromL <= 8)             //     i8 -> i16
                            return unary<To, ToL>(::wasm::ExtendLowSVecI8x16ToVecI16x8);
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 2 and FromL == 16) {          //     i8 -> i16
                            std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                            vectors[0] =
                                clone().template unary<To, ToL / 2>(::wasm::ExtendLowSVecI8x16ToVecI16x8);
                            vectors[1] = unary<To, ToL / 2>(::wasm::ExtendHighSVecI8x16ToVecI16x8);
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 4)                            //     i8 -> i32
                            return to<int16_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 8)                            //     i8 -> i64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4 and FromL <= 4)             //     i16 -> i32
                            return unary<To, ToL>(::wasm::ExtendLowSVecI16x8ToVecI32x4);
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4 and FromL == 8) {           //     i16 -> i32
                            std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                            vectors[0] =
                                clone().template unary<To, ToL / 2>(::wasm::ExtendLowSVecI16x8ToVecI32x4);
                            vectors[1] = unary<To, ToL / 2>(::wasm::ExtendHighSVecI16x8ToVecI32x4);
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 8)                            //     i16 -> i64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL <= 2)             //     i32 -> i64
                            return unary<To, ToL>(::wasm::ExtendLowSVecI32x4ToVecI64x2);
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL == 4) {           //     i32 -> i64
                            std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                            vectors[0] =
                                clone().template unary<To, ToL / 2>(::wasm::ExtendLowSVecI32x4ToVecI64x2);
                            vectors[1] = unary<To, ToL / 2>(::wasm::ExtendHighSVecI32x4ToVecI64x2);
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 2 and FromL <= 8)             //     u8 -> u16
                            return unary<To, ToL>(::wasm::ExtendLowUVecI8x16ToVecI16x8);
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 2 and FromL == 16) {          //     u8 -> u16
                            std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                            vectors[0] =
                                clone().template unary<To, ToL / 2>(::wasm::ExtendLowUVecI8x16ToVecI16x8);
                            vectors[1] = unary<To, ToL / 2>(::wasm::ExtendHighUVecI8x16ToVecI16x8);
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 4)                            //     u8 -> u32
                            return to<uint16_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 8)                            //     u8 -> u64
                            return to<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4 and FromL <= 4)             //     u16 -> u32
                            return unary<To, ToL>(::wasm::ExtendLowUVecI16x8ToVecI32x4);
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4 and FromL == 8) {           //     u16 -> u32
                            std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                            vectors[0] =
                                clone().template unary<To, ToL / 2>(::wasm::ExtendLowUVecI16x8ToVecI32x4);
                            vectors[1] = unary<To, ToL / 2>(::wasm::ExtendHighUVecI16x8ToVecI32x4);
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 8)                            //     u16 -> u64
                            return to<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL <= 2)             //     u32 -> u64
                            return unary<To, ToL>(::wasm::ExtendLowUVecI32x4ToVecI64x2);
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL == 4) {           //     u32 -> u64
                            std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                            vectors[0] =
                                clone().template unary<To, ToL / 2>(::wasm::ExtendLowUVecI32x4ToVecI64x2);
                            vectors[1] = unary<To, ToL / 2>(::wasm::ExtendHighUVecI32x4ToVecI64x2);
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                    }
                }
            }
            if constexpr (std::floating_point<To>) {                                                    //  to floating point
                if constexpr (FromL == 1 and ToL == 1) {                                                //   scalar
                    if constexpr (std::is_signed_v<From>) {                                             //    signed
                        if constexpr (sizeof(From) <= 4 and sizeof(To) == 4)                            //     i32 -> f32
                            return unary<To, ToL>(::wasm::ConvertSInt32ToFloat32);
                        if constexpr (sizeof(From) <= 4 and sizeof(To) == 8)                            //     i32 -> f64
                            return unary<To, ToL>(::wasm::ConvertSInt32ToFloat64);
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                            //     i64 -> f32
                            return unary<To, ToL>(::wasm::ConvertSInt64ToFloat32);
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     i64 -> f64
                            return unary<To, ToL>(::wasm::ConvertSInt64ToFloat64);
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) <= 4 and sizeof(To) == 4)                            //     u32 -> f32
                            return unary<To, ToL>(::wasm::ConvertUInt32ToFloat32);
                        if constexpr (sizeof(From) <= 4 and sizeof(To) == 8)                            //     u32 -> f64
                            return unary<To, ToL>(::wasm::ConvertUInt32ToFloat64);
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                            //     u64 -> f32
                            return unary<To, ToL>(::wasm::ConvertUInt64ToFloat32);
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     u64 -> f64
                            return unary<To, ToL>(::wasm::ConvertUInt64ToFloat64);
                    }
                }
                if constexpr (FromL > 1 and FromL == ToL) {                                             //   vectorial
                    if constexpr (std::is_signed_v<From>) {                                             //    signed
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 4)                            //     i8 -> f32
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 8)                            //     i8 -> f64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4)                            //     i16 -> f32
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 8)                            //     i16 -> f64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 4)                            //     i32 -> f32
                            return unary<To, ToL>(::wasm::ConvertSVecI32x4ToVecF32x4);
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL <= 2)             //     i32 -> f64
                            return unary<To, ToL>(::wasm::ConvertLowSVecI32x4ToVecF64x2);
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL == 4) {           //     i32 -> f64
                            std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                            vectors[0] =
                                clone().template unary<To, ToL / 2>(::wasm::ConvertLowSVecI32x4ToVecF64x2);
                            auto high_to_low = swizzle_lanes(std::to_array<uint8_t>({ 2, 3 }));
                            vectors[1] =
                                high_to_low.template unary<To, ToL / 2>(::wasm::ConvertLowSVecI32x4ToVecF64x2);
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 4)                            //     u8 -> f32
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 8)                            //     u8 -> f64
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4)                            //     u16 -> f32
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 8)                            //     u16 -> f64
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 4)                            //     u32 -> f32
                            return unary<To, ToL>(::wasm::ConvertUVecI32x4ToVecF32x4);
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL <= 2)             //     u32 -> f64
                            return unary<To, ToL>(::wasm::ConvertLowUVecI32x4ToVecF64x2);
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL == 4) {           //     u32 -> f64
                            std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                            vectors[0] =
                                clone().template unary<To, ToL / 2>(::wasm::ConvertLowUVecI32x4ToVecF64x2);
                            auto high_to_low = swizzle_lanes(std::to_array<uint8_t>({ 2, 3 }));
                            vectors[1] =
                                high_to_low.template unary<To, ToL / 2>(::wasm::ConvertLowUVecI32x4ToVecF64x2);
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                    }
                }
            }
        }

        if constexpr (std::floating_point<From>) {                                                      // from floating point
            if constexpr (integral<To>) {                                                               //  to integer
                if constexpr (FromL == 1 and ToL == 1) {                                                //   scalar
                    if constexpr (std::is_signed_v<To>) {                                               //    signed
                        if constexpr (sizeof(From) == 4 and sizeof(To) <= 4)                            //     f32 -> i32
                            return unary<int32_t, ToL>(::wasm::TruncSFloat32ToInt32).template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8)                            //     f32 -> i64
                            return unary<To, ToL>(::wasm::TruncSFloat32ToInt64);
                        if constexpr (sizeof(From) == 8 and sizeof(To) <= 4)                            //     f64 -> i32
                            return unary<int32_t, ToL>(::wasm::TruncSFloat64ToInt32).template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     f64 -> i64
                            return unary<To, ToL>(::wasm::TruncSFloat64ToInt64);
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) == 4 and sizeof(To) <= 4)                            //     f32 -> u32
                            return unary<uint32_t, ToL>(::wasm::TruncUFloat32ToInt32).template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8)                            //     f32 -> u64
                            return unary<To, ToL>(::wasm::TruncUFloat32ToInt64);
                        if constexpr (sizeof(From) == 8 and sizeof(To) <= 4)                            //     f64 -> u32
                            return unary<uint32_t, ToL>(::wasm::TruncUFloat64ToInt32).template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     f64 -> u64
                            return unary<To, ToL>(::wasm::TruncUFloat64ToInt64);
                    }
                }
                if constexpr (FromL > 1 and FromL == ToL) {                                             //   vectorial
                    if constexpr (std::is_signed_v<To>) {                                               //    signed
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 4)                            //     f32 -> i32
                            return unary<To, ToL>(::wasm::TruncSatSVecF32x4ToVecI32x4);
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                            //     f64 -> i32
                            return unary<To, ToL>(::wasm::TruncSatZeroSVecF64x2ToVecI32x4);
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8)                            //     f32 -> i64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     f64 -> i64
                            return to<int32_t, ToL>().template to<To, ToL>();
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 4)                            //     f32 -> u32
                            return unary<To, ToL>(::wasm::TruncSatUVecF32x4ToVecI32x4);
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                            //     f64 -> u32
                            return unary<To, ToL>(::wasm::TruncSatZeroUVecF64x2ToVecI32x4);
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8)                            //     f32 -> u64
                            return convert<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     f64 -> u64
                            return convert<uint32_t, ToL>().template to<To, ToL>();
                    }
                }
            }
            if constexpr (std::floating_point<To>) {                                                    //  to floating point
                if constexpr (FromL == 1 and ToL == 1) {                                                //   scalar
                    if constexpr (sizeof(From) == 4 and sizeof(To) == 8)                                //    f32 -> f64
                        return unary<To, ToL>(::wasm::PromoteFloat32);
                    if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                                //    f64 -> f32
                        return unary<To, ToL>(::wasm::DemoteFloat64);
                }
                if constexpr (FromL > 1 and FromL == ToL) {                                             //   vectorial
                    if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL <= 2)                 //    f32 -> f64
                        return unary<To, ToL>(::wasm::PromoteLowVecF32x4ToVecF64x2);
                    if constexpr (sizeof(From) == 4 and sizeof(To) == 8 and FromL == 4) {               //    f32 -> f64
                        std::array<PrimitiveExpr<To, ToL / 2>, 2> vectors;
                        vectors[0] = clone().template unary<To, ToL / 2>(::wasm::PromoteLowVecF32x4ToVecF64x2);
                        auto high_to_low = swizzle_lanes(std::to_array<uint8_t>({ 2, 3 }));
                        vectors[1] =
                            high_to_low.template unary<To, ToL / 2>(::wasm::PromoteLowVecF32x4ToVecF64x2);
                        return PrimitiveExpr<To, ToL>(std::move(vectors));
                    }
                    if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                                //    f64 -> f32
                        return unary<To, ToL>(::wasm::DemoteZeroVecF64x2ToVecF32x4);
                }
            }
        }

        M_unreachable("illegal conversion");
    }

    public:
    /** Implicit conversion of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<To, ToL>`.  Only applicable if
     *
     * - `L` and `ToL` are equal, i.e. conversion does not change the number of SIMD lanes
     * - `T` and `To` have same signedness
     * - neither or both `T` and `To` are integers
     * - `T` can be *trivially* converted to `To` (e.g. `int` to `long` but not `long` to `int`)
     * - `To` is not `bool`
     */
    template<dsl_primitive To, std::size_t ToL = L>
    requires (L == ToL) and                     // L and ToL are equal
             same_signedness<T, To> and         // T and To have same signedness
             (integral<T> == integral<To>) and  // neither nor both T and To are integers (excluding bool)
             (sizeof(T) <= sizeof(To))          // T can be *trivially* converted to To
    operator PrimitiveExpr<To, ToL>() { return convert<To, ToL>(); }

    /** Explicit conversion of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<To, ToL>`.  Only applicable if
     *
     * - `L` and `ToL` are equal, i.e. conversion does not change the number of SIMD lanes
     * - `T` and `To` have same signedness or `T` is `bool` or `char` or `To` is `bool` or `char`
     * - for scalar values:
     *   - `T` can be converted to `To` (e.g. `int` to `long`, `long` to `int`, `float` to `int`)
     * - for vectorial values:
     *   - `T` can be converted to `To` (e.g. `int` to `long`, `float` to `int`) except integer conversion to a less
     *     precise type, conversion from 64-bit integer to floating point, or conversion from floating point to 8-bit
     *     or 16-bit integer
     */
    template<dsl_primitive To, std::size_t ToL = L>
    requires (L == ToL) and                                                         // L and ToL are equal
             (same_signedness<T, To> or                                             // T and To have same signedness
              boolean<T> or std::same_as<T, char> or                                //  or T is bool or char
              boolean<To> or std::same_as<To, char>) and                            //  or To is bool or char
             ((L != 1) or                                                           // for scalar values
              std::is_convertible_v<T, To>) and                                     //  T can be converted to To
             ((L == 1) or                                                           // for vectorial values
              (std::is_convertible_v<T, To> and                                     //  T can be converted to To
               not (integral<T> and integral<To> and sizeof(T) > sizeof(To)) and    //   except integer conversion to less precise type
               not (integral<T> and sizeof(T) == 8 and std::floating_point<To>) and //   except 64-bit integer to floating point
               not (std::floating_point<T> and integral<To> and sizeof(To) <= 2)))  //   except floating point to 8-bit or 16-bit integer
    PrimitiveExpr<To, ToL> to() { return convert<To, ToL>(); }

    /** Explicit conversion of a `PrimitiveExpr<uint32_t, 1>` to a `PrimitiveExpr<To*, ToL>`
     *
     * - `T` is `uint32_t`
     * - `L` equals 1, i.e. its scalar
     * - `To` is a pointer to primitive type
     */
    template<dsl_pointer_to_primitive To, std::size_t ToL = L>
    PrimitiveExpr<To, ToL> to() requires std::same_as<T, uint32_t> and (L == 1) {
        return PrimitiveExpr<To, ToL>(*this);
    }

    /** Conversion of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<std::make_signed_t<T>, L>`. Only applicable if
     *
     * - `T` is an unsigned integral type except `bool`
     */
    auto make_signed() requires unsigned_integral<T> { return PrimitiveExpr<std::make_signed_t<T>, L>(move()); }

    /** Conversion of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<std::make_unsigned_t<T>, L>`. Only available if
    *
    * - `T` is a signed integral type except `bool`
    */
    auto make_unsigned() requires signed_integral<T> { return PrimitiveExpr<std::make_unsigned_t<T>, L>(move()); }

    /** Reinterpretation of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<To, L>`.  Only applicable if
     *
     * - `L` equals 1, i.e. the value is scalar
     * - `T` is integral and `To` is floating point or vice versa
     * - `T` and `To` have same size
     */
    template<dsl_primitive To, std::size_t ToL = L>
    requires (L == ToL) and (L == 1) and                        // value is scalar
             ((integral<T> and std::floating_point<To>) or      // either T is integral and To is floating point
              (std::floating_point<T> and integral<To>)) and    //  or vice versa
             (sizeof(T) == sizeof(To))                          // T and To have same size
    PrimitiveExpr<To, ToL> reinterpret() {
        using From = T;
        constexpr std::size_t FromL = L;

        if constexpr (integral<From>) {                                                     // from integer
            if constexpr (std::floating_point<To>) {                                        //  to floating point
                if constexpr (FromL == 1) {                                                 //   scalar
                    if constexpr (sizeof(From) == 4 and sizeof(To) == 4)                    //    i32 -> f32
                        return unary<To, ToL>(::wasm::ReinterpretInt32);
                    if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                    //    i64 -> f64
                        return unary<To, ToL>(::wasm::ReinterpretInt64);
                }
            }
        }

        if constexpr (std::floating_point<From>) {                                          // from floating point
            if constexpr (integral<To>) {                                                   //  to integer
                if constexpr (FromL == 1) {                                                 //   scalar
                    if constexpr (sizeof(From) == 4 and sizeof(To) == 4)                    //    f32 -> i32
                        return unary<To, ToL>(::wasm::ReinterpretFloat32);
                    if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                    //    f64 -> i64
                        return unary<To, ToL>(::wasm::ReinterpretFloat64);
                }
            }
        }

        M_unreachable("illegal reinterpretation");
    }

    /** Broadcasts a `PrimitiveExpr<T, 1>` to a `PrimitiveExpr<T, ToL>`. */
    template<std::size_t ToL>
    requires (L == 1) and (ToL > 1) and (ToL * sizeof(T) <= 16)
    PrimitiveExpr<T, ToL> broadcast() {
        if constexpr (boolean<T>)
            return unary<T, ToL>(::wasm::UnaryOp::SplatVecI8x16);

        if constexpr (integral<T>) {
            if constexpr (sizeof(T) == 1)
                return unary<T, ToL>(::wasm::UnaryOp::SplatVecI8x16);
            if constexpr (sizeof(T) == 2)
                return unary<T, ToL>(::wasm::UnaryOp::SplatVecI16x8);
            if constexpr (sizeof(T) == 4)
                return unary<T, ToL>(::wasm::UnaryOp::SplatVecI32x4);
            if constexpr (sizeof(T) == 8)
                return unary<T, ToL>(::wasm::UnaryOp::SplatVecI64x2);
        }

        if constexpr (std::floating_point<T>) {
            if constexpr (sizeof(T) == 4)
                return unary<T, ToL>(::wasm::UnaryOp::SplatVecF32x4);
            if constexpr (sizeof(T) == 8)
                return unary<T, ToL>(::wasm::UnaryOp::SplatVecF64x2);
        }

        M_unreachable("illegal broadcast");
    }
    /** Broadcasts a `PrimitiveExpr<T, 1>` to a `PrimitiveExpr<T, ToL>`. */
    template<std::size_t ToL>
    requires (L == 1) and (ToL > 1) and (ToL * sizeof(T) > 16)
    PrimitiveExpr<T, ToL> broadcast() {
        using ResT = PrimitiveExpr<T, ToL>;
        std::array<typename ResT::vector_type, ResT::num_vectors> vectors;
        for (std::size_t idx = 0; idx < ResT::num_vectors; ++idx)
            vectors[idx] = clone().template broadcast<ResT::vector_type::num_simd_lanes>();
        return ResT(std::move(vectors));
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Unary operations
     *----------------------------------------------------------------------------------------------------------------*/

#define UNOP_(NAME, TYPE) (::wasm::UnaryOp::NAME##TYPE)
#define UNIOP_(NAME) [] { \
    if constexpr (sizeof(T) == 8) \
        return UNOP_(NAME,Int64); \
    else if constexpr (sizeof(T) <= 4) \
        return UNOP_(NAME,Int32); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define UNFOP_(NAME) [] { \
    if constexpr (sizeof(T) == 8) \
        return UNOP_(NAME,Float64); \
    else if constexpr (sizeof(T) == 4) \
        return UNOP_(NAME,Float32); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define UNVOP_(NAME, TYPE) (::wasm::UnaryOp::NAME##Vec##TYPE)
#define UNIVOP_(NAME) [] { \
    if constexpr (sizeof(T) == 8) \
        return UNVOP_(NAME,I64x2); \
    else if constexpr (sizeof(T) == 4) \
        return UNVOP_(NAME,I32x4); \
    else if constexpr (sizeof(T) == 2) \
        return UNVOP_(NAME,I16x8); \
    else if constexpr (sizeof(T) == 1) \
        return UNVOP_(NAME,I8x16); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define UNFVOP_(NAME) [] { \
    if constexpr (sizeof(T) == 8) \
        return UNVOP_(NAME,F64x2); \
    else if constexpr (sizeof(T) == 4) \
        return UNVOP_(NAME,F32x4); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define UNARY_VOP(NAME) [] { \
    if constexpr (std::integral<T>) \
        return UNIVOP_(NAME); \
    else if constexpr (std::floating_point<T>) \
        return UNFVOP_(NAME); \
    else \
        M_unreachable("unsupported operation"); \
} ()

    /*----- Arithmetical operations ----------------------------------------------------------------------------------*/

    PrimitiveExpr operator+() requires arithmetic<T> { return *this; }

    PrimitiveExpr operator-() requires integral<T> and (L == 1) { return PrimitiveExpr(T(0)) - *this; }
    PrimitiveExpr operator-() requires std::floating_point<T> and (L == 1) { return unary<T, L>(UNFOP_(Neg)); }
    PrimitiveExpr operator-() requires arithmetic<T> and (L > 1) { return unary<T, L>(UNARY_VOP(Neg)); }

    PrimitiveExpr abs() requires std::floating_point<T> and (L == 1) { return unary<T, L>(UNFOP_(Abs)); }
    PrimitiveExpr abs() requires (L > 1) { return unary<T, L>(UNARY_VOP(Abs)); }
    PrimitiveExpr ceil() requires std::floating_point<T> and (L == 1) { return unary<T, L>(UNFOP_(Ceil)); }
    PrimitiveExpr ceil() requires std::floating_point<T> and (L > 1) { return unary<T, L>(UNFVOP_(Ceil)); }
    PrimitiveExpr floor() requires std::floating_point<T> and (L == 1) { return unary<T, L>(UNFOP_(Floor)); }
    PrimitiveExpr floor() requires std::floating_point<T> and (L > 1) { return unary<T, L>(UNFVOP_(Floor)); }
    PrimitiveExpr trunc() requires std::floating_point<T> and (L > 1) { return unary<T, L>(UNFVOP_(Trunc)); }
    PrimitiveExpr nearest() requires std::floating_point<T> and (L > 1) { return unary<T, L>(UNFVOP_(Nearest)); }

    PrimitiveExpr sqrt() requires std::floating_point<T> and (L == 1) { return unary<T, L>(UNFOP_(Sqrt)); }
    PrimitiveExpr sqrt() requires std::floating_point<T> and (L > 1) { return unary<T, L>(UNFVOP_(Sqrt)); }

    PrimitiveExpr<int16_t, L / 2> add_pairwise() requires signed_integral<T> and (sizeof(T) == 1) and (L > 1) {
        static_assert(L % 2 == 0, "must mask this expression first");
        auto vec = unary<int16_t, L / 2>(UNVOP_(ExtAddPairwiseS, I8x16ToI16x8));
        return M_CONSTEXPR_COND(decltype(vec)::num_simd_lanes == 1,
                                vec.template extract_unsafe<0>(), // extract a single sum from vector to scalar
                                vec);
    }
    PrimitiveExpr<uint16_t, L / 2> add_pairwise() requires unsigned_integral<T> and (sizeof(T) == 1) and (L > 1) {
        static_assert(L % 2 == 0, "must mask this expression first");
        auto vec = unary<uint16_t, L / 2>(UNVOP_(ExtAddPairwiseU, I8x16ToI16x8));
        return M_CONSTEXPR_COND(decltype(vec)::num_simd_lanes == 1,
                                vec.template extract_unsafe<0>(), // extract a single sum from vector to scalar
                                vec);
    }
    PrimitiveExpr<int32_t, L / 2> add_pairwise() requires signed_integral<T> and (sizeof(T) == 2) and (L > 1) {
        static_assert(L % 2 == 0, "must mask this expression first");
        auto vec = unary<int32_t, L / 2>(UNVOP_(ExtAddPairwiseS, I16x8ToI32x4));
        return M_CONSTEXPR_COND(decltype(vec)::num_simd_lanes == 1,
                                vec.template extract_unsafe<0>(), // extract a single sum from vector to scalar
                                vec);
    }
    PrimitiveExpr<uint32_t, L / 2> add_pairwise() requires unsigned_integral<T> and (sizeof(T) == 2) and (L > 1) {
        static_assert(L % 2 == 0, "must mask this expression first");
        auto vec = unary<uint32_t, L / 2>(UNVOP_(ExtAddPairwiseU, I16x8ToI32x4));
        return M_CONSTEXPR_COND(decltype(vec)::num_simd_lanes == 1,
                                vec.template extract_unsafe<0>(), // extract a single sum from vector to scalar
                                vec);
    }

    /*----- Bitwise operations ---------------------------------------------------------------------------------------*/

    PrimitiveExpr operator~() requires integral<T> and (L == 1) { return PrimitiveExpr(T(-1)) xor *this; }
    PrimitiveExpr operator~() requires integral<T> and (L > 1) { return unary<T, L>(UNVOP_(Not, 128)); }

    PrimitiveExpr clz() requires unsigned_integral<T> and (sizeof(T) >= 4) and (L == 1) {
        return unary<T, L>(UNIOP_(Clz));
    }
    PrimitiveExpr clz() requires unsigned_integral<T> and (sizeof(T) == 2) and (L == 1) {
        return unary<T, L>(UNIOP_(Clz)) - PrimitiveExpr(16U); // the value is represented as I32
    }
    PrimitiveExpr clz() requires unsigned_integral<T> and (sizeof(T) == 1) and (L == 1) {
        return unary<T, L>(UNIOP_(Clz)) - PrimitiveExpr(24U); // the value is represented as I32
    }
    PrimitiveExpr ctz() requires unsigned_integral<T> and (L == 1) { return unary<T, L>(UNIOP_(Ctz)); }
    PrimitiveExpr popcnt() requires unsigned_integral<T> and (L == 1) { return unary<T, L>(UNIOP_(Popcnt)); }
    PrimitiveExpr popcnt() requires unsigned_integral<T> and (sizeof(T) == 1) and (L > 1) {
        return unary<T, L>(UNVOP_(Popcnt, I8x16));
    }
    PrimitiveExpr popcnt() requires unsigned_integral<T> and (sizeof(T) == 2) and (L > 1) {
        auto popcnt_on_I8x16 = this->unary<uint8_t, L * 2>(UNVOP_(Popcnt, I8x16));
        return popcnt_on_I8x16.add_pairwise();
    }
    PrimitiveExpr popcnt() requires unsigned_integral<T> and (sizeof(T) == 4) and (L > 1) {
        auto popcnt_on_I8x16 = this->unary<uint8_t, L * 4>(UNVOP_(Popcnt, I8x16));
        return popcnt_on_I8x16.add_pairwise().add_pairwise();
    }

    /** Concatenates the boolean values of `this` into a single mask. */
    PrimitiveExpr<uint32_t, 1> bitmask() requires boolean<T> and (L > 1) {
        auto bitmask = unary<uint32_t, 1>(UNVOP_(Bitmask, I8x16));
        return M_CONSTEXPR_COND(L * sizeof(T) == 16, bitmask, bitmask bitand uint32_t((1U << L) - 1U)); // to remove unused lanes
    }
    /** Concatenates the most significant bit of each value of `this` into a single mask. */
    PrimitiveExpr<uint32_t, 1> bitmask() requires integral<T> and (L > 1) {
        auto bitmask = unary<uint32_t, 1>(UNIVOP_(Bitmask));
        return M_CONSTEXPR_COND(L * sizeof(T) == 16, bitmask, bitmask bitand uint32_t((1U << L) - 1U)); // to remove unused lanes
    }

    /*----- Comparison operations ------------------------------------------------------------------------------------*/

    PrimitiveExpr<bool, L> eqz() requires integral<T> and (L == 1) { return unary<bool, L>(UNIOP_(EqZ)); }

    /*----- Logical operations ---------------------------------------------------------------------------------------*/

    PrimitiveExpr operator not() requires boolean<T> and (L == 1) { return unary<T, L>(UNIOP_(EqZ)); }
    PrimitiveExpr operator not() requires boolean<T> and (L > 1) { return unary<T, L>(UNVOP_(Not, 128)); }

    /** Returns `true` iff any value is `true`. */
    PrimitiveExpr<bool, 1> any_true() requires boolean<T> and (L > 1) {
        auto masked =
            M_CONSTEXPR_COND(L * sizeof(T) == 16, *this, PrimitiveExpr(true) and *this); // to set the unused lanes to false
        return masked.template unary<bool, 1>(UNVOP_(AnyTrue, 128));
    }
    /** Returns `true` iff any value is non-zero. */
    PrimitiveExpr<bool, 1> any_true() requires integral<T> and (L > 1) {
        auto masked =
            M_CONSTEXPR_COND(L * sizeof(T) == 16, *this, PrimitiveExpr(T(-1)) bitand *this); // to set the unused lanes to 0
        return masked.template unary<bool, 1>(UNVOP_(AnyTrue, 128));
    }
    /** Returns `true` iff all values are `true`. */
    PrimitiveExpr<bool, 1> all_true() requires boolean<T> and (L > 1) {
        std::array<uint8_t, 16> bytes;
        auto it = std::fill_n(bytes.begin(), L, 0);
        std::fill(it, bytes.end(), 0xff); // all bits to 1 represent true
        auto masked =
            M_CONSTEXPR_COND(L * sizeof(T) == 16, *this, PrimitiveExpr(bytes) or *this); // to set the unused lanes to true
        return masked.template unary<bool, 1>(UNVOP_(AllTrue, I8x16));
    }
    /** Returns `true` iff all values are non-zero. */
    PrimitiveExpr<bool, 1> all_true() requires integral<T> and (L > 1) {
        std::array<uint8_t, 16> bytes;
        auto it = std::fill_n(bytes.begin(), L, 0);
        std::fill(it, bytes.end(), 1);
        auto masked =
            M_CONSTEXPR_COND(L * sizeof(T) == 16, *this, PrimitiveExpr(bytes) bitor *this); // to set the unused lanes to 1
        return masked.template unary<bool, 1>(UNIVOP_(AllTrue));
    }

    /*----- Hashing operations ---------------------------------------------------------------------------------------*/

    PrimitiveExpr<uint64_t, L> hash() requires unsigned_integral<T> and (L == 1) { return *this; }
    PrimitiveExpr<uint64_t, L> hash() requires signed_integral<T> and (L == 1) { return make_unsigned(); }
    PrimitiveExpr<uint64_t, L> hash() requires std::floating_point<T> and (sizeof(T) == 4) and (L == 1) {
        return reinterpret<int32_t>().make_unsigned();
    }
    PrimitiveExpr<uint64_t, L> hash() requires std::floating_point<T> and (sizeof(T) == 8) and (L == 1) {
        return reinterpret<int64_t>().make_unsigned();
    }
    PrimitiveExpr<uint64_t, L> hash() requires std::same_as<T, bool> and (L == 1) { return to<uint64_t>(); }

#undef UNARY_VOP
#undef UNFVOP_
#undef UNIVOP_
#undef UNVOP_
#undef UNFOP_
#undef UNIOP_
#undef UNOP_


    /*------------------------------------------------------------------------------------------------------------------
     * Binary operations
     *----------------------------------------------------------------------------------------------------------------*/

#define BINOP_(NAME, SIGN, TYPE) (::wasm::BinaryOp::NAME##SIGN##TYPE)
#define BINIOP_(NAME, SIGN) [] { \
    if constexpr (sizeof(To) == 8) \
        return BINOP_(NAME,SIGN,Int64); \
    else if constexpr (sizeof(To) <= 4) \
        return BINOP_(NAME,SIGN,Int32); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define BINFOP_(NAME) [] { \
    if constexpr (sizeof(To) == 8) \
        return BINOP_(NAME,,Float64); \
    else if constexpr (sizeof(To) == 4) \
        return BINOP_(NAME,,Float32); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define BINARY_OP(NAME, SIGN) [] { \
    if constexpr (std::integral<To>) \
        return BINIOP_(NAME, SIGN); \
    else if constexpr (std::floating_point<To>) \
        return BINFOP_(NAME); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define BINVOP_(NAME, SIGN, TYPE) (::wasm::BinaryOp::NAME##SIGN##Vec##TYPE)
#define BINIVOP_(NAME, SIGN) [] { \
    if constexpr (sizeof(To) == 8) \
        return BINVOP_(NAME,SIGN,I64x2); \
    else if constexpr (sizeof(To) == 4) \
        return BINVOP_(NAME,SIGN,I32x4); \
    else if constexpr (sizeof(To) == 2) \
        return BINVOP_(NAME,SIGN,I16x8); \
    else if constexpr (sizeof(To) == 1) \
        return BINVOP_(NAME,SIGN,I8x16); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define BINFVOP_(NAME) [] { \
    if constexpr (sizeof(To) == 8) \
        return BINVOP_(NAME,,F64x2); \
    else if constexpr (sizeof(To) == 4) \
        return BINVOP_(NAME,,F32x4); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define BINARY_VOP(NAME, SIGN) [] { \
    if constexpr (std::integral<To>) \
        return BINIVOP_(NAME, SIGN); \
    else if constexpr (std::floating_point<To>) \
        return BINFVOP_(NAME); \
    else \
        M_unreachable("unsupported operation"); \
} ()

    /*----- Arithmetical operations ----------------------------------------------------------------------------------*/

    /** Adds `this` to \p other. */
    template<arithmetic U>
    requires arithmetically_combinable<T, U, L>
    auto operator+(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> {
        using To = common_type_t<T, U>;
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(M_CONSTEXPR_COND(L == 1, BINARY_OP(Add,), BINARY_VOP(Add,)), other);
        else
            return this->template to<To, L>().operator+(other.template to<To, L>());
    }

    /** Subtracts \p other from `this`. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    auto operator-(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> {
        using To = common_type_t<T, U>;
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(M_CONSTEXPR_COND(L == 1, BINARY_OP(Sub,), BINARY_VOP(Sub,)), other);
        else
            return this->template to<To, L>().operator-(other.template to<To, L>());
    }

    /** Multiplies `this` and \p other. */
    template<arithmetic U>
    requires arithmetically_combinable<T, U, L>
    auto operator*(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires (L == 1) {
        using To = common_type_t<T, U>;
        return binary<To, L, To, L>(BINARY_OP(Mul,), other);
    }
    /** Multiplies `this` and \p other. */
    template<arithmetic U>
    requires arithmetically_combinable<T, U, L> and (sizeof(common_type_t<T, U>) != 1)
    auto operator*(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires (L > 1) {
        using To = common_type_t<T, U>;
        auto op = [](){
            if constexpr (std::integral<To>) {
                if constexpr (sizeof(To) == 8)
                    return BINVOP_(Mul,, I64x2);
                else if constexpr (sizeof(To) == 4)
                    return BINVOP_(Mul,, I32x4);
                else if constexpr (sizeof(To) == 2)
                    return BINVOP_(Mul,, I16x8);
            } else if (std::floating_point<To>) {
                return BINFVOP_(Mul);
            }
            M_unreachable("unsupported operation");
        }();
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(op, other);
        else
            return this->template to<To, L>().operator*(other.template to<To, L>());
    }
    /** Multiplies `this` and \p other. */
    template<arithmetic U>
    requires arithmetically_combinable<T, U, L> and (sizeof(common_type_t<T, U>) == 1)
    auto operator*(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires (L > 8) {
        static_assert(integral<U> and integral<T> and same_signedness<T, U>);
        static_assert(L == 16);
        using To = std::conditional_t<std::is_signed_v<T>, int16_t, uint16_t>;
        auto op_low =
            M_CONSTEXPR_COND(std::is_signed_v<T>, BINVOP_(ExtMulLow, S, I16x8), BINVOP_(ExtMulLow, U, I16x8));
        auto op_high =
            M_CONSTEXPR_COND(std::is_signed_v<T>, BINVOP_(ExtMulHigh, S, I16x8), BINVOP_(ExtMulHigh, U, I16x8));
        auto this_cpy  = this->clone();
        auto other_cpy = other.clone();
        auto referenced_bits_low = this_cpy.referenced_bits(); // moved
        auto referenced_bits_high = this->referenced_bits(); // moved
        referenced_bits_low.splice(referenced_bits_low.end(), other_cpy.referenced_bits());
        referenced_bits_high.splice(referenced_bits_high.end(), other.referenced_bits());
        PrimitiveExpr<To, 8> low(
            /* expr=            */ Module::Builder().makeBinary(op_low, this_cpy.expr(), other_cpy.expr()),
            /* referenced_bits= */ std::move(referenced_bits_low)
        );
        PrimitiveExpr<To, 8> high(
            /* expr=            */ Module::Builder().makeBinary(op_high, this->expr(), other.expr()),
            /* referenced_bits= */ std::move(referenced_bits_high)
        );
        /* Shuffle to select the lower byte of each lane since integer narrowing would be performed saturating, i.e.
         * instead of overflows the values are mapped to the extremes of their domain. */
        auto indices = std::to_array<uint8_t>({ 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30 });
        return PrimitiveExpr<common_type_t<T, U>, L>(ShuffleBytes(low, high, indices).move());
    }

    /** Divides `this` by \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    auto operator/(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires (L == 1) {
        using To = common_type_t<T, U>;
        return binary<To, L, To, L>(M_CONSTEXPR_COND(std::is_signed_v<To>, BINARY_OP(Div, S), BINARY_OP(Div, U)),
                                    other);
    }
    /** Divides `this` by \p other. */
    template<std::floating_point U>
    requires arithmetically_combinable<T, U, L>
    auto operator/(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L>
    requires std::floating_point<T> and (L > 1) {
        using To = common_type_t<T, U>;
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(BINFVOP_(Div), other);
        else
            return this->template to<To, L>().operator/(other.template to<To, L>());
    }

    /** Computes the remainder of dividing `this` by \p other. */
    template<integral U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    auto operator%(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L>
    requires integral<T> and (L == 1) {
        using To = common_type_t<T, U>;
        return binary<To, L, To, L>(M_CONSTEXPR_COND(std::is_signed_v<To>, BINIOP_(Rem, S), BINIOP_(Rem, U)), other);
    }

    /** Copy the sign bit of \p other to `this`. */
    template<std::floating_point U>
    requires arithmetically_combinable<T, U, L>
    auto copy_sign(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L>
    requires std::floating_point<T> and (L == 1) {
        using To = common_type_t<T, U>;
        return binary<To, L, To, L>(BINFOP_(CopySign), other);
    }

    /** Computes the minimum of `this` and \p other. */
    template<std::floating_point U>
    requires arithmetically_combinable<T, U, L>
    auto min(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires std::floating_point<T> {
        using To = common_type_t<T, U>;
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(M_CONSTEXPR_COND(L == 1, BINFOP_(Min), BINFVOP_(Min)), other); // XXX: or PMin?
        else
            return this->template to<To, L>().min(other.template to<To, L>());
    }
    /** Computes the minimum of `this` and \p other. */
    template<integral U>
    requires arithmetically_combinable<T, U, L> and (sizeof(common_type_t<T, U>) != 8)
    auto min(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires integral<T> and (L > 1) {
        using To = common_type_t<T, U>;
        auto op = [](){
            if constexpr (sizeof(To) == 4)
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Min, S, I32x4), BINVOP_(Min, U, I32x4));
            else if constexpr (sizeof(To) == 2)
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Min, S, I16x8), BINVOP_(Min, U, I16x8));
            else if constexpr (sizeof(To) == 1)
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Min, S, I8x16), BINVOP_(Min, U, I8x16));
            M_unreachable("unsupported operation");
        }();
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(op, other);
        else
            return this->template to<To, L>().min(other.template to<To, L>());
    }

    /** Computes the maximum of `this` and \p other. */
    template<std::floating_point U>
    requires arithmetically_combinable<T, U, L>
    auto max(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires std::floating_point<T> {
        using To = common_type_t<T, U>;
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(M_CONSTEXPR_COND(L == 1, BINFOP_(Max), BINFVOP_(Max)), other); // XXX: or PMax?
        else
            return this->template to<To, L>().max(other.template to<To, L>());
    }
    /** Computes the maximum of `this` and \p other. */
    template<integral U>
    requires arithmetically_combinable<T, U, L> and (sizeof(common_type_t<T, U>) != 8)
    auto max(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires integral<T> and (L > 1) {
        using To = common_type_t<T, U>;
        auto op = [](){
            if constexpr (sizeof(To) == 4)
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Max, S, I32x4), BINVOP_(Max, U, I32x4));
            else if constexpr (sizeof(To) == 2)
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Max, S, I16x8), BINVOP_(Max, U, I16x8));
            else if constexpr (sizeof(To) == 1)
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Max, S, I8x16), BINVOP_(Max, U, I8x16));
            M_unreachable("unsupported operation");
        }();
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(op, other);
        else
            return this->template to<To, L>().max(other.template to<To, L>());
    }

    /** Computes the (ceiled) average of `this` and \p other. */
    template<unsigned_integral U>
    requires arithmetically_combinable<T, U, L> and (sizeof(common_type_t<T, U>) <= 2)
    auto avg(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L>
    requires unsigned_integral<T> and (L > 1) {
        using To = common_type_t<T, U>;
        auto op = [](){
            if constexpr (sizeof(To) == 2)
                return BINVOP_(Avgr, U, I16x8);
            else if constexpr (sizeof(To) == 1)
                return BINVOP_(Avgr, U, I8x16);
            M_unreachable("unsupported operation");
        }();
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(op, other);
        else
            return this->template to<To, L>().avg(other.template to<To, L>());
    }

    /*----- Bitwise operations ---------------------------------------------------------------------------------------*/

    /** Computes the bitwise *and* of `this` and \p other. */
    template<std::integral U>
    requires arithmetically_combinable<T, U, L>
    auto operator bitand(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires std::integral<T> {
        using To = common_type_t<T, U>;
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(M_CONSTEXPR_COND(L == 1, BINIOP_(And,), BINVOP_(And,, 128)), other);
        else
            return this->template to<To, L>().operator bitand(other.template to<To, L>());
    }

    /** Computes the bitwise *or* of `this` and \p other. */
    template<std::integral U>
    requires arithmetically_combinable<T, U, L>
    auto operator bitor(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires std::integral<T> {
        using To = common_type_t<T, U>;
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(M_CONSTEXPR_COND(L == 1, BINIOP_(Or,), BINVOP_(Or,, 128)), other);
        else
            return this->template to<To, L>().operator bitor(other.template to<To, L>());
    }

    /** Computes the (bitwise) *xor* of `this` and \p other. */
    template<std::integral U>
    requires arithmetically_combinable<T, U, L>
    auto operator xor(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires std::integral<T> {
        using To = common_type_t<T, U>;
        if constexpr (L * sizeof(To) <= 16)
            return binary<To, L, To, L>(M_CONSTEXPR_COND(L == 1, BINIOP_(Xor,), BINVOP_(Xor,, 128)), other);
        else
            return this->template to<To, L>().operator xor(other.template to<To, L>());
    }

    /** Shifts `this` *left* by \p other. */
    template<integral U>
    requires arithmetically_combinable<T, U, L>
    auto operator<<(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L>
    requires integral<T> and (L == 1) {
        using To = common_type_t<T, U>;
        if constexpr (sizeof(To) >= 4)
            return binary<To, L, To, L>(BINIOP_(Shl,), other);
        else if constexpr (sizeof(To) == 2)
            return binary<To, L, To, L>(BINOP_(Shl,, Int32), other) bitand PrimitiveExpr<To, 1>(0xffff);
        else if constexpr (sizeof(To) == 1)
            return binary<To, L, To, L>(BINOP_(Shl,, Int32), other) bitand PrimitiveExpr<To, 1>(0xff);
        else
            M_unreachable("unsupported operation");
    }
    /** Shifts `this` *left* by \p other. */
    template<integral U>
    requires requires (PrimitiveExpr<U, 1> e) {
        PrimitiveExpr<std::conditional_t<std::is_signed_v<T>, int32_t, uint32_t>, 1>(e);
    }
    PrimitiveExpr operator<<(PrimitiveExpr<U, 1> other)
    requires integral<T> and (L > 1) {
        using Op = std::conditional_t<std::is_signed_v<T>, int32_t, uint32_t>;
        auto op = [](){
            if constexpr (sizeof(T) == 8)
                return ::wasm::SIMDShiftOp::ShlVecI64x2;
            else if constexpr (sizeof(T) == 4)
                return ::wasm::SIMDShiftOp::ShlVecI32x4;
            else if constexpr (sizeof(T) == 2)
                return ::wasm::SIMDShiftOp::ShlVecI16x8;
            else if constexpr (sizeof(T) == 1)
                return ::wasm::SIMDShiftOp::ShlVecI8x16;
            else
                M_unreachable("unsupported operation");
        }();
        auto referenced_bits = this->referenced_bits(); // moved
        referenced_bits.splice(referenced_bits.end(), other.referenced_bits());
        return PrimitiveExpr(
            /* expr=            */ Module::Builder().makeSIMDShift(op, this->expr(), PrimitiveExpr<Op, 1>(other).expr()),
            /* referenced_bits= */ std::move(referenced_bits)
        );
    }

    /** Shifts `this` *right* by \p other. */
    template<integral U>
    requires arithmetically_combinable<T, U, L>
    auto operator>>(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L>
    requires integral<T> and (L == 1) {
        using To = common_type_t<T, U>;
        return binary<To, L, To, L>(M_CONSTEXPR_COND(std::is_signed_v<T>, BINIOP_(Shr, S), BINIOP_(Shr, U)), other);
    }
    /** Shifts `this` *right* by \p other. */
    template<integral U>
    requires requires (PrimitiveExpr<U, 1> e) {
        PrimitiveExpr<std::conditional_t<std::is_signed_v<T>, int32_t, uint32_t>, 1>(e);
    }
    PrimitiveExpr operator>>(PrimitiveExpr<U, 1> other)
    requires integral<T> and (L > 1) {
        using Op = std::conditional_t<std::is_signed_v<T>, int32_t, uint32_t>;
        auto op = [](){
            if constexpr (sizeof(T) == 8)
                return M_CONSTEXPR_COND(std::is_signed_v<T>, ::wasm::SIMDShiftOp::ShrSVecI64x2,
                                                             ::wasm::SIMDShiftOp::ShrUVecI64x2);
            else if constexpr (sizeof(T) == 4)
                return M_CONSTEXPR_COND(std::is_signed_v<T>, ::wasm::SIMDShiftOp::ShrSVecI32x4,
                                                             ::wasm::SIMDShiftOp::ShrUVecI32x4);
            else if constexpr (sizeof(T) == 2)
                return M_CONSTEXPR_COND(std::is_signed_v<T>, ::wasm::SIMDShiftOp::ShrSVecI16x8,
                                                             ::wasm::SIMDShiftOp::ShrUVecI16x8);
            else if constexpr (sizeof(T) == 1)
                return M_CONSTEXPR_COND(std::is_signed_v<T>, ::wasm::SIMDShiftOp::ShrSVecI8x16,
                                                             ::wasm::SIMDShiftOp::ShrUVecI8x16);
            else
                M_unreachable("unsupported operation");
        }();
        auto referenced_bits = this->referenced_bits(); // moved
        referenced_bits.splice(referenced_bits.end(), other.referenced_bits());
        return PrimitiveExpr(
            /* expr=            */ Module::Builder().makeSIMDShift(op, this->expr(), PrimitiveExpr<Op, 1>(other).expr()),
            /* referenced_bits= */ std::move(referenced_bits)
        );
    }

    /** Rotates `this` *left* by \p other. */
    template<integral U>
    requires arithmetically_combinable<T, U, L> and (sizeof(common_type_t<T, U>) >= 4)
    auto rotl(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires integral<T> and (L == 1) {
        using To = common_type_t<T, U>;
        return binary<To, L, To, L>(BINIOP_(RotL,), other);
    }

    /** Rotates `this` *right* by \p other. */
    template<integral U>
    requires arithmetically_combinable<T, U, L> and (sizeof(common_type_t<T, U>) >= 4)
    auto rotr(PrimitiveExpr<U, L> other) -> PrimitiveExpr<common_type_t<T, U>, L> requires integral<T> and (L == 1) {
        using To = common_type_t<T, U>;
        return binary<To, L, To, L>(BINIOP_(RotR,), other);
    }

    /*----- Comparison operations ------------------------------------------------------------------------------------*/

    /** Checks whether `this` equals \p other. */
    template<dsl_primitive U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator==(PrimitiveExpr<U, L> other) {
        using To = common_type_t<T, U>;
        constexpr std::size_t ToL = L == 1 ? L : L * sizeof(T);
        if constexpr (L * sizeof(To) <= 16) {
            auto cmp = binary<bool, ToL, To, L>(M_CONSTEXPR_COND(L == 1, BINARY_OP(Eq,), BINARY_VOP(Eq,)), other);
            std::array<uint8_t, L> indices;
            for (std::size_t idx = 0; idx < L; ++idx)
                indices[idx] = idx * sizeof(To);
            return M_CONSTEXPR_COND(L == 1 or sizeof(To) == 1, cmp, cmp.swizzle_bytes(indices));
        } else {
            return this->template to<To, L>().operator==(other.template to<To, L>());
        }
    }

    /** Checks whether `this` unequal to \p other. */
    template<dsl_primitive U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator!=(PrimitiveExpr<U, L> other) {
        using To = common_type_t<T, U>;
        constexpr std::size_t ToL = L == 1 ? L : L * sizeof(T);
        if constexpr (L * sizeof(To) <= 16) {
            auto cmp = binary<bool, ToL, To, L>(M_CONSTEXPR_COND(L == 1, BINARY_OP(Ne,), BINARY_VOP(Ne,)), other);
            std::array<uint8_t, L> indices;
            for (std::size_t idx = 0; idx < L; ++idx)
                indices[idx] = idx * sizeof(To);
            return M_CONSTEXPR_COND(L == 1 or sizeof(To) == 1, cmp, cmp.swizzle_bytes(indices));
        } else {
            return this->template to<To, L>().operator!=(other.template to<To, L>());
        }
    }

    /** Checks whether `this` less than \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator<(PrimitiveExpr<U, L> other) requires arithmetic<T> {
        using To = common_type_t<T, U>;
        constexpr std::size_t ToL = L == 1 ? L : L * sizeof(T);
        auto op = [](){
            if constexpr (L == 1) {
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINARY_OP(Lt, S), BINARY_OP(Lt, U));
            } else {
                if constexpr (std::integral<To>) {
                    if constexpr (sizeof(To) == 8)
                        return BINVOP_(Lt, S, I64x2); // unsigned comparison missing, use signed but flip MSB of operands
                    else if constexpr (sizeof(To) == 4)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Lt, S, I32x4), BINVOP_(Lt, U, I32x4));
                    else if constexpr (sizeof(To) == 2)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Lt, S, I16x8), BINVOP_(Lt, U, I16x8));
                    else if constexpr (sizeof(To) == 1)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Lt, S, I8x16), BINVOP_(Lt, U, I8x16));
                } else if (std::floating_point<To>) {
                    return BINFVOP_(Lt);
                }
            }
            M_unreachable("unsupported operation");
        }();
        if constexpr (L * sizeof(To) <= 16) {
            /* Wasm does not support comparison of U64 SIMD vectors. Thus, flip the MSB in each lane (which
             * basically shifts the unsigned domain into the signed one) and use the signed comparison afterwards. */
            constexpr bool is_u64_vec = unsigned_integral<To> and sizeof(To) == 8 and L > 1;
            auto _this =
                M_CONSTEXPR_COND(is_u64_vec, (*this xor PrimitiveExpr<T, L>(T(1) << (CHAR_BIT * sizeof(T) - 1))), *this);
            auto _other =
                M_CONSTEXPR_COND(is_u64_vec, (other xor PrimitiveExpr<U, L>(U(1) << (CHAR_BIT * sizeof(U) - 1))), other);
            auto cmp = _this.template binary<bool, ToL, To, L>(op, _other);
            std::array<uint8_t, L> indices;
            for (std::size_t idx = 0; idx < L; ++idx)
                indices[idx] = idx * sizeof(To);
            return M_CONSTEXPR_COND(L == 1 or sizeof(To) == 1, cmp, cmp.swizzle_bytes(indices));
        } else {
            return this->template to<To, L>().operator<(other.template to<To, L>());
        }
    }

    /** Checks whether `this` less than or equals to \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator<=(PrimitiveExpr<U, L> other) requires arithmetic<T> {
        using To = common_type_t<T, U>;
        constexpr std::size_t ToL = L == 1 ? L : L * sizeof(T);
        auto op = [](){
            if constexpr (L == 1) {
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINARY_OP(Le, S), BINARY_OP(Le, U));
            } else {
                if constexpr (std::integral<To>) {
                    if constexpr (sizeof(To) == 8)
                        return BINVOP_(Le, S, I64x2); // unsigned comparison missing, use signed but flip MSB of operands
                    else if constexpr (sizeof(To) == 4)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Le, S, I32x4), BINVOP_(Le, U, I32x4));
                    else if constexpr (sizeof(To) == 2)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Le, S, I16x8), BINVOP_(Le, U, I16x8));
                    else if constexpr (sizeof(To) == 1)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Le, S, I8x16), BINVOP_(Le, U, I8x16));
                } else if (std::floating_point<To>) {
                    return BINFVOP_(Le);
                }
            }
            M_unreachable("unsupported operation");
        }();
        if constexpr (L * sizeof(To) <= 16) {
            /* Wasm does not support comparison of U64 SIMD vectors. Thus, flip the MSB in each lane (which
             * basically shifts the unsigned domain into the signed one) and use the signed comparison afterwards. */
            constexpr bool is_u64_vec = unsigned_integral<To> and sizeof(To) == 8 and L > 1;
            auto _this =
                M_CONSTEXPR_COND(is_u64_vec, (*this xor PrimitiveExpr<T, L>(T(1) << (CHAR_BIT * sizeof(T) - 1))), *this);
            auto _other =
                M_CONSTEXPR_COND(is_u64_vec, (other xor PrimitiveExpr<U, L>(U(1) << (CHAR_BIT * sizeof(U) - 1))), other);
            auto cmp = _this.template binary<bool, ToL, To, L>(op, _other);
            std::array<uint8_t, L> indices;
            for (std::size_t idx = 0; idx < L; ++idx)
                indices[idx] = idx * sizeof(To);
            return M_CONSTEXPR_COND(L == 1 or sizeof(To) == 1, cmp, cmp.swizzle_bytes(indices));
        } else {
            return this->template to<To, L>().operator<=(other.template to<To, L>());
        }
    }

    /** Checks whether `this` greater than to \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator>(PrimitiveExpr<U, L> other) requires arithmetic<T> {
        using To = common_type_t<T, U>;
        constexpr std::size_t ToL = L == 1 ? L : L * sizeof(T);
        auto op = [](){
            if constexpr (L == 1) {
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINARY_OP(Gt, S), BINARY_OP(Gt, U));
            } else {
                if constexpr (std::integral<To>) {
                    if constexpr (sizeof(To) == 8)
                        return BINVOP_(Gt, S, I64x2); // unsigned comparison missing, use signed but flip MSB of operands
                    else if constexpr (sizeof(To) == 4)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Gt, S, I32x4), BINVOP_(Gt, U, I32x4));
                    else if constexpr (sizeof(To) == 2)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Gt, S, I16x8), BINVOP_(Gt, U, I16x8));
                    else if constexpr (sizeof(To) == 1)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Gt, S, I8x16), BINVOP_(Gt, U, I8x16));
                } else if (std::floating_point<To>) {
                    return BINFVOP_(Gt);
                }
            }
            M_unreachable("unsupported operation");
        }();
        if constexpr (L * sizeof(To) <= 16) {
            /* Wasm does not support comparison of U64 SIMD vectors. Thus, flip the MSB in each lane (which
             * basically shifts the unsigned domain into the signed one) and use the signed comparison afterwards. */
            constexpr bool is_u64_vec = unsigned_integral<To> and sizeof(To) == 8 and L > 1;
            auto _this =
                M_CONSTEXPR_COND(is_u64_vec, (*this xor PrimitiveExpr<T, L>(T(1) << (CHAR_BIT * sizeof(T) - 1))), *this);
            auto _other =
                M_CONSTEXPR_COND(is_u64_vec, (other xor PrimitiveExpr<U, L>(U(1) << (CHAR_BIT * sizeof(U) - 1))), other);
            auto cmp = _this.template binary<bool, ToL, To, L>(op, _other);
            std::array<uint8_t, L> indices;
            for (std::size_t idx = 0; idx < L; ++idx)
                indices[idx] = idx * sizeof(To);
            return M_CONSTEXPR_COND(L == 1 or sizeof(To) == 1, cmp, cmp.swizzle_bytes(indices));
        } else {
            return this->template to<To, L>().operator>(other.template to<To, L>());
        }
    }

    /** Checks whether `this` greater than or equals to \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator>=(PrimitiveExpr<U, L> other) requires arithmetic<T> {
        using To = common_type_t<T, U>;
        constexpr std::size_t ToL = L == 1 ? L : L * sizeof(T);
        auto op = [](){
            if constexpr (L == 1) {
                return M_CONSTEXPR_COND(std::is_signed_v<To>, BINARY_OP(Ge, S), BINARY_OP(Ge, U));
            } else {
                if constexpr (std::integral<To>) {
                    if constexpr (sizeof(To) == 8)
                        return BINVOP_(Ge, S, I64x2); // unsigned comparison missing, use signed but flip MSB of operands
                    else if constexpr (sizeof(To) == 4)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Ge, S, I32x4), BINVOP_(Ge, U, I32x4));
                    else if constexpr (sizeof(To) == 2)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Ge, S, I16x8), BINVOP_(Ge, U, I16x8));
                    else if constexpr (sizeof(To) == 1)
                        return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Ge, S, I8x16), BINVOP_(Ge, U, I8x16));
                } else if (std::floating_point<To>) {
                    return BINFVOP_(Ge);
                }
            }
            M_unreachable("unsupported operation");
        }();
        if constexpr (L * sizeof(To) <= 16) {
            /* Wasm does not support comparison of U64 SIMD vectors. Thus, flip the MSB in each lane (which
             * basically shifts the unsigned domain into the signed one) and use the signed comparison afterwards. */
            constexpr bool is_u64_vec = unsigned_integral<To> and sizeof(To) == 8 and L > 1;
            auto _this =
                M_CONSTEXPR_COND(is_u64_vec, (*this xor PrimitiveExpr<T, L>(T(1) << (CHAR_BIT * sizeof(T) - 1))), *this);
            auto _other =
                M_CONSTEXPR_COND(is_u64_vec, (other xor PrimitiveExpr<U, L>(U(1) << (CHAR_BIT * sizeof(U) - 1))), other);
            auto cmp = _this.template binary<bool, ToL, To, L>(op, _other);
            std::array<uint8_t, L> indices;
            for (std::size_t idx = 0; idx < L; ++idx)
                indices[idx] = idx * sizeof(To);
            return M_CONSTEXPR_COND(L == 1 or sizeof(To) == 1, cmp, cmp.swizzle_bytes(indices));
        } else {
            return this->template to<To, L>().operator>=(other.template to<To, L>());
        }
    }

    /*----- Logical operations ---------------------------------------------------------------------------------------*/

    /** Computes the logical conjunction (`and`) of `this` and \p other. */
    template<boolean U>
    PrimitiveExpr<bool, L> operator and(PrimitiveExpr<U, L> other) requires boolean<T> {
        return binary<T, L, T, L>(M_CONSTEXPR_COND(L == 1, BINOP_(And,,Int32), BINVOP_(And,, 128)), other);
    }

    /** Computes the logical conjunction (`and`) of `this` and the logical negation (`not`) of \p other. */
    template<boolean U>
    PrimitiveExpr<bool, L> and_not(PrimitiveExpr<U, L> other) requires boolean<T> and (L > 1) {
        return binary<T, L, T, L>(BINVOP_(AndNot,, 128), other);
    }

    /** Computes the logical disjunction (`or`) of `this` and \p other. */
    template<boolean U>
    PrimitiveExpr<bool, L> operator or(PrimitiveExpr<U, L> other) requires boolean<T> {
        return binary<T, L, T, L>(M_CONSTEXPR_COND(L == 1, BINOP_(Or,,Int32), BINVOP_(Or,, 128)), other);
    }

#undef BINARY_VOP
#undef BINFVOP_
#undef BINIVOP_
#undef BINVOP_
#undef BINARY_OP
#undef BINFOP_
#undef BINIOP_
#undef BINOP_


    /*------------------------------------------------------------------------------------------------------------------
     * Modifications
     *----------------------------------------------------------------------------------------------------------------*/

    private:
    /** Extracts the \tparam M -th value of the underlying 128 bit vector of `this`.  Special care must be taken as
     * this method *must* not be called on scalar expressions and the extracted value might not be one of the first
     * (defined) `L` values. */
    template<std::size_t M>
    requires (M * sizeof(T) < 16)
    PrimitiveExpr<T, 1> extract_unsafe() {
        auto op = [](){
            if constexpr (std::integral<T>) {
                if constexpr (sizeof(T) == 8)
                    return ::wasm::SIMDExtractOp::ExtractLaneVecI64x2;
                else if constexpr (sizeof(T) == 4)
                    return ::wasm::SIMDExtractOp::ExtractLaneVecI32x4;
                else if constexpr (sizeof(T) == 2)
                    return M_CONSTEXPR_COND(std::is_signed_v<T>, ::wasm::SIMDExtractOp::ExtractLaneSVecI16x8,
                                                                 ::wasm::SIMDExtractOp::ExtractLaneUVecI16x8);
                else if constexpr (sizeof(T) == 1)
                    return M_CONSTEXPR_COND(std::is_signed_v<T>, ::wasm::SIMDExtractOp::ExtractLaneSVecI8x16,
                                                                 ::wasm::SIMDExtractOp::ExtractLaneUVecI8x16);
            } else if (std::floating_point<T>) {
                if constexpr (sizeof(T) == 8)
                    return ::wasm::SIMDExtractOp::ExtractLaneVecF64x2;
                else if constexpr (sizeof(T) == 4)
                    return ::wasm::SIMDExtractOp::ExtractLaneVecF32x4;
            }
            M_unreachable("unsupported operation");
        }();
        auto extracted = PrimitiveExpr<T, 1>(
            /* expr=            */ Module::Builder().makeSIMDExtract(op, expr(), M),
            /* referenced_bits= */ referenced_bits() // moved
        );
        return M_CONSTEXPR_COND(boolean<T>, extracted != false, extracted);
    }
    public:
    /** Extracts the \tparam M -th value of `this`. */
    template<std::size_t M>
    requires (M < L)
    PrimitiveExpr<T, 1> extract() requires (L > 1) { return extract_unsafe<M>(); }

    /** Replaces the \tparam M -th value of `this` with \p value. */
    template<std::size_t M, primitive_convertible U>
    requires (M < L) and
    requires (primitive_expr_t<U> u) { PrimitiveExpr<T, 1>(u); }
    PrimitiveExpr replace(U &&_value) requires (L > 1) {
        auto op = [](){
            if constexpr (std::integral<T>) {
                if constexpr (sizeof(T) == 8)
                    return ::wasm::SIMDReplaceOp::ReplaceLaneVecI64x2;
                else if constexpr (sizeof(T) == 4)
                    return ::wasm::SIMDReplaceOp::ReplaceLaneVecI32x4;
                else if constexpr (sizeof(T) == 2)
                    return ::wasm::SIMDReplaceOp::ReplaceLaneVecI16x8;
                else if constexpr (sizeof(T) == 1)
                    return ::wasm::SIMDReplaceOp::ReplaceLaneVecI8x16;
            } else if (std::floating_point<T>) {
                if constexpr (sizeof(T) == 8)
                    return ::wasm::SIMDReplaceOp::ReplaceLaneVecF64x2;
                else if constexpr (sizeof(T) == 4)
                    return ::wasm::SIMDReplaceOp::ReplaceLaneVecF32x4;
            }
            M_unreachable("unsupported operation");
        }();
        PrimitiveExpr<T, 1> value(primitive_expr_t<U>(std::forward<U>(_value)));
        auto replacement =
            M_CONSTEXPR_COND(boolean<T>, (PrimitiveExpr<T, 1>((-value.template to<uint8_t, 1>()).move())), value);
        auto referenced_bits = this->referenced_bits(); // moved
        referenced_bits.splice(referenced_bits.end(), replacement.referenced_bits());
        return PrimitiveExpr(
            /* expr=            */ Module::Builder().makeSIMDReplace(op, this->expr(), M, replacement.expr()),
            /* referenced_bits= */ std::move(referenced_bits)
        );
    }

    /** Selects lanes of `this` in byte granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, 15] select the `i`-th` lane, indices outside of this range result in the value 0. */
    PrimitiveExpr swizzle_bytes(PrimitiveExpr<uint8_t, 16> indices) requires (L > 1) {
        auto referenced_bits = this->referenced_bits(); // moved
        referenced_bits.splice(referenced_bits.end(), indices.referenced_bits());
        return PrimitiveExpr(
            /* expr=            */ Module::Builder().makeBinary(::wasm::BinaryOp::SwizzleVecI8x16,
                                                                this->expr(),
                                                                indices.expr()),
            /* referenced_bits= */ std::move(referenced_bits)
        );
    }
    /** Selects lanes of `this` in byte granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, L * sizeof(T)) select the `i`-th` lane, indices outside of this range result in the value 0. */
    template<std::size_t M>
    requires (M > 0) and (M <= 16) and (M % sizeof(T) == 0)
    PrimitiveExpr<T, M / sizeof(T)> swizzle_bytes(const std::array<uint8_t, M> &_indices) requires (L > 1) {
        std::array<uint8_t, 16> indices;
        for (std::size_t idx = 0; idx < M; ++idx)
            indices[idx] = _indices[idx] < L * sizeof(T) ? _indices[idx] : 16;
        std::fill(indices.begin() + M, indices.end(), 16);
        PrimitiveExpr<uint8_t, 16> indices_expr(
            /* expr= */ Module::Builder().makeConst(::wasm::Literal(indices.data()))
        );
        auto vec = PrimitiveExpr<T, M / sizeof(T)>(swizzle_bytes(indices_expr).move());
        return M_CONSTEXPR_COND(decltype(vec)::num_simd_lanes == 1,
                                vec.template extract_unsafe<0>(), // extract a single value from vector to scalar
                                vec);
    }

    /** Selects lanes of `this` in lane granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, L) select the `i`-th` lane, indices outside of this range result in the value 0. */
    template<std::size_t M>
    requires (M > 0) and (is_pow_2(M)) and (M * sizeof(T) <= 16)
    PrimitiveExpr<T, M> swizzle_lanes(const std::array<uint8_t, M> &_indices) requires (L > 1) {
        std::array<uint8_t, 16> indices;
        for (std::size_t idx = 0; idx < M; ++idx) {
            for (std::size_t byte = 0; byte < sizeof(T); ++byte)
                indices[idx * sizeof(T) + byte] = _indices[idx] < L ? _indices[idx] * sizeof(T) + byte : 16;
        }
        std::fill(indices.begin() + M * sizeof(T), indices.end(), 16);
        PrimitiveExpr<uint8_t, 16> indices_expr(
            /* expr= */ Module::Builder().makeConst(::wasm::Literal(indices.data()))
        );
        auto vec = PrimitiveExpr<T, M>(swizzle_bytes(indices_expr).move());
        return M_CONSTEXPR_COND(decltype(vec)::num_simd_lanes == 1,
                                vec.template extract_unsafe<0>(), // extract a single value from vector to scalar
                                vec);
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Printing
     *----------------------------------------------------------------------------------------------------------------*/

    friend std::ostream & operator<<(std::ostream &out, const PrimitiveExpr &P) {
        out << "PrimitiveExpr<" << typeid(type).name() << "," << num_simd_lanes << ">: ";
        if (P.expr_) out << *P.expr_;
        else         out << "None";
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

/** Specialization of `PrimitiveExpr<T, L>` for primitive type \tparam T and vectorial values not fitting in a single
 * SIMD vector, i.e. (multiple) double pumping must be used.  Represents an expression (AST) evaluating to \tparam L
 * runtime values of primitive type \tparam T. */
template<dsl_primitive T, std::size_t L>
requires (L > 1) and (is_pow_2(L)) and (L * sizeof(T) > 16) and ((L * sizeof(T)) % 16 == 0)
struct PrimitiveExpr<T, L>
{
    ///> the primitive type of the represented expression
    using type = T;
    ///> the number of SIMD lanes of the represented expression
    static constexpr std::size_t num_simd_lanes = L;
    ///> the type of a single fully utilized vector
    using vector_type = PrimitiveExpr<T, 16 / sizeof(T)>;
    ///> the number of SIMD vectors needed for the represented expression
    static constexpr std::size_t num_vectors = (L * sizeof(T)) / 16;
    static_assert(num_vectors >= 2);

    /*----- Friends --------------------------------------------------------------------------------------------------*/
    template<typename, std::size_t> friend struct PrimitiveExpr; // to convert U to T and U* to uint32_t
    template<typename, std::size_t>
    friend struct Expr; // to construct an empty `PrimitiveExpr<bool>` for the NULL information
    template<typename, VariableKind, bool, std::size_t>
    friend class detail::variable_storage; // to construct from `::wasm::Expression` and access private `expr()`
    friend struct Module; // to access internal `::wasm::Expression`, e.g. in `emit_return()`
    friend struct Block; // to access internal `::wasm::Expression`, e.g. in `go_to()`
    template<typename> friend struct FunctionProxy; // to access internal `::wasm::Expr` to construct function calls
    template<typename> friend struct invoke_interpreter; // to access private `expr()`

    private:
    ///> the fully utilized SIMD vectors represented as `PrimitiveExpr`s
    std::array<vector_type, num_vectors> vectors_;

    private:
    ///> Constructs an empty `PrimitiveExpr`, for which `operator bool()` returns `false`.
    explicit PrimitiveExpr() { vectors_.fill(vector_type()); }

    ///> Constructs a `PrimitiveExpr` from an array of fully utilized `PrimitiveExpr`s \p vectors.
    explicit PrimitiveExpr(std::array<vector_type, num_vectors> vectors) : vectors_(std::move(vectors)) { }
    ///> Constructs a `PrimitiveExpr` from an initializer list of fully utilized `PrimitiveExpr`s \p vectors.
    explicit PrimitiveExpr(std::initializer_list<vector_type> vectors) : vectors_(std::move(vectors)) { }

    public:
    /** Constructs a new `PrimitiveExpr` from a constant \p value. */
    template<dsl_primitive... Us>
    requires (sizeof...(Us) > 0) and
    requires (Us... us) { { make_literal<T, L>(us...) } -> std::same_as<std::array<::wasm::Literal, num_vectors>>; }
    explicit PrimitiveExpr(Us... value)
        : PrimitiveExpr([&](){
            std::array<vector_type, num_vectors> vectors;
            auto it = vectors.begin();
            for (auto literal : make_literal<T, L>(value...))
                *(it++) = vector_type(Module::Builder().makeConst(literal));
            M_insist(it == vectors.end());
            return std::move(vectors);
        }())
    { }

    /** Constructs a new `PrimitiveExpr` from a decayable constant \p value. */
    template<decayable... Us>
    requires (sizeof...(Us) > 0) and (dsl_primitive<std::decay_t<Us>> and ...) and
    requires (Us... us) { PrimitiveExpr(std::decay_t<Us>(us)...); }
    explicit PrimitiveExpr(Us... value)
        : PrimitiveExpr(std::decay_t<Us>(value)...)
    { }

    PrimitiveExpr(const PrimitiveExpr&) = delete;
    /** Constructs a new `PrimitiveExpr` by **moving** the underlying `vectors_` of `other` to `this`. */
    PrimitiveExpr(PrimitiveExpr &other) : PrimitiveExpr(std::move(other.vectors_)) { /* move, not copy */ }
    /** Constructs a new `PrimitiveExpr` by **moving** the underlying `vectors_` of `other` to `this`. */
    PrimitiveExpr(PrimitiveExpr &&other) : PrimitiveExpr(std::move(other.vectors_)) { }

    PrimitiveExpr & operator=(PrimitiveExpr&&) = delete;

    ~PrimitiveExpr() = default;

    private:
    /** **Moves** the underlying `PrimitiveExpr<T, 16 / sizeof(T)>`s out of `this`. */
    std::array<vector_type, num_vectors> vectors() { return std::move(vectors_); }
    /** **Moves** the underlying vectors as `PrimitiveExpr<U, 16 / sizeof(U)>` out of `this`. */
    template<dsl_primitive U, std::size_t M>
    requires ((M * sizeof(U)) / 16 == num_vectors)
    auto move() {
        using ToVecT = PrimitiveExpr<U, 16 / sizeof(U)>;
        std::array<ToVecT, num_vectors> vectors;
        for (std::size_t idx = 0; idx < num_vectors; ++idx)
            vectors[idx] = ToVecT(vectors_[idx].move());
        return std::move(vectors);
    }

    public:
    /** Returns `true` if this `PrimitiveExpr` actually holds a value (Binaryen AST), `false` otherwise. Can be used to
     * test whether this `PrimitiveExpr` has already been used. */
    explicit operator bool() const {
        return std::all_of(vectors_.cbegin(), vectors_.cend(), [](const auto &expr){ return bool(expr); });
    }

    /** Creates and returns a *deep copy* of `this`. */
    PrimitiveExpr clone() const {
        M_insist(bool(*this), "cannot clone an already moved or discarded `PrimitiveExpr`");
        std::array<vector_type, num_vectors> vectors_cpy;
        for (std::size_t idx = 0; idx < num_vectors; ++idx)
            vectors_cpy[idx] = vectors_[idx].clone();
        return PrimitiveExpr(
            /* vectors= */ std::move(vectors_cpy)
        );
    }

    /** Discards `this`.  This is necessary to signal in our DSL that a value is *expectedly* unused (and not dead
     * code). For example, the return value of a function that was invoked because of its side effects may remain
     * unused.  One **must** discard the returned value to signal that the value is expectedly left unused. */
    void discard() {
        M_insist(bool(*this), "cannot discard an already moved or discarded `PrimitiveExpr`");
        std::for_each(vectors_.begin(), vectors_.end(), [](auto &expr){ expr.discard(); });
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Conversion operations
     *----------------------------------------------------------------------------------------------------------------*/

    private:
    template<dsl_primitive U, std::size_t M>
    requires ((M * sizeof(U)) % 16 == 0)
    PrimitiveExpr<U, M> convert() {
        using From = T;
        using To = U;
        constexpr std::size_t FromL = L;
        constexpr std::size_t ToL = M;
        using FromVecT = PrimitiveExpr<T, 16 / sizeof(T)>;
        using ToVecT = PrimitiveExpr<U, 16 / sizeof(U)>;
        constexpr std::size_t FromVecL = (L * sizeof(T)) / 16;
        constexpr std::size_t ToVecL = (M * sizeof(U)) / 16;

        if constexpr (std::same_as<From, To> and FromL == ToL)
            return *this;
        if constexpr (integral<From> and integral<To> and std::is_signed_v<From> == std::is_signed_v<To> and
                      sizeof(From) == sizeof(To) and FromL == ToL)
            return PrimitiveExpr<To, ToL>(move<To, ToL>());

        if constexpr (boolean<From>) {                                                                  // from boolean
            if constexpr (integral<To>) {                                                               //  to integer
                if constexpr (FromL == ToL) {                                                           //   vectorial
                    if constexpr (sizeof(To) == 1)                                                      //    bool -> i8/u8
                        return -PrimitiveExpr<To, ToL>(move<To, ToL>()); // negate to convert 0xff to 1
                    if constexpr (std::is_signed_v<To>) {
                        if constexpr (sizeof(To) == 2)                                                  //    bool -> i16
                            return to<int8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(To) == 4)                                                  //    bool -> i32
                            return to<int8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(To) == 8)                                                  //    bool -> i64
                            return to<int8_t, ToL>().template to<To, ToL>();
                    } else {
                        if constexpr (sizeof(To) == 2)                                                  //    bool -> u16
                            return to<uint8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(To) == 4)                                                  //    bool -> u32
                            return to<uint8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(To) == 8)                                                  //    bool -> u64
                            return to<uint8_t, ToL>().template to<To, ToL>();
                    }
                }
            }
            if constexpr (std::floating_point<To>) {                                                    //  to floating point
                if constexpr (FromL == ToL) {                                                           //   vectorial
                    if constexpr (sizeof(To) == 4)                                                      //    bool -> f32
                        return to<uint32_t, ToL>().template convert<To, ToL>();
                    if constexpr (sizeof(To) == 8)                                                      //    bool -> f64
                        return to<uint32_t, ToL>().template convert<To, ToL>();
                }
            }
        }

        if constexpr (boolean<To>) {                                                                    // to boolean
            if constexpr (integral<From>) {                                                             //  from integer
                if constexpr (FromL == ToL) {                                                           //   vectorial
                    if constexpr (sizeof(From) == 1)                                                    //    i8/u8 -> bool
                        return *this != PrimitiveExpr(static_cast<From>(0));
                    if constexpr (std::is_signed_v<From>) {
                        if constexpr (sizeof(From) == 2)                                                //    i16 -> bool
                            return to<int8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and FromVecL >= 4)                              //    i32 -> bool
                            return to<int8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and FromVecL >= 8)                              //    i64 -> bool
                            return to<int8_t, ToL>().template to<To, ToL>();
                    } else {
                        if constexpr (sizeof(From) == 2)                                                //    u16 -> bool
                            return to<uint8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and FromVecL >= 4)                              //    u32 -> bool
                            return to<uint8_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and FromVecL >= 8)                              //    u64 -> bool
                            return to<uint8_t, ToL>().template to<To, ToL>();
                    }
                }
            }
            if constexpr (std::floating_point<From>) {                                                  //  from floating point
                if constexpr (FromL == ToL) {                                                           //   vectorial
                    if constexpr (sizeof(From) == 4 and FromVecL >= 4)                                  //    f32 -> bool
                        return *this != PrimitiveExpr(static_cast<From>(0));
                    if constexpr (sizeof(From) == 8 and FromVecL >= 8)                                  //    f64 -> bool
                        return *this != PrimitiveExpr(static_cast<From>(0));
                }
            }
        }

        if constexpr (integral<From>) {                                                                 // from integer
            if constexpr (integral<To>) {                                                               //  to integer
                if constexpr (FromL == ToL) {                                                           //   vectorial
                    if constexpr (std::is_signed_v<From>) {                                             //    signed
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 2) {                          //     i8 -> i16
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                                vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendLowSVecI8x16ToVecI16x8
                                );
                                vectors[2 * idx + 1] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendHighSVecI8x16ToVecI16x8
                                );
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 1) {                          //     i16 -> i8
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx)
                                vectors[idx] = vectors_[2 * idx].template binary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::NarrowSVecI16x8ToVecI8x16, vectors_[2 * idx + 1]
                                );
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 4)                            //     i8 -> i32
                            return to<int16_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 1 and FromVecL >= 4)          //     i32 -> i8
                            return to<int16_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 8)                            //     i8 -> i64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 1 and FromVecL >= 8)          //     i64 -> i8
                            return to<int16_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4) {                          //     i16 -> i32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                                vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendLowSVecI16x8ToVecI32x4
                                );
                                vectors[2 * idx + 1] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendHighSVecI16x8ToVecI32x4
                                );
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 2) {                          //     i32 -> i16
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx)
                                vectors[idx] = vectors_[2 * idx].template binary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::NarrowSVecI32x4ToVecI16x8, vectors_[2 * idx + 1]
                                );
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 8)                            //     i16 -> i64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 2 and FromVecL >= 4)          //     i64 -> i16
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8) {                          //     i32 -> i64
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                                vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendLowSVecI32x4ToVecI64x2
                                );
                                vectors[2 * idx + 1] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendHighSVecI32x4ToVecI64x2
                                );
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4) {                          //     i64 -> i32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx) {
                                std::array<uint8_t, 16> indices =
                                    { 0, 1, 2, 3, 8, 9 , 10, 11, 16, 17, 18, 19, 24, 25, 26, 27 };
                                vectors[idx] =
                                    ToVecT(ShuffleBytes(vectors_[2 * idx], vectors_[2 * idx + 1], indices).move());
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 2) {                          //     u8 -> u16
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                                vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendLowUVecI8x16ToVecI16x8
                                );
                                vectors[2 * idx + 1] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendHighUVecI8x16ToVecI16x8
                                );
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 1) {                          //     u16 -> u8
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx)
                                vectors[idx] = vectors_[2 * idx].template binary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::NarrowSVecI16x8ToVecI8x16, vectors_[2 * idx + 1]
                                );
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 4)                            //     u8 -> u32
                            return to<uint16_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 1 and FromVecL >= 4)          //     u32 -> u8
                            return to<uint16_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 8)                            //     u8 -> u64
                            return to<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 1 and FromVecL >= 8)          //     u64 -> u8
                            return to<uint16_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4) {                          //     u16 -> u32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                                vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendLowUVecI16x8ToVecI32x4
                                );
                                vectors[2 * idx + 1] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendHighUVecI16x8ToVecI32x4
                                );
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 2) {                          //     u32 -> u16
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx)
                                vectors[idx] = vectors_[2 * idx].template binary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::NarrowSVecI32x4ToVecI16x8, vectors_[2 * idx + 1]
                                );
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 8)                            //     u16 -> u64
                            return to<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 2 and FromVecL >= 4)          //     u64 -> u16
                            return to<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8) {                          //     u32 -> u64
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                                vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendLowUVecI32x4ToVecI64x2
                                );
                                vectors[2 * idx + 1] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ExtendHighUVecI32x4ToVecI64x2
                                );
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4) {                          //     u64 -> u32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx) {
                                std::array<uint8_t, 16> indices =
                                    { 0, 1, 2, 3, 8, 9 , 10, 11, 16, 17, 18, 19, 24, 25, 26, 27 };
                                vectors[idx] =
                                    ToVecT(ShuffleBytes(vectors_[2 * idx], vectors_[2 * idx + 1], indices).move());
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                    }
                }
            }
            if constexpr (std::floating_point<To>) {                                                    //  to floating point
                if constexpr (FromL == ToL) {                                                           //   vectorial
                    if constexpr (std::is_signed_v<From>) {                                             //    signed
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 4)                            //     i8 -> f32
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 8)                            //     i8 -> f64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4)                            //     i16 -> f32
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 8)                            //     i16 -> f64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 4) {                          //     i32 -> f32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx)
                                vectors[idx] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ConvertSVecI32x4ToVecF32x4
                                );
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8) {                          //     i32 -> f64
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                                vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ConvertLowSVecI32x4ToVecF64x2
                                );
                                auto high_to_low = vectors_[idx].swizzle_lanes(std::to_array<uint8_t>({ 2, 3 }));
                                vectors[2 * idx + 1] = high_to_low.template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ConvertLowSVecI32x4ToVecF64x2
                                );
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                            //     i64 -> f32
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     i64 -> f64
                            return to<int32_t, ToL>().template to<To, ToL>();
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 4)                            //     u8 -> f32
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 1 and sizeof(To) == 8)                            //     u8 -> f64
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 4)                            //     u16 -> f32
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 2 and sizeof(To) == 8)                            //     u16 -> f64
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 4) {                          //     u32 -> f32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx)
                                vectors[idx] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ConvertUVecI32x4ToVecF32x4
                                );
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8) {                          //     u32 -> f64
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                                vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ConvertLowUVecI32x4ToVecF64x2
                                );
                                auto high_to_low = vectors_[idx].swizzle_lanes(std::to_array<uint8_t>({ 2, 3 }));
                                vectors[2 * idx + 1] = high_to_low.template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::ConvertLowUVecI32x4ToVecF64x2
                                );
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4)                            //     u64 -> f32
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     u64 -> f64
                            return to<uint32_t, ToL>().template convert<To, ToL>();
                    }
                }
            }
        }

        if constexpr (std::floating_point<From>) {                                                      // from floating point
            if constexpr (integral<To>) {                                                               //  to integer
                if constexpr (FromL == ToL) {                                                           //   vectorial
                    if constexpr (std::is_signed_v<To>) {                                               //    signed
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 1 and FromVecL >= 4)          //     f32 -> i8
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 1 and FromVecL >= 8)          //     f64 -> i8
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 2)                            //     f32 -> i16
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 2 and FromVecL >= 4)          //     f64 -> i16
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 4) {                          //     f32 -> i32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx)
                                vectors[idx] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::TruncSatSVecF32x4ToVecI32x4
                                );
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4) {                          //     f64 -> i32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx) {
                                auto low  = vectors_[2 * idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::TruncSatZeroSVecF64x2ToVecI32x4
                                );
                                auto high = vectors_[2 * idx + 1].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::TruncSatZeroSVecF64x2ToVecI32x4
                                );
                                vectors[idx] = ShuffleLanes(low, high, std::to_array<uint8_t>({ 0, 1, 4, 5 }));
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8)                            //     f32 -> i64
                            return to<int32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     f64 -> i64
                            return to<int32_t, ToL>().template to<To, ToL>();
                    } else {                                                                            //    unsigned
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 1 and FromVecL >= 4)          //     f32 -> u8
                            return convert<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 1 and FromVecL >= 8)          //     f64 -> u8
                            return convert<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 2)                            //     f32 -> u16
                            return convert<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 2 and FromVecL >= 4)          //     f64 -> u16
                            return convert<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 4) {                          //     f32 -> u32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx)
                                vectors[idx] = vectors_[idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::TruncSatUVecF32x4ToVecI32x4
                                );
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 4) {                          //     f64 -> u32
                            std::array<ToVecT, ToVecL> vectors;
                            for (std::size_t idx = 0; idx < ToVecL; ++idx) {
                                auto low  = vectors_[2 * idx].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::TruncSatZeroUVecF64x2ToVecI32x4
                                );
                                auto high = vectors_[2 * idx + 1].template unary<To, ToVecT::num_simd_lanes>(
                                    ::wasm::TruncSatZeroUVecF64x2ToVecI32x4
                                );
                                vectors[idx] = ShuffleLanes(low, high, std::to_array<uint8_t>({ 0, 1, 4, 5 }));
                            }
                            return PrimitiveExpr<To, ToL>(std::move(vectors));
                        }
                        if constexpr (sizeof(From) == 4 and sizeof(To) == 8)                            //     f32 -> u64
                            return convert<uint32_t, ToL>().template to<To, ToL>();
                        if constexpr (sizeof(From) == 8 and sizeof(To) == 8)                            //     f64 -> u64
                            return convert<uint32_t, ToL>().template to<To, ToL>();
                    }
                }
            }
            if constexpr (std::floating_point<To>) {                                                    // to floating point
                if constexpr (FromL == ToL) {                                                           //  vectorial
                    if constexpr (sizeof(From) == 4 and sizeof(To) == 8) {                              //    f32 -> f64
                        std::array<ToVecT, ToVecL> vectors;
                        for (std::size_t idx = 0; idx < FromVecL; ++idx) {
                            vectors[2 * idx] = vectors_[idx].clone().template unary<To, ToVecT::num_simd_lanes>(
                                ::wasm::PromoteLowVecF32x4ToVecF64x2
                            );
                            auto high_to_low = vectors_[idx].swizzle_lanes(std::to_array<uint8_t>({ 2, 3 }));
                            vectors[2 * idx + 1] = high_to_low.template unary<To, ToVecT::num_simd_lanes>(
                                ::wasm::PromoteLowVecF32x4ToVecF64x2
                            );
                        }
                        return PrimitiveExpr<To, ToL>(std::move(vectors));
                    }
                    if constexpr (sizeof(From) == 8 and sizeof(To) == 4) {                              //    f64 -> f32
                        std::array<ToVecT, ToVecL> vectors;
                        for (std::size_t idx = 0; idx < ToVecL; ++idx) {
                            auto low  = vectors_[2 * idx].template unary<To, ToVecT::num_simd_lanes>(
                                ::wasm::DemoteZeroVecF64x2ToVecF32x4
                            );
                            auto high = vectors_[2 * idx + 1].template unary<To, ToVecT::num_simd_lanes>(
                                ::wasm::DemoteZeroVecF64x2ToVecF32x4
                            );
                            vectors[idx] = ShuffleLanes(low, high, std::to_array<uint8_t>({ 0, 1, 4, 5 }));
                        }
                        return PrimitiveExpr<To, ToL>(std::move(vectors));
                    }
                }
            }
        }

        M_unreachable("illegal conversion");
    }

    public:
    /** Implicit conversion of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<To, ToL>`.  Only applicable if
     *
     * - `L` and `ToL` are equal, i.e. conversion does not change the number of SIMD lanes
     * - `T` and `To` have same signedness
     * - neither or both `T` and `To` are integers
     * - `T` can be *trivially* converted to `To` (e.g. `int` to `long` but not `long` to `int`)
     * - `To` is not `bool`
     */
    template<dsl_primitive To, std::size_t ToL = L>
    requires (L == ToL) and                     // L and ToL are equal
             same_signedness<T, To> and         // T and To have same signedness
             (integral<T> == integral<To>) and  // neither nor both T and To are integers (excluding bool)
             (sizeof(T) <= sizeof(To))          // T can be *trivially* converted to To
    operator PrimitiveExpr<To, ToL>() { return convert<To, ToL>(); }

    /** Explicit conversion of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<To, ToL>`.  Only applicable if
     *
     * - `L` and `ToL` are equal, i.e. conversion does not change the number of SIMD lanes
     * - `T` and `To` have same signedness or `T` is `bool` or `char` or `To` is `bool` or `char
     * - `T` can be converted to `To` (e.g. `int` to `long`, `long` to `int`, `float` to `int`)
     * - narrowing `T` to `To` results in at least one fully utilized SIMD vector
     */
    template<dsl_primitive To, std::size_t ToL = L>
    requires (L == ToL) and                                                         // L and ToL are equal
             (same_signedness<T, To> or                                             // T and To have same signedness
              boolean<T> or std::same_as<T, char> or                                //  or T is bool or char
              boolean<To> or std::same_as<To, char>) and                            //  or To is bool or char
             std::is_convertible_v<T, To> and                                       // T can be converted to To
             (num_vectors >= sizeof(T) / sizeof(To))                                // at least one vector afterwards
    PrimitiveExpr<To, ToL> to() { return convert<To, ToL>(); }

    /** Conversion of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<std::make_signed_t<T>, L>`. Only applicable if
     *
     * - `T` is an unsigned integral type except `bool`
     */
    auto make_signed() requires unsigned_integral<T> {
        return PrimitiveExpr<std::make_signed_t<T>, L>(move<std::make_signed_t<T>, L>());
    }

    /** Conversion of a `PrimitiveExpr<T, L>` to a `PrimitiveExpr<std::make_unsigned_t<T>, L>`. Only available if
    *
    * - `T` is a signed integral type except `bool`
    */
    auto make_unsigned() requires signed_integral<T> {
        return PrimitiveExpr<std::make_unsigned_t<T>, L>(move<std::make_unsigned_t<T>, L>());
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Unary operations
     *----------------------------------------------------------------------------------------------------------------*/

#define UNARY(OP) \
    auto OP() requires requires (vector_type v) { v.OP(); } { \
        using ResVecT = decltype(std::declval<vector_type>().OP()); \
        static_assert(ResVecT::num_simd_lanes * sizeof(typename ResVecT::type) == 16, \
                      "result vectors must be fully utilized"); \
        std::array<ResVecT, num_vectors> vectors; \
        for (std::size_t idx = 0; idx < num_vectors; ++idx) \
            vectors[idx] = vectors_[idx].OP(); \
        return PrimitiveExpr<typename ResVecT::type, ResVecT::num_simd_lanes * num_vectors>(std::move(vectors)); \
    }

    UNARY(operator +)
    UNARY(operator -)
    UNARY(abs)
    UNARY(ceil)
    UNARY(floor)
    UNARY(trunc)
    UNARY(nearest)
    UNARY(sqrt)
    UNARY(add_pairwise)
    UNARY(operator ~)
    UNARY(popcnt)
    UNARY(operator not)
#undef UNARY

    /** Concatenates the most significant bit (or the boolean value if `this` is boolean) of each value of `this`
     * into a single mask. */
    PrimitiveExpr<uint32_t, 1> bitmask() requires (L <= 32) and requires (vector_type v) { v.bitmask(); } {
        std::optional<PrimitiveExpr<uint32_t, 1>> res = vectors_[0].bitmask();
        for (std::size_t idx = 1; idx < num_vectors; ++idx)
            res.emplace((vectors_[idx].bitmask() << uint32_t(idx * vector_type::num_simd_lanes)) bitor *res);
        return *res;
    }
    /** Concatenates the most significant bit (or the boolean value if `this` is boolean) of each value of `this`
     * into a single mask. */
    PrimitiveExpr<uint64_t, 1> bitmask() requires (L > 32) and (L <= 64) and requires (vector_type v) { v.bitmask(); } {
        std::optional<PrimitiveExpr<uint64_t, 1>> res = vectors_[0].bitmask();
        for (std::size_t idx = 1; idx < num_vectors; ++idx)
            res.emplace((vectors_[idx].bitmask() << uint64_t(idx * vector_type::num_simd_lanes)) bitor *res);
        return *res;
    }

    /** Returns `true` iff any value is `true` or rather non-zero. */
    PrimitiveExpr<bool, 1> any_true() requires requires (vector_type v) { v.any_true(); } {
        std::optional<PrimitiveExpr<bool, 1>> res = vectors_[0].any_true();
        for (std::size_t idx = 1; idx < num_vectors; ++idx)
            res.emplace(vectors_[idx].any_true() or *res);
        return *res;
    }

    /** Returns `true` iff all values are `true` or rather non-zero. */
    PrimitiveExpr<bool, 1> all_true() requires requires (vector_type v) { v.all_true(); } {
        std::optional<PrimitiveExpr<bool, 1>> res = vectors_[0].all_true();
        for (std::size_t idx = 1; idx < num_vectors; ++idx)
            res.emplace(vectors_[idx].all_true() and *res);
        return *res;
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Binary operations
     *----------------------------------------------------------------------------------------------------------------*/

#define BINARY(OP) \
    template<dsl_primitive U> \
    requires arithmetically_combinable<T, U, L> and \
    requires (typename PrimitiveExpr<common_type_t<T, U>, L>::vector_type left, \
              typename PrimitiveExpr<common_type_t<T, U>, L>::vector_type right) \
    { left.OP(right); } \
    auto OP(PrimitiveExpr<U, L> other) { \
        using To = common_type_t<T, U>; \
        using OpT = decltype(to<To, L>()); \
        using ResVecT = \
            decltype(std::declval<typename OpT::vector_type>().OP(std::declval<typename OpT::vector_type>())); \
        static_assert(ResVecT::num_simd_lanes * sizeof(typename ResVecT::type) == 16, \
                      "result vectors must be fully utilized"); \
        auto this_converted  = this->template to<To, L>(); \
        auto other_converted = other.template to<To, L>(); \
        std::array<ResVecT, OpT::num_vectors> vectors; \
        for (std::size_t idx = 0; idx < OpT::num_vectors; ++idx) \
            vectors[idx] = this_converted.vectors_[idx].OP(other_converted.vectors_[idx]); \
        return PrimitiveExpr<typename ResVecT::type, ResVecT::num_simd_lanes * OpT::num_vectors>(std::move(vectors)); \
    }

    BINARY(operator +)
    BINARY(operator -)
    BINARY(operator *)
    BINARY(operator /)
    BINARY(min)
    BINARY(max)
    BINARY(avg)
    BINARY(operator bitand)
    BINARY(operator bitor)
    BINARY(operator xor)
    BINARY(operator and)
    BINARY(and_not)
    BINARY(operator or)
#undef BINARY

#define SHIFT(OP) \
    template<dsl_primitive U> \
    PrimitiveExpr OP(PrimitiveExpr<U, 1> other) requires requires (vector_type v) { v.OP(other); } { \
        std::array<vector_type, num_vectors> vectors; \
        for (std::size_t idx = 0; idx < num_vectors; ++idx) \
            vectors[idx] = vectors_[idx].OP(other.clone()); \
        other.discard(); \
        return PrimitiveExpr(std::move(vectors)); \
    }

    SHIFT(operator <<)
    SHIFT(operator >>)
#undef SHIFT

#define BINVOP_(NAME, SIGN, TYPE) (::wasm::BinaryOp::NAME##SIGN##Vec##TYPE)
#define BINIVOP_(NAME, SIGN) [] { \
    if constexpr (sizeof(To) == 8) \
        return BINVOP_(NAME,SIGN,I64x2); \
    else if constexpr (sizeof(To) == 4) \
        return BINVOP_(NAME,SIGN,I32x4); \
    else if constexpr (sizeof(To) == 2) \
        return BINVOP_(NAME,SIGN,I16x8); \
    else if constexpr (sizeof(To) == 1) \
        return BINVOP_(NAME,SIGN,I8x16); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define BINFVOP_(NAME) [] { \
    if constexpr (sizeof(To) == 8) \
        return BINVOP_(NAME,,F64x2); \
    else if constexpr (sizeof(To) == 4) \
        return BINVOP_(NAME,,F32x4); \
    else \
        M_unreachable("unsupported operation"); \
} ()
#define BINARY_VOP(NAME, SIGN) [] { \
    if constexpr (std::integral<To>) \
        return BINIVOP_(NAME, SIGN); \
    else if constexpr (std::floating_point<To>) \
        return BINFVOP_(NAME); \
    else \
        M_unreachable("unsupported operation"); \
} ()

    private:
    /** Transforms a comparison result into its boolean representation. */
    PrimitiveExpr<bool, L> cmp_helper() requires unsigned_integral<T> {
        if constexpr (L > 16) { // enough values present s.t. integer narrowing can be used on multiple vectors
            auto narrowed = to<uint8_t, L>();
            return PrimitiveExpr<bool, L>(narrowed.template move<bool, L>());
        } else if constexpr (L == 16) { // enough values present s.t. integer narrowing can be used on single vector
            auto narrowed = to<uint8_t, L>();
            return PrimitiveExpr<bool, L>(narrowed.move());
        } else if constexpr (num_vectors == 2) { // swizzle bytes of two vectors together
            PrimitiveExpr<bool, L * sizeof(T)> cmp(move<bool, L * sizeof(T)>());
            std::array<uint8_t, L> indices;
            for (std::size_t idx = 0; idx < L; ++idx)
                indices[idx] = idx * sizeof(T);
            return cmp.swizzle_bytes(indices);
        } else { // shuffle bytes of four vectors together
            auto vectors = move<bool, L * sizeof(T)>();
            static_assert(vectors.size() == 4);
            std::array<uint8_t, L / 2> indices;
            for (std::size_t idx = 0; idx < L / 2; ++idx)
                indices[idx]  = idx * sizeof(T);
            auto low  = ShuffleBytes(vectors[0], vectors[1], indices);
            auto high = ShuffleBytes(vectors[2], vectors[3], indices);
            std::array<uint8_t, L> lanes;
            std::iota(lanes.begin(), lanes.end(), 0); // fill with [0, L), i.e. all L/2 lanes of low and high concatenated
            return ShuffleLanes(low, high, lanes);
        }
    }

    public:
    /** Checks whether `this` equals \p other. */
    template<dsl_primitive U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator==(PrimitiveExpr<U, L> other) {
        using To = common_type_t<T, U>;
        using OpT = decltype(to<To, L>());
        static constexpr std::size_t lanes = OpT::vector_type::num_simd_lanes;
        auto this_converted  = this->template to<To, L>();
        auto other_converted = other.template to<To, L>();
        std::array<PrimitiveExpr<uint_t<sizeof(To)>, lanes>, OpT::num_vectors> vectors;
        for (std::size_t idx = 0; idx < OpT::num_vectors; ++idx)
            vectors[idx] = this_converted.vectors_[idx].template binary<uint_t<sizeof(To)>, lanes>(
                BINARY_VOP(Eq,), other_converted.vectors_[idx]
            );
        return PrimitiveExpr<uint_t<sizeof(To)>, L>(std::move(vectors)).cmp_helper();
    }

    /** Checks whether `this` unequal to \p other. */
    template<dsl_primitive U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator!=(PrimitiveExpr<U, L> other) {
        using To = common_type_t<T, U>;
        using OpT = decltype(to<To, L>());
        static constexpr std::size_t lanes = OpT::vector_type::num_simd_lanes;
        auto this_converted  = this->template to<To, L>();
        auto other_converted = other.template to<To, L>();
        std::array<PrimitiveExpr<uint_t<sizeof(To)>, lanes>, OpT::num_vectors> vectors;
        for (std::size_t idx = 0; idx < OpT::num_vectors; ++idx)
            vectors[idx] = this_converted.vectors_[idx].template binary<uint_t<sizeof(To)>, lanes>(
                BINARY_VOP(Ne,), other_converted.vectors_[idx]
            );
        return PrimitiveExpr<uint_t<sizeof(To)>, L>(std::move(vectors)).cmp_helper();
    }

    /** Checks whether `this` less than \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator<(PrimitiveExpr<U, L> other) requires arithmetic<T> {
        using To = common_type_t<T, U>;
        using OpT = decltype(to<To, L>());
        static constexpr std::size_t lanes = OpT::vector_type::num_simd_lanes;
        auto op = [](){
            if constexpr (std::integral<To>) {
                if constexpr (sizeof(To) == 8)
                    return BINVOP_(Lt, S, I64x2); // unsigned comparison missing, use signed but flip MSB of operands
                else if constexpr (sizeof(To) == 4)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Lt, S, I32x4), BINVOP_(Lt, U, I32x4));
                else if constexpr (sizeof(To) == 2)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Lt, S, I16x8), BINVOP_(Lt, U, I16x8));
                else if constexpr (sizeof(To) == 1)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Lt, S, I8x16), BINVOP_(Lt, U, I8x16));
            } else if (std::floating_point<To>) {
                return BINFVOP_(Lt);
            }
            M_unreachable("unsupported operation");
        }();
        /* Wasm does not support comparison of U64 SIMD vectors. Thus, flip the MSB in each lane (which
         * basically shifts the unsigned domain into the signed one) and use the signed comparison afterwards. */
        constexpr bool is_u64_vec = unsigned_integral<To> and sizeof(To) == 8 and L > 1;
        auto _this =
            M_CONSTEXPR_COND(is_u64_vec, (*this xor PrimitiveExpr<T, L>(T(1) << (CHAR_BIT * sizeof(T) - 1))), *this);
        auto _other =
            M_CONSTEXPR_COND(is_u64_vec, (other xor PrimitiveExpr<U, L>(U(1) << (CHAR_BIT * sizeof(U) - 1))), other);
        auto this_converted  = _this.template to<To, L>();
        auto other_converted = _other.template to<To, L>();
        std::array<PrimitiveExpr<uint_t<sizeof(To)>, lanes>, OpT::num_vectors> vectors;
        for (std::size_t idx = 0; idx < OpT::num_vectors; ++idx)
            vectors[idx] = this_converted.vectors_[idx].template binary<uint_t<sizeof(To)>, lanes>(
                op, other_converted.vectors_[idx]
            );
        return PrimitiveExpr<uint_t<sizeof(To)>, L>(std::move(vectors)).cmp_helper();
    }

    /** Checks whether `this` less than or equals to \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator<=(PrimitiveExpr<U, L> other) requires arithmetic<T> {
        using To = common_type_t<T, U>;
        using OpT = decltype(to<To, L>());
        static constexpr std::size_t lanes = OpT::vector_type::num_simd_lanes;
        auto op = [](){
            if constexpr (std::integral<To>) {
                if constexpr (sizeof(To) == 8)
                    return BINVOP_(Le, S, I64x2); // unsigned comparison missing, use signed but flip MSB of operands
                else if constexpr (sizeof(To) == 4)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Le, S, I32x4), BINVOP_(Le, U, I32x4));
                else if constexpr (sizeof(To) == 2)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Le, S, I16x8), BINVOP_(Le, U, I16x8));
                else if constexpr (sizeof(To) == 1)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Le, S, I8x16), BINVOP_(Le, U, I8x16));
            } else if (std::floating_point<To>) {
                return BINFVOP_(Le);
            }
            M_unreachable("unsupported operation");
        }();
        /* Wasm does not support comparison of U64 SIMD vectors. Thus, flip the MSB in each lane (which
         * basically shifts the unsigned domain into the signed one) and use the signed comparison afterwards. */
        constexpr bool is_u64_vec = unsigned_integral<To> and sizeof(To) == 8 and L > 1;
        auto _this =
            M_CONSTEXPR_COND(is_u64_vec, (*this xor PrimitiveExpr<T, L>(T(1) << (CHAR_BIT * sizeof(T) - 1))), *this);
        auto _other =
            M_CONSTEXPR_COND(is_u64_vec, (other xor PrimitiveExpr<U, L>(U(1) << (CHAR_BIT * sizeof(U) - 1))), other);
        auto this_converted  = _this.template to<To, L>();
        auto other_converted = _other.template to<To, L>();
        std::array<PrimitiveExpr<uint_t<sizeof(To)>, lanes>, OpT::num_vectors> vectors;
        for (std::size_t idx = 0; idx < OpT::num_vectors; ++idx)
            vectors[idx] = this_converted.vectors_[idx].template binary<uint_t<sizeof(To)>, lanes>(
                op, other_converted.vectors_[idx]
            );
        return PrimitiveExpr<uint_t<sizeof(To)>, L>(std::move(vectors)).cmp_helper();
    }

    /** Checks whether `this` greater than to \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator>(PrimitiveExpr<U, L> other) requires arithmetic<T> {
        using To = common_type_t<T, U>;
        using OpT = decltype(to<To, L>());
        static constexpr std::size_t lanes = OpT::vector_type::num_simd_lanes;
        auto op = [](){
            if constexpr (std::integral<To>) {
                if constexpr (sizeof(To) == 8)
                    return BINVOP_(Gt, S, I64x2); // unsigned comparison missing, use signed but flip MSB of operands
                else if constexpr (sizeof(To) == 4)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Gt, S, I32x4), BINVOP_(Gt, U, I32x4));
                else if constexpr (sizeof(To) == 2)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Gt, S, I16x8), BINVOP_(Gt, U, I16x8));
                else if constexpr (sizeof(To) == 1)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Gt, S, I8x16), BINVOP_(Gt, U, I8x16));
            } else if (std::floating_point<To>) {
                return BINFVOP_(Gt);
            }
            M_unreachable("unsupported operation");
        }();
        /* Wasm does not support comparison of U64 SIMD vectors. Thus, flip the MSB in each lane (which
         * basically shifts the unsigned domain into the signed one) and use the signed comparison afterwards. */
        constexpr bool is_u64_vec = unsigned_integral<To> and sizeof(To) == 8 and L > 1;
        auto _this =
            M_CONSTEXPR_COND(is_u64_vec, (*this xor PrimitiveExpr<T, L>(T(1) << (CHAR_BIT * sizeof(T) - 1))), *this);
        auto _other =
            M_CONSTEXPR_COND(is_u64_vec, (other xor PrimitiveExpr<U, L>(U(1) << (CHAR_BIT * sizeof(U) - 1))), other);
        auto this_converted  = _this.template to<To, L>();
        auto other_converted = _other.template to<To, L>();
        std::array<PrimitiveExpr<uint_t<sizeof(To)>, lanes>, OpT::num_vectors> vectors;
        for (std::size_t idx = 0; idx < OpT::num_vectors; ++idx)
            vectors[idx] = this_converted.vectors_[idx].template binary<uint_t<sizeof(To)>, lanes>(
                op, other_converted.vectors_[idx]
            );
        return PrimitiveExpr<uint_t<sizeof(To)>, L>(std::move(vectors)).cmp_helper();
    }

    /** Checks whether `this` greater than or equals to \p other. */
    template<arithmetic U>
    requires same_signedness<T, U> and arithmetically_combinable<T, U, L>
    PrimitiveExpr<bool, L> operator>=(PrimitiveExpr<U, L> other) requires arithmetic<T> {
        using To = common_type_t<T, U>;
        using OpT = decltype(to<To, L>());
        static constexpr std::size_t lanes = OpT::vector_type::num_simd_lanes;
        auto op = [](){
            if constexpr (std::integral<To>) {
                if constexpr (sizeof(To) == 8)
                    return BINVOP_(Ge, S, I64x2); // unsigned comparison missing, use signed but flip MSB of operands
                else if constexpr (sizeof(To) == 4)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Ge, S, I32x4), BINVOP_(Ge, U, I32x4));
                else if constexpr (sizeof(To) == 2)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Ge, S, I16x8), BINVOP_(Ge, U, I16x8));
                else if constexpr (sizeof(To) == 1)
                    return M_CONSTEXPR_COND(std::is_signed_v<To>, BINVOP_(Ge, S, I8x16), BINVOP_(Ge, U, I8x16));
            } else if (std::floating_point<To>) {
                return BINFVOP_(Ge);
            }
            M_unreachable("unsupported operation");
        }();
        /* Wasm does not support comparison of U64 SIMD vectors. Thus, flip the MSB in each lane (which
         * basically shifts the unsigned domain into the signed one) and use the signed comparison afterwards. */
        constexpr bool is_u64_vec = unsigned_integral<To> and sizeof(To) == 8 and L > 1;
        auto _this =
            M_CONSTEXPR_COND(is_u64_vec, (*this xor PrimitiveExpr<T, L>(T(1) << (CHAR_BIT * sizeof(T) - 1))), *this);
        auto _other =
            M_CONSTEXPR_COND(is_u64_vec, (other xor PrimitiveExpr<U, L>(U(1) << (CHAR_BIT * sizeof(U) - 1))), other);
        auto this_converted  = _this.template to<To, L>();
        auto other_converted = _other.template to<To, L>();
        std::array<PrimitiveExpr<uint_t<sizeof(To)>, lanes>, OpT::num_vectors> vectors;
        for (std::size_t idx = 0; idx < OpT::num_vectors; ++idx)
            vectors[idx] = this_converted.vectors_[idx].template binary<uint_t<sizeof(To)>, lanes>(
                op, other_converted.vectors_[idx]
            );
        return PrimitiveExpr<uint_t<sizeof(To)>, L>(std::move(vectors)).cmp_helper();
    }

#undef BINARY_VOP
#undef BINFVOP_
#undef BINIVOP_
#undef BINVOP_


    /*------------------------------------------------------------------------------------------------------------------
     * Modifications
     *----------------------------------------------------------------------------------------------------------------*/

    /** Extracts the \tparam M -th value of `this`. */
    template<std::size_t M>
    requires (M < L)
    PrimitiveExpr<T, 1> extract() {
        auto res = vectors_[M / vector_type::num_simd_lanes].clone().template extract<M % vector_type::num_simd_lanes>();
        discard(); // to discard all vectors not used for extraction
        return res;
    }

    /** Replaces the \tparam M -th value of `this` with \p value. */
    template<std::size_t M, primitive_convertible U>
    requires (M < L)
    PrimitiveExpr replace(U &&value) requires requires (vector_type v) { v.replace<0>(std::forward<U>(value)); } {
        static constexpr std::size_t lanes = vector_type::num_simd_lanes;
        vectors_[M / lanes] = vectors_[M / lanes].template replace<M % lanes>(std::forward<U>(value));
        return *this;
    }

    /** Selects lanes of `this` in byte granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, L * sizeof(T)) select the `i`-th` lane, indices outside of this range result in undefined values. */
    template<std::size_t M>
    requires (M > 0) and (M <= 16) and (M % sizeof(T) == 0)
    PrimitiveExpr<T, M / sizeof(T)> swizzle_bytes(const std::array<uint8_t, M> &indices) requires (num_vectors == 2) {
        return Module::Get().emit_shuffle_bytes(vectors_[0], vectors_[1], indices);
    }

    /** Selects lanes of `this` in lane granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, L) select the `i`-th` lane, indices outside of this range result in undefined values. */
    template<std::size_t M>
    requires (M > 0) and (is_pow_2(M)) and (M * sizeof(T) <= 16)
    PrimitiveExpr<T, M> swizzle_lanes(const std::array<uint8_t, M> &indices) requires (num_vectors == 2) {
        return Module::Get().emit_shuffle_lanes(vectors_[0], vectors_[1], indices);
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Printing
     *----------------------------------------------------------------------------------------------------------------*/

    friend std::ostream & operator<<(std::ostream &out, const PrimitiveExpr &P) {
        out << "PrimitiveExpr<" << typeid(T).name() << "," << L << ">: [";
        for (auto it = P.vectors_.cbegin(); it != P.vectors_.cend(); ++it) {
            if (it != P.vectors_.cbegin())
                out << ", ";
            out << *it;
        }
        out << "]";
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};


/*======================================================================================================================
 * Define binary operators on `PrimitiveExpr`
 *====================================================================================================================*/

/** List of supported binary operators on `PrimitiveExpr`, `Expr`, `Variable`, etc. */
#define BINARY_LIST(X) \
    X(operator +) \
    X(operator -) \
    X(operator *) \
    X(operator /) \
    X(operator %) \
    X(operator bitand) \
    X(operator bitor) \
    X(operator xor) \
    X(operator <<) \
    X(operator >>) \
    X(operator ==) \
    X(operator !=) \
    X(operator <) \
    X(operator <=) \
    X(operator >) \
    X(operator >=) \
    X(operator and) \
    X(operator or) \
    X(copy_sign) \
    X(min) \
    X(max) \
    X(avg) \
    X(rotl) \
    X(rotr) \
    X(and_not)

/*----- Forward binary operators on operands convertible to PrimitiveExpr<T, L> --------------------------------------*/
#define MAKE_BINARY(OP) \
    template<primitive_convertible T, primitive_convertible U> \
    requires requires (primitive_expr_t<T> t, primitive_expr_t<U> u) { t.OP(u); } \
    auto OP(T &&t, U &&u) \
    { \
        return primitive_expr_t<T>(std::forward<T>(t)).OP(primitive_expr_t<U>(std::forward<U>(u))); \
    }
BINARY_LIST(MAKE_BINARY)
#undef MAKE_BINARY

/** Specialization of `PrimitiveExpr<T, L>` for pointer to primitive type \tparam T.  Represents an expression (AST)
 * evaluating to \tparam L runtime values of pointer to primitive type \tparam T. */
template<dsl_pointer_to_primitive T, std::size_t L>
requires (L > 0) and (is_pow_2(L)) and
         ((L == 1) or requires { PrimitiveExpr<std::remove_pointer_t<T>, L>(); })
struct PrimitiveExpr<T, L>
{
    using type = T;
    static constexpr std::size_t num_simd_lanes = L;
    using pointed_type = std::decay_t<std::remove_pointer_t<T>>;
    using offset_t = int32_t;

    /*----- Friends --------------------------------------------------------------------------------------------------*/
    template<typename, std::size_t> friend struct PrimitiveExpr; // to convert U* to T* and to convert uint32_t to T*
    template<typename, VariableKind, bool, std::size_t>
    friend class detail::variable_storage; // to construct from `::wasm::Expression` and access private `expr()`
    friend struct Module; // to acces internal ::wasm::Expr
    template<typename> friend struct FunctionProxy; // to access internal `::wasm::Expr` to construct function calls
    template<dsl_primitive, std::size_t, bool> friend struct detail::the_reference; // to access load()/store()
    template<typename> friend struct invoke_interpreter; // to access private `expr()`

    private:
    PrimitiveExpr<uint32_t, 1> addr_; ///< the address into the Wasm linear memory
    offset_t offset_ = 0; ///< offset to this in bytes; used to directly address pointer via base address and offset

    public:
    /** Constructs a `PrimitiveExpr` from the memory address \p addr.  Optionally accepts an \p offset. */
    explicit PrimitiveExpr(PrimitiveExpr<uint32_t, 1> addr, offset_t offset = 0) : addr_(addr), offset_(offset) { }

    private:
    /** Constructs a `PrimitiveExpr` from the given address \p addr and \p referenced_bits.  Optionally accepts an \p
     * offset. */
    explicit PrimitiveExpr(::wasm::Expression *addr, std::list<std::shared_ptr<Bit>> referenced_bits = {},
                           offset_t offset = 0)
        : addr_(addr, std::move(referenced_bits))
        , offset_(offset)
    { }
    /** Constructs a `PrimitiveExpr` from a `std::pair` \p addr of the addres and the shared bits.  Optionally
     * accepts an \p offset. */
    explicit PrimitiveExpr(std::pair<::wasm::Expression*, std::list<std::shared_ptr<Bit>>> addr, offset_t offset = 0)
        : PrimitiveExpr(std::move(addr.first), std::move(addr.second), offset)
    { }

    public:
    PrimitiveExpr(const PrimitiveExpr&) = delete;
    /** Constructs a new `PrimitiveExpr` by **moving** the underlying `expr_`, `referenced_bits`, and `offset_`
     * of `other` to `this`. */
    PrimitiveExpr(PrimitiveExpr &other) : addr_(other.addr_), offset_(other.offset_) { /* move, not copy */ }
    /** Constructs a new `PrimitiveExpr` by **moving** the underlying `expr_`, `referenced_bits`, and `offset_`
     * of `other` to `this`. */
    PrimitiveExpr(PrimitiveExpr &&other) : addr_(other.addr_), offset_(other.offset_) { }

    PrimitiveExpr & operator=(PrimitiveExpr&&) = delete;

    /** Constructs a Wasm `nullptr`.  Note, that in order to implement `nullptr` in Wasm, we must create an artificial
     * address that cannot be accessed. */
    static PrimitiveExpr Nullptr() { return PrimitiveExpr(PrimitiveExpr<uint32_t, 1>(0U)); }

    private:
    /** **Moves** the underlying Binaryen `::wasm::Expression` out of `this`. */
    ::wasm::Expression * expr() { return to<uint32_t>().expr(); }
    /** **Moves** the referenced bits out of `this`. */
    std::list<std::shared_ptr<Bit>> referenced_bits() { return addr_.referenced_bits(); }
    /** **Moves** the underlying Binaryen `wasm::Expression` and the referenced bits out of `this`. */
    std::pair<::wasm::Expression*, std::list<std::shared_ptr<Bit>>> move() { return addr_.move(); }

    public:
    /** Returns `true` if this `PrimitiveExpr` actually holds a value (Binaryen AST), `false` otherwise. Can be used to
     * test whether this `PrimitiveExpr` has already been used. */
    explicit operator bool() const { return bool(addr_); }

    /** Creates and returns a *deep copy* of `this`. */
    PrimitiveExpr clone() const { return PrimitiveExpr(addr_.clone(), offset_); }

    /** Discards `this`.  This is necessary to signal in our DSL that a value is *expectedly* unused (and not dead
     * code). For example, the return value of a function that was invoked because of its side effects may remain
     * unused.  One **must** discard the returned value to signal that the value is expectedly left unused. */
    void discard() { addr_.discard(); }


    /*------------------------------------------------------------------------------------------------------------------
     * Conversion operations
     *----------------------------------------------------------------------------------------------------------------*/

    public:
    /** Explicit conversion of a `PrimitiveExpr<void*, 1>` to a `PrimitiveExpr<To, ToL>`.  Only applicable if \tparam To
     * is a pointer to primitive type. */
    template<dsl_pointer_to_primitive To, std::size_t ToL = L>
    requires (not std::is_void_v<std::remove_pointer_t<To>>)
    PrimitiveExpr<To, ToL> to() requires std::is_void_v<pointed_type> and (L == 1) {
        Wasm_insist((clone().template to<uint32_t>() % uint32_t(alignof(std::remove_pointer_t<To>))).eqz(),
                    "cannot convert to type whose alignment requirement is not fulfilled");
        return PrimitiveExpr<To, ToL>(addr_.move(), offset_);
    }

    /** Explicit conversion of a `PrimitiveExpr<T*, L>` to a `PrimitiveExpr<uint32_t, 1>`.  Adds possible offset to
     * the pointer. */
    template<typename To, std::size_t ToL = 1>
    requires std::same_as<To, uint32_t> and (ToL == 1)
    PrimitiveExpr<uint32_t, 1> to() {
        return offset_ ? (offset_ > 0 ? addr_ + uint32_t(offset_) : addr_ - uint32_t(-offset_)) : addr_;
    }

    /** Explicit conversion of a `PrimitiveExpr<T*, L>` to a `PrimitiveExpr<void*, 1>`. */
    template<typename To, std::size_t ToL = 1>
    requires (not std::same_as<To, T>) and std::same_as<To, void*> and (ToL == 1)
    PrimitiveExpr<void*, 1> to() { return PrimitiveExpr<void*, 1>(addr_.move(), offset_); }

    /** Explicit dummy conversion of a `PrimitiveExpr<T*, L>` to a `PrimitiveExpr<T*, L>`.  Only needed for convenience
     * reasons, i.e. to match behaviour of `PrimitiveExpr<dsl_primitive>`. */
    template<typename To, std::size_t ToL = L>
    requires std::same_as<To, T> and (L == ToL)
    PrimitiveExpr to() { return *this; }


    /*------------------------------------------------------------------------------------------------------------------
     * Hashing operations
     *----------------------------------------------------------------------------------------------------------------*/

    PrimitiveExpr<uint64_t, L> hash() { return to<uint32_t>().hash(); }


    /*------------------------------------------------------------------------------------------------------------------
     * Pointer operations
     *----------------------------------------------------------------------------------------------------------------*/

    public:
    /** Returns `true` if `this` is `nullptr`. */
    PrimitiveExpr<bool, 1> is_nullptr() { return to<uint32_t>() == 0U; }

    /** Returns `true` if `this` is `NULL`, `false` otherwise.  Even if this method performs the same operation
     * as `is_nullptr()` it should be used for ternary logic since it additionally checks whether ternary logic usage
     * is expected. */
    PrimitiveExpr<bool, 1> is_null() {
        M_insist_no_ternary_logic();
        return to<uint32_t>() == 0U;
    }

    /** Returns `true` if `this` is `NOT NULL`, `false` otherwise.  Even if this method performs the same operation
     * as `not is_nullptr()` it should be used for ternary logic since it additionally checks whether ternary logic
     * usage is expected. */
    PrimitiveExpr<bool, 1> not_null() {
        M_insist_no_ternary_logic();
        return to<uint32_t>() != 0U;
    }

    /** Returns a `std::pair` of `this` and a `PrimitiveExpr<bool, 1>` that tells whether `this` is `nullptr`. */
    std::pair<PrimitiveExpr, PrimitiveExpr<bool, 1>> split() { auto cpy = clone(); return { cpy, is_nullptr() }; }

    /** Dereferencing a pointer `PrimitiveExpr<T*, L>` yields a `Reference<T, L>`. */
    auto operator*() requires dsl_primitive<pointed_type> {
        Wasm_insist(not clone().is_nullptr(), "cannot dereference `nullptr`");
        return Reference<pointed_type, L>(*this);
    }

    /** Dereferencing a `const` pointer `PrimitiveExpr<T*, L>` yields a `ConstReference<T, L>`. */
    auto operator*() const requires dsl_primitive<pointed_type> {
        Wasm_insist(not clone().is_nullptr(), "cannot dereference `nullptr`");
        return ConstReference<pointed_type, L>(*this);
    }

    /** Dereferencing and loading a `const` pointer `PrimitiveExpr<T*, L>` yields a `PrimitiveExpr<T, L>`. */
    PrimitiveExpr<pointed_type, L> operator->() const requires dsl_primitive<pointed_type> {
        return operator*(); // implicitly convert from ConstReference<pointed_type, L>
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Pointer arithmetic
     *----------------------------------------------------------------------------------------------------------------*/

    ///> Adds a \p delta, in elements, to `this`.
    PrimitiveExpr operator+(PrimitiveExpr<offset_t, 1> delta) {
        if constexpr (std::is_void_v<pointed_type>) {
            return PrimitiveExpr(addr_ + delta.make_unsigned(), offset_);
        } else {
            const uint32_t log_size = std::countr_zero(sizeof(pointed_type));
            return PrimitiveExpr(addr_ + (delta.make_unsigned() << log_size), offset_);
        }
    }

    ///> Adds a \p delta, in elements, to `this`.
    PrimitiveExpr operator+(offset_t delta) {
        if constexpr (std::is_void_v<pointed_type>) {
            offset_ += delta; // in bytes
        } else {
            const uint32_t log_size = std::countr_zero(sizeof(pointed_type));
            offset_ += delta << log_size; // in elements
        }
        return *this;
    }

    ///> Subtracts a \p delta, in elements, from `this`.
    PrimitiveExpr operator-(PrimitiveExpr<offset_t, 1> delta) {
        if constexpr (std::is_void_v<pointed_type>) {
            return PrimitiveExpr(addr_ - delta.make_unsigned(), offset_);
        } else {
            const uint32_t log_size = std::countr_zero(sizeof(pointed_type));
            return PrimitiveExpr(addr_ - (delta.make_unsigned() << log_size), offset_);
        }
    }

    ///> Subtracts a \p delta, in elements, from `this`.
    PrimitiveExpr operator-(offset_t delta) {
        if constexpr (std::is_void_v<pointed_type>) {
            offset_ -= delta; // in bytes
        } else {
            const uint32_t log_size = std::countr_zero(sizeof(pointed_type));
            offset_ -= delta << log_size; // in elements
        }
        return *this;
    }

    ///> Computes the difference, in elements, between `this` and \p other.
    PrimitiveExpr<offset_t, 1> operator-(PrimitiveExpr other) {
        if constexpr (std::is_void_v<pointed_type>) {
            PrimitiveExpr<offset_t, 1> delta_addr = (this->addr_ - other.addr_).make_signed();
            offset_t delta_offset = this->offset_ - other.offset_;
            return (delta_offset ? (delta_addr + delta_offset) : delta_addr);
        } else {
            const int32_t log_size = std::countr_zero(sizeof(pointed_type));
            PrimitiveExpr<offset_t, 1> delta_addr = (this->addr_ - other.addr_).make_signed() >> log_size;
            offset_t delta_offset = (this->offset_ - other.offset_) >> log_size;
            return (delta_offset ? (delta_addr + delta_offset) : delta_addr);

        }
    }

#define CMP_OP(SYMBOL) \
    /** Compares `this` to \p other by their addresses. */ \
    PrimitiveExpr<bool, 1> operator SYMBOL(PrimitiveExpr other) { \
        return this->to<uint32_t>() SYMBOL other.to<uint32_t>(); \
    }
    CMP_OP(==)
    CMP_OP(!=)
    CMP_OP(<)
    CMP_OP(<=)
    CMP_OP(>)
    CMP_OP(>=)
#undef CMP_OP


    /*------------------------------------------------------------------------------------------------------------------
     * Load/Store operations
     *----------------------------------------------------------------------------------------------------------------*/

    private:
    PrimitiveExpr<pointed_type, L> load() requires dsl_primitive<pointed_type> {
        M_insist(bool(addr_), "address already moved or discarded");
        if constexpr (L * sizeof(pointed_type) <= 16) {
            auto value = Module::Builder().makeLoad(
                /* bytes=  */ M_CONSTEXPR_COND(L == 1, sizeof(pointed_type), 16),
                /* signed= */ std::is_signed_v<pointed_type>,
                /* offset= */ offset_ >= 0 ? offset_ : 0,
                /* align=  */ alignof(pointed_type),
                /* ptr=    */ offset_ >= 0 ? addr_.expr() : (addr_ - uint32_t(-offset_)).expr(),
                /* type=   */ wasm_type<pointed_type, L>(),
                /* memory= */ Module::Get().memory_->name
            );
            return PrimitiveExpr<pointed_type, L>(value, addr_.referenced_bits());
        } else {
            using ResT = PrimitiveExpr<pointed_type, L>;
            std::array<typename ResT::vector_type, ResT::num_vectors> vectors;
            for (std::size_t idx = 0; idx < ResT::num_vectors; ++idx) {
                auto addr_cpy = addr_.clone();
                auto offset = offset_ + offset_t(idx * 16);
                auto value = Module::Builder().makeLoad(
                    /* bytes=  */ 16,
                    /* signed= */ std::is_signed_v<pointed_type>,
                    /* offset= */ offset >= 0 ? offset : 0,
                    /* align=  */ alignof(pointed_type),
                    /* ptr=    */ offset >= 0 ? addr_cpy.expr() : (addr_cpy - uint32_t(-offset)).expr(),
                    /* type=   */ ::wasm::Type(::wasm::Type::v128),
                    /* memory= */ Module::Get().memory_->name
                );
                vectors[idx] = typename ResT::vector_type(value, addr_cpy.referenced_bits());
            }
            addr_.discard(); // since it was always cloned
            return ResT(std::move(vectors));
        }
    }

    void store(PrimitiveExpr<pointed_type, L> value) requires dsl_primitive<pointed_type> {
        M_insist(bool(addr_), "address already moved or discarded");
        M_insist(bool(value), "value already moved or discarded");
        if constexpr (L * sizeof(pointed_type) <= 16) {
            auto e = Module::Builder().makeStore(
                /* bytes=  */ M_CONSTEXPR_COND(L == 1, sizeof(pointed_type), 16),
                /* offset= */ offset_ >= 0 ? offset_ : 0,
                /* align=  */ alignof(pointed_type),
                /* ptr=    */ offset_ >= 0 ? addr_.expr() : (addr_ - uint32_t(-offset_)).expr(),
                /* value=  */ value.expr(),
                /* type=   */ wasm_type<pointed_type, L>(),
                /* memory= */ Module::Get().memory_->name
            );
            Module::Block().list.push_back(e);
        } else {
            auto vectors = value.vectors();
            for (std::size_t idx = 0; idx < PrimitiveExpr<pointed_type, L>::num_vectors; ++idx) {
                auto addr_cpy = addr_.clone();
                auto offset = offset_ + offset_t(idx * 16);
                auto e = Module::Builder().makeStore(
                    /* bytes=  */ 16,
                    /* offset= */ offset >= 0 ? offset : 0,
                    /* align=  */ alignof(pointed_type),
                    /* ptr=    */ offset >= 0 ? addr_cpy.expr() : (addr_cpy - uint32_t(-offset)).expr(),
                    /* value=  */ vectors[idx].expr(),
                    /* type=   */ ::wasm::Type(::wasm::Type::v128),
                    /* memory= */ Module::Get().memory_->name
                );
                Module::Block().list.push_back(e);
            }
            addr_.discard(); // since it was always cloned
        }
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Printing
     *----------------------------------------------------------------------------------------------------------------*/

    public:
    friend std::ostream & operator<<(std::ostream &out, const PrimitiveExpr &P) {
        out << "PrimitiveExpr<" << typeid(T).name() << "*, " << L << ">: " << P.addr_ << " [" << P.offset_ << "]";
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

namespace detail {

template<typename T>
struct ptr_helper;

template<>
struct ptr_helper<void>
{
    using type = PrimitiveExpr<void*, 1>;
};

template<typename T, std::size_t L>
struct ptr_helper<PrimitiveExpr<T, L>>
{
    using type = PrimitiveExpr<T*, L>;
};

}

/** Alias to easily declare `PrimitiveExpr` of pointer to primitive type. */
template<typename T>
using Ptr = typename detail::ptr_helper<T>::type;


/*======================================================================================================================
 * Expr
 *====================================================================================================================*/

/** An `Expr<T, L>` combines a `PrimitiveExpr<T, L>` value with a `PrimitiveExpr<bool, L>`, called NULL information,
 * to implement a value with *three-valued logic* (3VL).  `Expr<T, L>` provides the same operations as
 * `PrimitiveExpr<T, L>`.  It delegates operations to the underlying value and additionally combines the NULL
 * information of the operand(s) into the new NULL information of the result.  Particular exceptions are `operator
 * and` and `operator or`, for which `Expr<T, L>` implements 3VL according to [Kleene and Priest's
 * logic](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics). */
template<dsl_primitive T, std::size_t L>
requires requires { PrimitiveExpr<T, L>(); PrimitiveExpr<bool, L>(); }
struct Expr<T, L>
{
    using type = T;
    static constexpr std::size_t num_simd_lanes = L;
    using primitive_type = PrimitiveExpr<T, L>;

    /*----- Friends --------------------------------------------------------------------------------------------------*/
    template<typename, std::size_t> friend struct Expr; // to convert Expr<U, L> to Expr<T, L>
    template<typename, VariableKind, bool, std::size_t> friend class detail::variable_storage; // to use split_unsafe()

    private:
    ///> the referenced value expression
    PrimitiveExpr<T, L> value_;
    /** A boolean expression that evaluates to `true` at runtime iff this `Expr` is `NULL`.
     * If this `Expr` cannot be `NULL`, then `is_null_` evaluates to `false` at compile time, i.e. `not is_null_`. */
    PrimitiveExpr<bool, L> is_null_ = PrimitiveExpr<bool, L>();

    public:
    ///> *Implicitly* constructs an `Expr` from a \p value.
    Expr(PrimitiveExpr<T, L> value) : value_(value) {
        M_insist(bool(value_), "value must be present");
    }

    ///> Constructs an `Expr` from a \p value and NULL information \p is_null.
    Expr(PrimitiveExpr<T, L> value, PrimitiveExpr<bool, L> is_null)
        : value_(value)
        , is_null_(is_null)
    {
        M_insist(bool(value_), "value must be present");
        if (is_null)
            M_insist_no_ternary_logic();
    }

    ///> Constructs an `Expr` from a `std::pair` \p value of value and NULL info.
    explicit Expr(std::pair<PrimitiveExpr<T, L>, PrimitiveExpr<bool, L>> value)
        : Expr(value.first, value.second)
    { }

    /** Constructs an `Expr` from a constant \p value. */
    template<dsl_primitive... Us>
    requires (sizeof...(Us) > 0) and requires (Us... us) { PrimitiveExpr<T, L>(us...); }
    explicit Expr(Us... value)
        : Expr(PrimitiveExpr<T, L>(value...))
    { }

    /** Constructs an `Expr` from a decayable constant \p value. */
    template<decayable... Us>
    requires (sizeof...(Us) > 0) and (dsl_primitive<std::decay_t<Us>> and ...) and
    requires (Us... us) { Expr(std::decay_t<Us>(us)...); }
    explicit Expr(Us... value)
        : Expr(std::decay_t<Us>(value)...)
    { }

    Expr(const Expr&) = delete;
    ///> Constructs a new `Expr` by *moving* the underlying `value_` and `is_null_` of `other` to `this`.
    Expr(Expr &other) : Expr(other.split_unsafe()) { /* move, not copy */ }
    ///> Constructs a new `Expr` by *moving* the underlying `value_` and `is_null_` of `other` to `this`.
    Expr(Expr &&other) : Expr(other.split_unsafe()) { }

    Expr & operator=(Expr&&) = delete;

    ~Expr() {
        M_insist(not bool(value_), "value must be used or explicitly discarded");
        M_insist(not bool(is_null_), "NULL flag must be used or explicitly discarded");
    }

    private:
    /** Splits this `Expr` into a `PrimitiveExpr<T, L>` with the value and a `PrimitiveExpr<bool, L>` with the `NULL`
     * information.  Then, *moves* these `PrimitiveExpr`s out of `this`.  Special care must be taken as the NULL
     * information may be unusable, i.e. missing AST. */
    std::pair<PrimitiveExpr<T, L>, PrimitiveExpr<bool, L>> split_unsafe() {
        M_insist(bool(value_), "`Expr` has already been moved");
        return { value_, is_null_ };
    }

    public:
    /** Returns `true` if this `Expr` actually holds a value (Binaryen AST), `false` otherwise. Can be used to test
     * whether this `Expr` has already been used. */
    explicit operator bool() const { return bool(value_); }

    /** *Moves* the current `value_` out of `this`.  Requires (and insists) that `this` cannot be `NULL`. */
    PrimitiveExpr<T, L> insist_not_null() {
        M_insist(bool(value_), "`Expr` has already been moved");
        if (can_be_null())
            Wasm_insist(not is_null_, "must not be NULL");
        return value_;
    }

    /** Splits this `Expr` into a `PrimitiveExpr<T, L>` with the value and a `PrimitiveExpr<bool, L>` with the `NULL`
     * information.  Then, *moves* these `PrimitiveExpr`s out of `this`. */
    std::pair<PrimitiveExpr<T, L>, PrimitiveExpr<bool, L>> split() {
        M_insist(bool(value_), "`Expr` has already been moved");
        auto [value, is_null] = split_unsafe();
        if (is_null)
            return { value, is_null };
        else
            return { value, PrimitiveExpr<bool, L>(false) };
    }

    /** Returns a *deep copy* of `this`. */
    Expr clone() const {
        M_insist(bool(value_), "`Expr` has already been moved`");
        return Expr(
            /* value=   */ value_.clone(),
            /* is_null= */ is_null_ ? is_null_.clone() : PrimitiveExpr<bool, L>()
        );
    }

    /** Discards `this`. */
    void discard() {
        value_.discard();
        if (can_be_null())
            is_null_.discard();
    }


    /*------------------------------------------------------------------------------------------------------------------
     * methods related to NULL
     *----------------------------------------------------------------------------------------------------------------*/

    public:
    /** Returns `true` if `this` *may be* `NULL`, `false` otherwise. */
    bool can_be_null() const { return bool(is_null_); }

    /** Returns `true` if `this` is `NULL`, `false` otherwise. */
    PrimitiveExpr<bool, L> is_null() {
        value_.discard();
        if (can_be_null()) {
            M_insist_no_ternary_logic();
            return is_null_;
        } else {
            return PrimitiveExpr<bool, L>(false);
        }
    }

    /** Returns `true` if `this` is `NOT NULL`, `false` otherwise. */
    PrimitiveExpr<bool, L> not_null() {
        value_.discard();
        if (can_be_null()) {
            M_insist_no_ternary_logic();
            return not is_null_;
        } else {
            return PrimitiveExpr<bool, L>(true);
        }
    }

    /** Returns `true` if the value is `true` and `NOT NULL`.  Useful to use this `Expr<bool, L>` for conditional
     * control flow. */
    PrimitiveExpr<bool, L> is_true_and_not_null() requires boolean<T> {
        if (can_be_null())
            return value_ and not is_null_;
        else
            return value_;
    }

    /** Returns `true` if the value is `false` and `NOT NULL`.  Useful to use this `Expr<bool, L>` for conditional
     * control flow. */
    PrimitiveExpr<bool, L> is_false_and_not_null() requires boolean<T> {
        if (can_be_null())
            return not value_ and not is_null_;
        else
            return not value_;
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Factory method for NULL
     *----------------------------------------------------------------------------------------------------------------*/

    public:
    /** Returns an `Expr` that is `NULL`. */
    static Expr Null() { return Expr(PrimitiveExpr<T, L>(T()), PrimitiveExpr<bool, L>(true)); }


    /*------------------------------------------------------------------------------------------------------------------
     * Conversion operations
     *----------------------------------------------------------------------------------------------------------------*/

    public:
    /** *Implicitly* converts an `Expr<T, L>` to an `Expr<To, ToL>`.  Only applicable if `PrimitiveExpr<T, L>` is
     * implicitly convertible to `PrimitiveExpr<To, ToL>`. */
    template<dsl_primitive To, std::size_t ToL = L>
    requires requires { static_cast<PrimitiveExpr<To, ToL>>(value_); }
    operator Expr<To, ToL>() { return Expr<To, ToL>(static_cast<PrimitiveExpr<To, ToL>>(value_), is_null_); }

    /** *Explicitly* converts an `Expr<T, L>` to an `Expr<To, ToL>`.  Only applicable if `PrimitiveExpr<T, L>` is
     * explicitly convertible to `PrimitiveExpr<To, ToL>` (via method `to<To, ToL>()`). */
    template<dsl_primitive To, std::size_t ToL = L>
    requires requires { value_.template to<To, ToL>(); }
    Expr<To, ToL> to() { return Expr<To, ToL>(value_.template to<To, ToL>(), is_null_); }

    /** Reinterpret an `Expr<T, L>` to an `Expr<To, ToL>`.  Only applicable if `PrimitiveExpr<T, L>` can be
     * reinterpreted to `PrimitiveExpr<To, ToL>`. */
    template<dsl_primitive To, std::size_t ToL = L>
    requires requires { value_.template reinterpret<To, ToL>(); }
    Expr<To, ToL> reinterpret() { return Expr<To, ToL>(value_.template reinterpret<To, ToL>(), is_null_); }

    /** Broadcasts a `PrimitiveExpr<T, 1>` to a `PrimitiveExpr<T, ToL>`.  Only applicable if `PrimitiveExpr<T, 1>`
     * can be broadcasted to `PrimitiveExpr<T, ToL>`. */
    template<std::size_t ToL>
    requires requires { value_.template broadcast<ToL>(); is_null_.template broadcast<ToL>(); }
    Expr<T, ToL> broadcast() {
        return Expr<T, ToL>(value_.template broadcast<ToL>(), is_null_.template broadcast<ToL>());
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Unary operations
     *----------------------------------------------------------------------------------------------------------------*/

    public:
    /** List of supported unary operators on `PrimitiveExpr`. */
#define UNARY_LIST(X) \
    X(make_signed) \
    X(make_unsigned) \
    X(operator +) \
    X(operator -) \
    X(abs) \
    X(ceil) \
    X(floor) \
    X(trunc) \
    X(nearest) \
    X(sqrt) \
    X(operator ~) \
    X(clz) \
    X(ctz) \
    X(popcnt) \
    X(eqz) \
    X(operator not)

#define UNARY(OP) \
    auto OP() requires requires { value_.OP(); } { \
        using PrimExprT = decltype(value_.OP()); \
        using ExprT = expr_t<PrimExprT>; \
        return ExprT(value_.OP(), is_null_); \
    }
UNARY_LIST(UNARY)
#undef UNARY

    /*----- Arithmetical operations with special three-valued logic --------------------------------------------------*/

    auto add_pairwise() requires requires { value_.add_pairwise(); } {
        using PrimExprT = decltype(value_.add_pairwise());
        using ExprT = expr_t<PrimExprT>;
        if (can_be_null())
            return ExprT(value_.add_pairwise(), is_null_.template to<uint8_t>().add_pairwise().template to<bool>());
        else
            return ExprT(value_.add_pairwise());
    }

    /*----- Bitwise operations with special three-valued logic -------------------------------------------------------*/

    Expr<uint32_t, 1> bitmask() requires requires { value_.bitmask(); } {
        if (can_be_null())
            return Expr<uint32_t, 1>(value_.bitmask(), is_null_.any_true());
        else
            return Expr<uint32_t, 1>(value_.bitmask());
    }

    /*----- Logical operations with special three-valued logic -------------------------------------------------------*/

    Expr<bool, 1> any_true() requires requires { value_.any_true(); } {
        if (can_be_null())
            return Expr<bool, 1>(value_.any_true(), is_null_.any_true());
        else
            return Expr<bool, 1>(value_.any_true());
    }

    Expr<bool, 1> all_true() requires requires { value_.all_true(); } {
        if (can_be_null())
            return Expr<bool, 1>(value_.all_true(), is_null_.any_true());
        else
            return Expr<bool, 1>(value_.all_true());
    }

    /*----- Hashing operations with special three-valued logic -------------------------------------------------------*/

    PrimitiveExpr<uint64_t, L> hash() requires requires { value_.hash(); } {
        if (can_be_null())
            return Select(is_null_, PrimitiveExpr<uint64_t, 1>(1UL << 63), value_.hash());
        else
            return value_.hash();
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Binary operations
     *----------------------------------------------------------------------------------------------------------------*/

    public:
#define BINARY(OP) \
    template<dsl_primitive U> \
    auto OP(Expr<U, L> other) requires requires { this->value_.OP(other.value_); } { \
        const unsigned idx = (other.can_be_null() << 1U) | this->can_be_null(); \
        auto result = this->value_.OP(other.value_); \
        using ReturnType = typename decltype(result)::type; \
        constexpr std::size_t ReturnLength = decltype(result)::num_simd_lanes; \
        switch (idx) { \
            default: M_unreachable("invalid index"); \
            case 0b00: /* neither `this` nor `other` can be `NULL` */ \
                return Expr<ReturnType, ReturnLength>(result); \
            case 0b01: /* `this` can be `NULL` */ \
                return Expr<ReturnType, ReturnLength>(result, this->is_null_); \
            case 0b10: /* `other` can be `NULL` */ \
                return Expr<ReturnType, ReturnLength>(result, other.is_null_); \
            case 0b11: /* both `this` and `other` can be `NULL` */ \
                return Expr<ReturnType, ReturnLength>(result, this->is_null_ or other.is_null_); \
        } \
    }

    BINARY(operator +)
    BINARY(operator -)
    BINARY(operator *)
    BINARY(operator /)
    BINARY(operator %)
    BINARY(operator bitand)
    BINARY(operator bitor)
    BINARY(operator xor)
    BINARY(operator <<)
    BINARY(operator >>)
    BINARY(operator ==)
    BINARY(operator !=)
    BINARY(operator <)
    BINARY(operator <=)
    BINARY(operator >)
    BINARY(operator >=)
    BINARY(copy_sign)
    BINARY(min)
    BINARY(max)
    BINARY(avg)
    BINARY(rotl)
    BINARY(rotr)
#undef BINARY

    /*----- Bitwise operations with special three-valued logic -------------------------------------------------------*/

    template<dsl_primitive U>
    Expr operator<<(Expr<U, 1> other) requires requires { this->value_ << other.value_; } and (L > 1) {
        const unsigned idx = (bool(other.is_null_) << 1U) | bool(this->is_null_);
        PrimitiveExpr result = this->value_ << other.value_;
        switch (idx) {
            default: M_unreachable("invalid index");

            case 0b00: { /* neither `this` nor `other` can be `NULL` */
                return Expr(result);
            }
            case 0b01: { /* `this` can be `NULL` */
                return Expr(result, this->is_null_);
            }
            case 0b10: { /* `other` can be `NULL` */
                PrimitiveExpr<bool, L> is_null = other.is_null_.template broadcast<L>();
                return Expr(result, is_null);
            }
            case 0b11: { /* both `this` and `other` can be `NULL` */
                PrimitiveExpr<bool, L> is_null = this->is_null_ or other.is_null_.template broadcast<L>();
                return Expr(result, is_null);
            }
        }
    }

    template<dsl_primitive U>
    Expr operator>>(Expr<U, 1> other) requires requires { this->value_ >> other.value_; } and (L > 1) {
        const unsigned idx = (bool(other.is_null_) << 1U) | bool(this->is_null_);
        PrimitiveExpr result = this->value_ >> other.value_;
        switch (idx) {
            default: M_unreachable("invalid index");

            case 0b00: { /* neither `this` nor `other` can be `NULL` */
                return Expr(result);
            }
            case 0b01: { /* `this` can be `NULL` */
                return Expr(result, this->is_null_);
            }
            case 0b10: { /* `other` can be `NULL` */
                PrimitiveExpr<bool, L> is_null = other.is_null_.template broadcast<L>();
                return Expr(result, is_null);
            }
            case 0b11: { /* both `this` and `other` can be `NULL` */
                PrimitiveExpr<bool, L> is_null = this->is_null_ or other.is_null_.template broadcast<L>();
                return Expr(result, is_null);
            }
        }
    }

    /*----- Logical operations with special three-valued logic -------------------------------------------------------*/

    /** Implements logical *and* according to 3VL of [Kleene and Priest's
     * logic](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics). */
    Expr<bool, L> operator and(Expr<bool, L> other) requires requires { this->value_ and other.value_; } {
        const unsigned idx = (bool(other.is_null_) << 1U) | bool(this->is_null_);
        switch (idx) {
            default: M_unreachable("invalid index");

            case 0b00: { /* neither `this` nor `other` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_ and other.value_;
                return Expr<bool, L>(result);
            }
            case 0b01: { /* `this` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_ and other.value_.clone();
                PrimitiveExpr<bool, L> is_null =
                    this->is_null_ and  // `this` is NULL
                    other.value_;       // `other` does not dominate, i.e. is true
                return Expr<bool, L>(result, is_null);
            }
            case 0b10: { /* `other` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_.clone() and other.value_;
                PrimitiveExpr<bool, L> is_null =
                    other.is_null_ and  // `other` is NULL
                    this->value_;       // `this` does not dominate, i.e. is true
                return Expr<bool, L>(result, is_null);
            }
            case 0b11: { /* both `this` and `other` can be `NULL` */
                auto this_is_null  = this->is_null_.clone();
                auto other_is_null = other.is_null_.clone();
                PrimitiveExpr<bool, L> result = this->value_.clone() and other.value_.clone();
                PrimitiveExpr<bool, L> is_null =
                    (this_is_null or other_is_null) and     // at least one is NULL
                    (this->value_ or this->is_null_) and    // `this` does not dominate, i.e. is not real false
                    (other.value_ or other.is_null_);       // `other` does not dominate, i.e. is not real false
                return Expr<bool, L>(result, is_null);
            }
        }
    }

    /** Implements logical *and not* according to 3VL of [Kleene and Priest's
     * logic](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics). */
    Expr<bool, L> and_not(Expr<bool, L> other) requires requires { this->value_.and_not(other.value_); } {
        const unsigned idx = (bool(other.is_null_) << 1U) | bool(this->is_null_);
        switch (idx) {
            default: M_unreachable("invalid index");

            case 0b00: { /* neither `this` nor `other` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_.and_not(other.value_);
                return Expr<bool, L>(result);
            }
            case 0b01: { /* `this` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_.and_not(other.value_.clone());
                PrimitiveExpr<bool, L> is_null =
                    this->is_null_.and_not( // `this` is NULL
                        other.value_        // `other` does not dominate, i.e. is false
                    );
                return Expr<bool, L>(result, is_null);
            }
            case 0b10: { /* `other` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_.clone().and_not(other.value_);
                PrimitiveExpr<bool, L> is_null =
                    other.is_null_ and  // `other` is NULL
                    this->value_;       // `this` does not dominate, i.e. is true
                return Expr<bool, L>(result, is_null);
            }
            case 0b11: { /* both `this` and `other` can be `NULL` */
                auto this_is_null  = this->is_null_.clone();
                auto other_is_null = other.is_null_.clone();
                PrimitiveExpr<bool, L> result = this->value_.clone().and_not(other.value_.clone());
                PrimitiveExpr<bool, L> is_null =
                    (this_is_null or other_is_null) and     // at least one is NULL
                    (this->value_ or this->is_null_) and    // `this` does not dominate, i.e. is not real false
                    (not other.value_ or other.is_null_);   // `other` does not dominate, i.e. is not real true
                return Expr<bool, L>(result, is_null);
            }
        }
    }

    /** Implements logical *or* according to 3VL of [Kleene and Priest's
     * logic](https://en.wikipedia.org/wiki/Three-valued_logic#Kleene_and_Priest_logics). */
    Expr<bool, L> operator or(Expr<bool, L> other) requires requires { this->value_ or other.value_; } {
        const unsigned idx = (bool(other.is_null_) << 1U) | bool(this->is_null_);
        switch (idx) {
            default: M_unreachable("invalid index");

            case 0b00: { /* neither `this` nor `other` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_ or other.value_;
                return Expr<bool, L>(result);
            }
            case 0b01: { /* `this` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_ or other.value_.clone();
                PrimitiveExpr<bool, L> is_null =
                    this->is_null_ and  // `this` is NULL
                    not other.value_;   // `other` does not dominate, i.e. is false
                return Expr<bool, L>(result, is_null);
            }
            case 0b10: { /* `other` can be `NULL` */
                PrimitiveExpr<bool, L> result = this->value_.clone() or other.value_;
                PrimitiveExpr<bool, L> is_null =
                    other.is_null_ and  // `other` is NULL
                    not this->value_;   // `this` does not dominate, i.e. is false
                return Expr<bool, L>(result, is_null);
            }
            case 0b11: { /* both `this` and `other` can be `NULL` */
                auto this_is_null  = this->is_null_.clone();
                auto other_is_null = other.is_null_.clone();
                PrimitiveExpr<bool, L> result = this->value_.clone() or other.value_.clone();
                PrimitiveExpr<bool, L> is_null =
                    (this_is_null or other_is_null) and         // at least one is NULL
                    (not this->value_ or this->is_null_) and    // `this` does not dominate, i.e. is not real true
                    (not other.value_ or other.is_null_);       // `other` does not dominate, i.e. is not real true
                return Expr<bool, L>(result, is_null);
            }
        }
    }


    /*------------------------------------------------------------------------------------------------------------------
     * Modifications
     *----------------------------------------------------------------------------------------------------------------*/

    /** Extracts the \tparam M -th value of `this`. */
    template<std::size_t M>
    Expr<T, 1> extract() requires requires { value_.template extract<M>(); } {
        if (can_be_null())
            return Expr<T, 1>(value_.template extract<M>(), is_null_.template extract<M>());
        else
            return Expr<T, 1>(value_.template extract<M>());
    }

    /** Replaces the \tparam M -th value of `this` with \p u. */
    template<std::size_t M, expr_convertible U>
    requires requires (expr_t<U> u) { Expr<T, 1>(u); }
    Expr replace(U &&_value) requires requires (Expr<T, 1> e) { this->value_.template replace<M>(e.value_); } {
        Expr<T, 1> value(expr_t<U>(std::forward<U>(_value)));
        if (can_be_null()) {
            auto [value_val, value_is_null] = value.split();
            return Expr(value_.template replace<M>(value_val), is_null_.template replace<M>(value_is_null));
        } else {
            M_insist(not value.can_be_null(), "cannot replace a non-nullable value with a nullable one");
            return Expr(value_.template replace<M>(value.value_));
        }
    }

    /** Selects lanes of `this` in byte granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, 15] select the `i`-th` lane, indices outside of this range result in the value 0. */
    Expr swizzle_bytes(PrimitiveExpr<uint8_t, 16> indices)
    requires (sizeof(T) == 1) and requires { value_.swizzle_bytes(indices); } {
        if (can_be_null()) {
            auto indices_cpy = indices.clone();
            return Expr(value_.swizzle_bytes(indices), is_null_.swizzle_bytes(indices_cpy));
        } else {
            return Expr(value_.swizzle_bytes(indices));
        }
    }
    /** Selects lanes of `this` in byte granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, L * sizeof(T)) select the `i`-th` lane, indices outside of this range result in the value 0. */
    template<std::size_t M>
    Expr<T, M / sizeof(T)> swizzle_bytes(const std::array<uint8_t, M> &indices)
    requires (sizeof(T) == 1) and requires { value_.swizzle_bytes(indices); } {
        if (can_be_null())
            return Expr<T, M / sizeof(T)>(value_.swizzle_bytes(indices), is_null_.swizzle_bytes(indices));
        else
            return Expr<T, M / sizeof(T)>(value_.swizzle_bytes(indices));
    }

    /** Selects lanes of `this` in lane granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, L) select the `i`-th` lane, indices outside of this range result in the value 0. */
    template<std::size_t M>
    Expr<T, M> swizzle_lanes(const std::array<uint8_t, M> &indices)
    requires requires { value_.swizzle_lanes(indices); } {
        if (can_be_null())
            return Expr<T, M>(value_.swizzle_lanes(indices), is_null_.swizzle_lanes(indices));
        else
            return Expr<T, M>(value_.swizzle_lanes(indices));
    }



    /*------------------------------------------------------------------------------------------------------------------
     * Printing
     *----------------------------------------------------------------------------------------------------------------*/

    public:
    friend std::ostream & operator<<(std::ostream &out, const Expr &E) {
        out << "Expr<" << typeid(type).name() << "," << num_simd_lanes << ">: value_=" << E.value_
            << ", is_null_=" << E.is_null_;
        return out;
    }

    void dump(std::ostream &out) const { out << *this << std::endl; }
    void dump() const { dump(std::cerr); }
};

/** CTAD guide for `Expr` */
template<typename T, std::size_t L>
Expr(PrimitiveExpr<T, L>, PrimitiveExpr<bool, L>) -> Expr<T, L>;

/*----- Forward binary operators on operands convertible to Expr<T, L> -----------------------------------------------*/
#define MAKE_BINARY(OP) \
    template<expr_convertible T, expr_convertible U> \
    requires (not primitive_convertible<T> or not primitive_convertible<U>) and \
    requires (expr_t<T> t, expr_t<U> u) { t.OP(u); } \
    auto OP(T &&t, U &&u) \
    { \
        return expr_t<T>(std::forward<T>(t)).OP(expr_t<U>(std::forward<U>(u))); \
    }
BINARY_LIST(MAKE_BINARY)
#undef MAKE_BINARY

/*----- Short aliases for all `PrimitiveExpr` and `Expr` types. ------------------------------------------------------*/
#define USING_N(TYPE, LENGTH, NAME) \
    using NAME = PrimitiveExpr<TYPE, LENGTH>; \
    using _ ## NAME = Expr<TYPE, LENGTH>;
#define USING(TYPE, NAME) \
    template<std::size_t L> using NAME = PrimitiveExpr<TYPE, L>; \
    template<std::size_t L> using _ ## NAME = Expr<TYPE, L>; \
    USING_N(TYPE, 1,  NAME ## x1) \
    USING_N(TYPE, 2,  NAME ## x2) \
    USING_N(TYPE, 4,  NAME ## x4) \
    USING_N(TYPE, 8,  NAME ## x8) \
    USING_N(TYPE, 16, NAME ## x16) \
    USING_N(TYPE, 32, NAME ## x32)

    USING(bool,     Bool)
    USING(int8_t,   I8)
    USING(uint8_t,  U8)
    USING(int16_t,  I16)
    USING(uint16_t, U16)
    USING(int32_t,  I32)
    USING(uint32_t, U32)
    USING(int64_t,  I64)
    USING(uint64_t, U64)
    USING(float,    Float)
    USING(double,   Double)
    ///> this is neither signed nor unsigned char (see https://en.cppreference.com/w/cpp/language/types, Character types)
    USING_N(char,   1,  Charx1)
    USING_N(char,   16, Charx16)
    USING_N(char,   32, Charx32)
#undef USING


/*======================================================================================================================
 * Variable
 *====================================================================================================================*/

namespace detail {

/** Allocates a fresh local variable of type \tparam T and number of SIMD lanes \tparam L in the currently active
 * function's stack and returns the variable's `::wasm::Index`. */
template<dsl_primitive T, std::size_t L>
::wasm::Index allocate_local()
{
    ::wasm::Function &fn = Module::Function();
    const ::wasm::Index index = fn.getNumParams() + fn.vars.size();
    const ::wasm::Type type = wasm_type<T, L>();
    fn.vars.emplace_back(type); // allocate new local variable
    M_insist(fn.isVar(index));
    M_insist(fn.getLocalType(index) == type);
    return index;
}

/** Helper class to select the appropriate storage for a `Variable`.  Local variables are allocated on the currently
 * active function's stack whereas global variables are allocated globally.  Local variables of primitive type
 * can have an additional `NULL` information. */
template<typename T, VariableKind Kind, bool CanBeNull, std::size_t L>
class variable_storage;

/** Specialization for local variables of arithmetic type that *cannot* be `NULL`. */
template<dsl_primitive T, VariableKind Kind, std::size_t L>
requires arithmetic<T> or (boolean<T> and Kind == VariableKind::Param)
class variable_storage<T, Kind, /* CanBeNull= */ false, L>
{
    ///> the number of Wasm locals needed
    static constexpr std::size_t num_locals = ((L * sizeof(T)) + 15) / 16;
    static_assert(Kind != VariableKind::Param or num_locals == 1, "parameters must fit in a single Wasm local");

    template<typename, VariableKind, bool, std::size_t>
    friend class variable_storage; // to enable use in other `variable_storage`
    friend struct Variable<T, Kind, false, L>; // to be usable by the respective Variable

    std::array<::wasm::Index, num_locals> indices_; ///< the indices of the local(s)
    ::wasm::Type type_; ///< the type of the local(s)

    /** Default-construct. */
    variable_storage()
        : indices_([](){
            std::array<::wasm::Index, num_locals> indices;
            for (std::size_t idx = 0; idx < num_locals; ++idx)
                indices[idx] = allocate_local<T, L>();
            return indices;
        }())
        , type_(wasm_type<T, L>())
    { }

    variable_storage(const variable_storage&) = delete;
    variable_storage(variable_storage&&) = default;
    variable_storage & operator=(variable_storage&&) = default;

    /** Construct from `::wasm::Index` of already allocated local. */
    variable_storage(::wasm::Index idx, tag<int>)
    requires (num_locals == 1)
        : indices_(std::to_array({ idx })), type_(wasm_type<T, L>())
    {
#ifndef NDEBUG
        ::wasm::Function &fn = Module::Function();
        M_insist(fn.isParam(indices_[0]));
        M_insist(fn.getLocalType(indices_[0]) == type_);
#endif
    }

    /** Construct from value. */
    template<typename... Us>
    requires (sizeof...(Us) > 0) and requires (Us... us) { PrimitiveExpr<T, L>(us...); }
    explicit variable_storage(Us... value) : variable_storage() { operator=(PrimitiveExpr<T, L>(value...)); }

    /** Construct from value. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    explicit variable_storage(U &&value) : variable_storage() { operator=(std::forward<U>(value)); }

    /** Assign value. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    void operator=(U &&_value) requires (num_locals == 1) {
        PrimitiveExpr<T, L> value(primitive_expr_t<U>(std::forward<U>(_value)));
        Module::Block().list.push_back(Module::Builder().makeLocalSet(indices_[0], value.expr()));
    }
    /** Assign value. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    void operator=(U &&_value) requires (num_locals > 1) {
        PrimitiveExpr<T, L> value(primitive_expr_t<U>(std::forward<U>(_value)));
        static_assert(num_locals == decltype(value)::num_vectors);
        auto vectors = value.vectors();
        for (std::size_t idx = 0; idx < num_locals; ++idx)
            Module::Block().list.push_back(Module::Builder().makeLocalSet(indices_[idx], vectors[idx].expr()));
    }

    /** Retrieve value. */
    operator PrimitiveExpr<T, L>() const requires (num_locals == 1) {
        return PrimitiveExpr<T, L>(Module::Builder().makeLocalGet(indices_[0], type_));
    }
    /** Retrieve value. */
    operator PrimitiveExpr<T, L>() const requires (num_locals > 1) {
        static_assert(num_locals == PrimitiveExpr<T, L>::num_vectors);
        std::array<typename PrimitiveExpr<T, L>::vector_type, num_locals> vectors;
        for (std::size_t idx = 0; idx < num_locals; ++idx)
            vectors[idx] = typename PrimitiveExpr<T, L>::vector_type(
                Module::Builder().makeLocalGet(indices_[idx], type_)
            );
        return PrimitiveExpr<T, L>(std::move(vectors));
    }
};

/** Specialization for local variables of boolean type that *cannot* be `NULL`. */
template<std::size_t L>
class variable_storage<bool, VariableKind::Local, /* CanBeNull= */ false, L>
{
    ///> the number of SIMD lanes of each local bit
    static constexpr std::size_t bit_num_simd_lanes = std::min<std::size_t>(L, 16);
    ///> the number of local bits (with maximal number of SIMD lanes of 16) needed
    static constexpr std::size_t num_locals = (L + 15) / 16;

    template<typename, VariableKind, bool, std::size_t>
    friend class variable_storage; // to enable use in other `variable_storage`
    friend struct Variable<bool, VariableKind::Local, false, L>; // to be usable by the respective Variable

    ///> stores each boolean value in a single bit
    std::array<std::shared_ptr<LocalBit<bit_num_simd_lanes>>, num_locals> values_;

    /** Default-construct. */
    variable_storage(); // impl delayed because `LocalBit` defined later

    variable_storage(const variable_storage&) = delete;
    variable_storage(variable_storage&&) = default;
    variable_storage & operator=(variable_storage&&) = default;

    /** Construct from value. */
    template<typename... Us>
    requires (sizeof...(Us) > 0) and requires (Us... us) { PrimitiveExpr<bool, L>(us...); }
    explicit variable_storage(Us... value) : variable_storage() { operator=(PrimitiveExpr<bool, L>(value...)); }

    /** Construct from value. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<bool, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    explicit variable_storage(U &&value) : variable_storage() { operator=(std::forward<U>(value)); }

    /** Assign value. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<bool, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    void operator=(U &&value); // impl delayed because `LocalBit` defined later

    /** Retrieve value. */
    operator PrimitiveExpr<bool, L>() const; // impl delayed because `LocalBit` defined later
};

/** Specialization for local variables of primitive type (arithmetic and boolean) that *can* be `NULL`. */
template<dsl_primitive T, std::size_t L>
class variable_storage<T, VariableKind::Local, /* CanBeNull= */ true, L>
{
    friend struct Variable<T, VariableKind::Local, true, L>; // to be usable by the respective Variable

    variable_storage<T, VariableKind::Local, false, L> value_;
    variable_storage<bool, VariableKind::Local, false, L> is_null_;

    /** Default-construct. */
    variable_storage() { M_insist_no_ternary_logic(); }

    /** Construct from value. */
    template<typename... Us>
    requires (sizeof...(Us) > 0) and requires (Us... us) { Expr<T, L>(us...); }
    explicit variable_storage(Us... value) : variable_storage() { operator=(Expr<T, L>(value...)); }

    /** Construct from value. */
    template<expr_convertible U>
    requires requires (U &&u) { Expr<T, L>(expr_t<U>(std::forward<U>(u))); }
    explicit variable_storage(U &&value) : variable_storage() { operator=(std::forward<U>(value)); }

    /** Assign value. */
    template<expr_convertible U>
    requires requires (U &&u) { Expr<T, L>(expr_t<U>(std::forward<U>(u))); }
    void operator=(U &&_value) {
        Expr<T, L> value(expr_t<U>(std::forward<U>(_value)));
        auto [val, is_null] = value.split_unsafe();
        this->value_ = val;
        this->is_null_ = bool(is_null) ? is_null : PrimitiveExpr<bool, L>(false);
    }

    /** Retrieve value. */
    operator Expr<T, L>() const { return Expr<T, L>(PrimitiveExpr<T, L>(value_), PrimitiveExpr<bool, L>(is_null_)); }
};

/** Specialization for local variables of pointer to primitive type.  Pointers *cannot* be `NULL`. */
template<dsl_pointer_to_primitive T, VariableKind Kind, std::size_t L>
requires (Kind != VariableKind::Global)
class variable_storage<T, Kind, /* CanBeNull= */ false, L>
{
    friend struct Variable<T, Kind, false, L>; // to be usable by the respective Variable

    ///> the address
    variable_storage<uint32_t, Kind, false, 1> addr_;

    /** Default-construct. */
    variable_storage() = default;

    /** Construct from `::wasm::Index` of already allocated local. */
    explicit variable_storage(::wasm::Index idx, tag<int> tag) : addr_(idx, tag) { }

    /** Construct from pointer. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    explicit variable_storage(U &&value) : variable_storage() { operator=(std::forward<U>(value)); }

    /** Assign pointer. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    void operator=(U &&value) {
        addr_ = PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(value))).template to<uint32_t, 1>();
    }

    /** Retrieve pointer. */
    operator PrimitiveExpr<T, L>() const { return PrimitiveExpr<uint32_t, 1>(addr_).template to<T, L>(); }
};

/** Specialization for global variables of primitive or pointer to primitive type \tparam T.  Global variables must be
 * of primitive or pointer to primitive type and *cannot* be `NULL`. */
template<typename T, std::size_t L>
requires dsl_primitive<T> or dsl_pointer_to_primitive<T>
class variable_storage<T, VariableKind::Global, /* CanBeNull= */ false, L>
{
    ///> the number of Wasm globals needed; pointers are always stored as single 32-bit unsigned integer global
    static constexpr std::size_t num_globals = dsl_primitive<T> ? ((L * sizeof(T)) + 15) / 16 : 1;

    friend struct Variable<T, VariableKind::Global, false, L>; // to be usable by the respective Variable

    std::array<::wasm::Name, num_globals> names_; ///< the unique names of the global(s)
    ::wasm::Type type_; ///< the type of the global(s)

    /** Default construct. */
    variable_storage()
    requires dsl_primitive<T>
        : variable_storage(T())
    { }

    variable_storage(const variable_storage&) = delete;
    variable_storage(variable_storage&&) = default;
    variable_storage & operator=(variable_storage&&) = default;

    /** Construct with initial value. */
    template<typename... Us>
    requires (sizeof...(Us) > 0) and requires (Us... us) { Module::Get().emit_global<T, L>(names_, true, us...); }
    explicit variable_storage(Us... init)
    requires dsl_primitive<T>
        : names_([](){
            std::array<::wasm::Name, num_globals> names;
            for (std::size_t idx = 0; idx < num_globals; ++idx)
                names[idx] = Module::Unique_Global_Name();
            return names;
        }())
        , type_(wasm_type<T, L>())
    {
        Module::Get().emit_global<T, L>(names_, true, init...);
    }
    /** Construct with optional initial value. */
    explicit variable_storage(uint32_t init = 0)
    requires dsl_pointer_to_primitive<T>
        : names_(std::to_array<::wasm::Name>({ Module::Unique_Global_Name() }))
        , type_(wasm_type<T, L>())
    {
        Module::Get().emit_global<T, L>(names_[0], true, init);
    }

    /** Sets the initial value. */
    template<dsl_primitive... Us>
    requires (sizeof...(Us) > 0) and requires (Us... us) { make_literal<T, L>(us...); }
    void init(Us... init) requires dsl_primitive<T> and (num_globals == 1) {
        Module::Get().module_.getGlobal(names_[0])->init = Module::Builder().makeConst(make_literal<T, L>(init...));
    }
    /** Sets the initial value. */
    template<dsl_primitive... Us>
    requires (sizeof...(Us) > 0) and
    requires (Us... us) { { make_literal<T, L>(us...) } -> std::same_as<std::array<::wasm::Literal, num_globals>>; }
    void init(Us... init) requires dsl_primitive<T> and (num_globals > 1) {
        auto literals = make_literal<T, L>(init...);
        for (std::size_t idx = 0; idx < num_globals; ++idx)
            Module::Get().module_.getGlobal(names_[idx])->init = Module::Builder().makeConst(literals[idx]);
    }
    /** Sets the initial value. */
    void init(uint32_t init) requires dsl_pointer_to_primitive<T> {
        Module::Get().module_.getGlobal(names_[0])->init = Module::Builder().makeConst(::wasm::Literal(init));
    }

    /** Assign value. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    void operator=(U &&_value) requires (num_globals == 1) {
        PrimitiveExpr<T, L> value(primitive_expr_t<U>(std::forward<U>(_value)));
        Module::Block().list.push_back(Module::Builder().makeGlobalSet(names_[0], value.expr()));
    }
    /** Assign value. */
    template<primitive_convertible U>
    requires requires (U &&u) { PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    void operator=(U &&_value) requires (num_globals > 1) {
        PrimitiveExpr<T, L> value(primitive_expr_t<U>(std::forward<U>(_value)));
        static_assert(num_globals == decltype(value)::num_vectors);
        auto vectors = value.vectors();
        for (std::size_t idx = 0; idx < num_globals; ++idx)
            Module::Block().list.push_back(Module::Builder().makeGlobalSet(names_[idx], vectors[idx].expr()));
    }

    /** Retrieve value. */
    operator PrimitiveExpr<T, L>() const requires (num_globals == 1) {
        return PrimitiveExpr<T, L>(Module::Builder().makeGlobalGet(names_[0], type_));
    }
    /** Retrieve value. */
    operator PrimitiveExpr<T, L>() const requires (num_globals > 1) {
        static_assert(num_globals == PrimitiveExpr<T, L>::num_vectors);
        std::array<typename PrimitiveExpr<T, L>::vector_type, num_globals> vectors;
        for (std::size_t idx = 0; idx < num_globals; ++idx)
            vectors[idx] = typename PrimitiveExpr<T, L>::vector_type(
                Module::Builder().makeGlobalGet(names_[idx], type_)
            );
        return PrimitiveExpr<T, L>(std::move(vectors));
    }
};

}

template<typename T, VariableKind Kind, bool CanBeNull, std::size_t L>
requires (not (dsl_pointer_to_primitive<T> and CanBeNull)) and  // pointers cannot be NULL
         (not (Kind == VariableKind::Global and CanBeNull)) and // globals cannot be NULL
requires { typename std::conditional_t<CanBeNull, Expr<T, L>, PrimitiveExpr<T, L>>; }
struct Variable<T, Kind, CanBeNull, L>
{
    using type = T;
    static constexpr std::size_t num_simd_lanes = L;
    template<typename X>
    using dependent_expr_t = conditional_one_t<CanBeNull, expr_t, primitive_expr_t, X>;
    using dependent_expr_type = dependent_expr_t<PrimitiveExpr<T, L>>;

    private:
    ///> the type of storage for this `Variable`
    using storage_type = detail::variable_storage<T, Kind, CanBeNull, L>;
    ///> storage of this `Variable`
    storage_type storage_;
#ifdef M_ENABLE_SANITY_FIELDS
    ///> flag to insist that this `Variable` at least used once
    mutable bool used_ = false;
#define REGISTER_USE(VAR) (VAR).used_ = true
#else
#define REGISTER_USE(VAR)
#endif

    public:
    /** Default-constructs a new `Variable`. */
    Variable() = default;

    Variable(const Variable&) = delete;
    Variable(Variable &&other)
        : storage_(std::forward<storage_type>(other.storage_))
#ifdef M_ENABLE_SANITY_FIELDS
        , used_(other.used_)
#endif
    {
        REGISTER_USE(other);
    }

    Variable & operator=(const Variable &other) { operator=(other.val()); return *this; }
    Variable & operator=(Variable &&other) { operator=(other.val()); return *this; }

    ~Variable() {
#ifdef M_ENABLE_SANITY_FIELDS
        M_insist(used_, "variable must be used at least once");
#endif
    }

    /** Constructs a new `Variable` and initializes it with \p value. */
    template<typename... Us>
    requires requires (Us&&... us) { storage_type(std::forward<Us>(us)...); }
    explicit Variable(Us&&... value) : storage_(std::forward<Us>(value)...) { }

    protected:
    /** Constructs a `Variable` instance from an already allocated local with the given index \p idx.  Used by
     * `Parameter` to create `Variable` instances for function parameters. */
    Variable(::wasm::Index idx, tag<int> tag)
    requires (Kind == VariableKind::Param)
        : storage_(idx, tag)
    { }

    public:
    /** Check whether this `Variable` can be assigned to `NULL`, i.e. it has a NULL bit to store this information.
     * This is a compile-time information. */
    constexpr bool has_null_bit() const { return CanBeNull; }
    /** Check whether the value of this `Variable` can be `NULL`.  This is a runtime-time information. */
    bool can_be_null() const {
        if constexpr (CanBeNull)
            return dependent_expr_type(*this).can_be_null();
        else
            return false;
    }

    /** Obtain a `Variable<T, L>`s value as a `PrimitiveExpr<T, L>` or `Expr<T, L>`, depending on `CanBeNull`.
     * Although a `Variable`'s value can also be obtained through implicit conversion (see below), some C/C++
     * constructs fail to do so (e.g. arguments to calls) and it is therefore more convenient to call `val()`. */
    dependent_expr_type val() const { REGISTER_USE(*this); return dependent_expr_type(storage_); }

    /** Obtain a `Variable<T, L>`s value as a `PrimitiveExpr<T, L>` or `Expr<T, L>`, depending on `CanBeNull`.  This
     * implicit conversion enables using a `Variable` much like a `PrimitiveExpr` or `Expr`, respectively. */
    operator dependent_expr_type() const { REGISTER_USE(*this); return dependent_expr_type(storage_); }

    template<typename U>
    requires requires (dependent_expr_type v) { dependent_expr_t<U>(v); }
    operator dependent_expr_t<U>() const { return dependent_expr_t<U>(dependent_expr_type(*this)); }

    template<typename To, std::size_t ToL = L>
    requires requires (dependent_expr_type v) { v.template to<To, ToL>(); }
    dependent_expr_t<PrimitiveExpr<To, ToL>> to() const { return dependent_expr_type(*this).template to<To, ToL>(); }

    template<typename To, std::size_t ToL = L>
    requires requires (dependent_expr_type v) { v.template reinterpret<To, ToL>(); }
    dependent_expr_t<PrimitiveExpr<To, ToL>> reinterpret() const {
        return dependent_expr_type(*this).template reinterpret<To, ToL>();
    }

    template<std::size_t ToL>
    requires requires (dependent_expr_type v) { v.template broadcast<ToL>(); }
    dependent_expr_t<PrimitiveExpr<T, ToL>> broadcast() const {
        return dependent_expr_type(*this).template broadcast<ToL>();
    }

    template<typename... Us>
    requires requires (Us&&... us) { storage_.init(std::forward<Us>(us)...); }
    void init(Us&&... init) { storage_.init(std::forward<Us>(init)...); }

    template<typename U>
    requires requires (U &&u) { storage_ = std::forward<U>(u); }
    Variable & operator=(U &&value) { storage_ = std::forward<U>(value); return *this; }


    /*------------------------------------------------------------------------------------------------------------------
     * Forward operators on Variable<T, L>
     *----------------------------------------------------------------------------------------------------------------*/

    /*----- Unary operators ------------------------------------------------------------------------------------------*/
#define UNARY(OP) \
    auto OP() const requires requires (dependent_expr_type e) { e.OP(); } { return dependent_expr_type(*this).OP(); }

    UNARY_LIST(UNARY)
    UNARY(add_pairwise)             // from PrimitiveExpr and Expr
    UNARY(bitmask)                  // from PrimitiveExpr and Expr
    UNARY(any_true)                 // from PrimitiveExpr and Expr
    UNARY(all_true)                 // from PrimitiveExpr and Expr
    UNARY(hash)                     // from PrimitiveExpr and Expr
    UNARY(operator *)               // from PrimitiveExpr for pointers
    UNARY(operator ->)              // from PrimitiveExpr for pointers
    UNARY(is_nullptr)               // from PrimitiveExpr for pointers
    UNARY(is_null)                  // from Expr
    UNARY(not_null)                 // from Expr
    UNARY(is_true_and_not_null)     // from Expr
    UNARY(is_false_and_not_null)    // from Expr
#undef UNARY

    /*----- Assignment operators -------------------------------------------------------------------------------------*/
#define ASSIGNOP_LIST(X) \
    X(+) \
    X(-) \
    X(*) \
    X(/) \
    X(%) \
    X(&) \
    X(|) \
    X(^) \
    X(<<) \
    X(>>)

#define ASSIGNOP(SYMBOL) \
    template<typename U> \
    requires requires { typename dependent_expr_t<U>; } and \
             requires (U &&u) { dependent_expr_t<U>(std::forward<U>(u)); } and \
             requires (dependent_expr_type var_value, dependent_expr_t<U> other_value) \
                      { var_value SYMBOL other_value; } and \
             requires (Variable var, \
                       decltype(std::declval<dependent_expr_type>() SYMBOL std::declval<dependent_expr_t<U>>()) value) \
                      { var = value; } \
    Variable & operator SYMBOL##= (U &&value) { \
        dependent_expr_t<U> _value(std::forward<U>(value)); \
        this->operator=(dependent_expr_type(*this) SYMBOL _value); \
        return *this; \
    }
ASSIGNOP_LIST(ASSIGNOP)
#undef ASSIGNOP

    /*----- Modifications --------------------------------------------------------------------------------------------*/
    /** Extracts the \tparam M -th value of `this`. */
    template<std::size_t M>
    auto extract() const requires requires (dependent_expr_type e) { e.template extract<M>(); } {
        return dependent_expr_type(*this).template extract<M>();
    }

    /** Replaces the \tparam M -th value of `this` with \p value. */
    template<std::size_t M, typename U>
    Variable & replace(U &&value)
    requires requires (dependent_expr_type e) { e.template replace<M>(std::forward<U>(value)); } {
        this->operator=(dependent_expr_type(*this).template replace<M>(std::forward<U>(value)));
        return *this;
    }

    /** Selects lanes of `this` in byte granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, 15] select the `i`-th` lane, indices outside of this range result in the value 0. */
    auto swizzle_bytes(PrimitiveExpr<uint8_t, 16> indices) const
    requires requires (dependent_expr_type e) { e.swizzle_bytes(indices); } {
        return dependent_expr_type(*this).swizzle_bytes(indices);
    }

    /** Selects lanes of `this` in lane granularity depending on the indices specified by \p indices.  Indices `i` in
     * the range [0, L) select the `i`-th` lane, indices outside of this range result in the value 0. */
    template<std::size_t M>
    auto swizzle_lanes(const std::array<uint8_t, M> &indices) const
    requires requires (dependent_expr_type e) { e.swizzle_lanes(indices); } {
        return dependent_expr_type(*this).swizzle_lanes(indices);
    }

#undef REGISTER_USE
};

/*----- Overload forwarded binary operators for pointer advancing on PrimitiveExpr<T*, L> ----------------------------*/
template<dsl_pointer_to_primitive T, VariableKind Kind, bool CanBeNull, std::size_t L>
requires requires (const Variable<T, Kind, CanBeNull, L> &var, typename PrimitiveExpr<T, L>::offset_t delta)
         { var.val().operator+(delta); }
auto operator+(const Variable<T, Kind, CanBeNull, L> &var, typename PrimitiveExpr<T, L>::offset_t delta)
{
    return var.val().operator+(delta);
}

template<dsl_pointer_to_primitive T, VariableKind Kind, bool CanBeNull, std::size_t L>
requires requires (const Variable<T, Kind, CanBeNull, L> &var, typename PrimitiveExpr<T, L>::offset_t delta)
         { var.val().operator-(delta); }
auto operator-(const Variable<T, Kind, CanBeNull, L> &var, typename PrimitiveExpr<T, L>::offset_t delta)
{
    return var.val().operator-(delta);
}

namespace detail {

/** Deduces a suitable specialization of `Variable` for the given type \tparam T. */
template<typename T>
struct var_helper;

template<typename T, std::size_t L>
struct var_helper<PrimitiveExpr<T, L>>
{ using type = Variable<T, VariableKind::Local, /* CanBeNull= */ false, L>; };

template<typename T, std::size_t L>
struct var_helper<Expr<T, L>>
{ using type = Variable<T, VariableKind::Local, /* CanBeNull= */ true, L>; };

/** Deduces a suitable specialization of `Variable` *that can be NULL* for the given type \tparam T. */
template<typename T>
struct _var_helper;

template<typename T, std::size_t L>
struct _var_helper<PrimitiveExpr<T, L>>
{ using type = Variable<T, VariableKind::Local, /* CanBeNull= */ true, L>; };

template<typename T, std::size_t L>
struct _var_helper<Expr<T, L>>
{ using type = Variable<T, VariableKind::Local, /* CanBeNull= */ true, L>; };

/** Deduces a suitable specialization of `Variable` for global variables of the given type \tparam T. */
template<typename T>
struct global_helper;

template<typename T, std::size_t L>
struct global_helper<PrimitiveExpr<T, L>>
{ using type = Variable<T, VariableKind::Global, /* CanBeNull= */ false, L>; };

}

/** Local variable. Can be `NULL` if \tparam T can be `NULL`. */
template<typename T>
requires requires { typename detail::var_helper<T>::type; }
using Var = typename detail::var_helper<T>::type;

/** Local variable that *can always* be `NULL`. */
template<typename T>
requires requires { typename detail::_var_helper<T>::type; }
using _Var = typename detail::_var_helper<T>::type;

/** Global variable.  Cannot be `NULL`. */
template<typename T>
requires requires { typename detail::global_helper<T>::type; }
using Global = typename detail::global_helper<T>::type;


/*======================================================================================================================
 * Parameter
 *====================================================================================================================*/

/** A type to access function parameters.  Function parameters are like local variables, but they need not be explicitly
 * allocated on the stack but are implicitly allocated by the function's signature.  Parameters are indexed in the order
 * they occur in the function signature. */
template<typename T, std::size_t L>
requires (L * sizeof(T) <= 16) // parameter must fit in a single Wasm local
struct Parameter<T, L> : Variable<T, VariableKind::Param, /* CanBeNull= */ false, L>
{
    template<typename>
    friend struct Function; // to enable `Function` to create `Parameter` instances through private c'tor

    using base_type = Variable<T, VariableKind::Param, /* CanBeNull= */ false, L>;
    using base_type::operator=;
    using dependent_expr_type = typename base_type::dependent_expr_type;
    using base_type::operator dependent_expr_type;

    private:
    /** Create a `Parameter<T, L>` for the existing parameter local of given `index`.  Parameters can only be created by
     * `Function::parameter<I>()`. */
    Parameter(unsigned index)
        : base_type(::wasm::Index(index), tag<int>{})
    {
        ::wasm::Function &fn = Module::Function();
        M_insist(index < fn.getNumLocals(), "index out of bounds");
        M_insist(fn.isParam(index), "not a parameter");
        M_insist(fn.getLocalType(index) == (wasm_type<T, L>()), "type mismatch");
    }
};


/*======================================================================================================================
 * Pointer and References
 *====================================================================================================================*/

namespace detail {

template<dsl_primitive T, std::size_t L, bool IsConst>
struct the_reference
{
    friend struct PrimitiveExpr<T*, L>; // to construct a reference to the pointed-to memory
    friend struct Variable<T*, VariableKind::Local, false, L>; // to construct a reference to the pointed-to memory
    friend struct Variable<T*, VariableKind::Global, false, L>; // to construct a reference to the pointed-to memory
    friend struct Variable<T*, VariableKind::Param, false, L>; // to construct a reference to the pointed-to memory

    static constexpr bool is_const = IsConst;

    private:
    PrimitiveExpr<T*, L> ptr_;

    private:
    explicit the_reference(PrimitiveExpr<T*, L> ptr)
        : ptr_(ptr)
    {
        M_insist(bool(ptr_), "must not be moved or discarded");
    }

    public:
    template<typename U>
    requires (not is_const) and requires (U &&u) { PrimitiveExpr<T, L>(primitive_expr_t<U>(std::forward<U>(u))); }
    void operator=(U &&_value) {
        PrimitiveExpr<T, L> value(primitive_expr_t<U>(std::forward<U>(_value)));
        ptr_.store(value);
    }

    ///> implicit loading of the referenced value
    operator PrimitiveExpr<T, L>() { return ptr_.load(); }

#define ASSIGNOP(SYMBOL) \
    template<typename U> \
    requires requires (the_reference ref, U &&u) { ref SYMBOL std::forward<U>(u); } and \
             requires (the_reference ref, decltype(ref SYMBOL std::declval<U>()) value) \
                      { ref = value; } \
    void operator SYMBOL##= (U &&value) { \
        this->operator=(the_reference(ptr_.clone()) SYMBOL std::forward<U>(value)); \
    }
ASSIGNOP_LIST(ASSIGNOP)
#undef ASSIGNOP
};

}


/*======================================================================================================================
 * LocalBitmap, LocalBitvector, Bit, and LocalBit
 *====================================================================================================================*/

struct LocalBitmap
{
    friend struct Module;

    Var<U64x1> u64;
    uint64_t bitmask = uint64_t(-1UL);

    private:
    LocalBitmap() = default;
    LocalBitmap(const LocalBitmap&) = delete;
};

struct LocalBitvector
{
    friend struct Module;

    Var<U8x16> u8x16;
    ///> entry at index `i` is a bitmask for the 16 lanes using the constant bit offset `i`
    std::array<uint16_t, 8> bitmask_per_offset;

    private:
    LocalBitvector()
    {
        bitmask_per_offset.fill(uint16_t(-1U));
    }
    LocalBitvector(const LocalBitvector&) = delete;
};

struct Bit
{
    virtual ~Bit() { }
};

namespace detail {

/** Helper class to select appropriate storage for a `LocalBit`. */
template<std::size_t L>
requires (L <= 16)
class local_bit_storage
{
    friend struct LocalBit<L>; // to be usable by the respective LocalBit

    friend void swap(local_bit_storage &first, local_bit_storage &second) {
        using std::swap;
        swap(first.bitvector_,     second.bitvector_);
        swap(first.bit_offset_,    second.bit_offset_);
        swap(first.starting_lane_, second.starting_lane_);
    }

    LocalBitvector *bitvector_ = nullptr; ///< the bitvector in which the *multiple* bits are contained
    uint8_t bit_offset_; ///< the offset of each bit in every lane
    uint8_t starting_lane_; ///< the lane index at which the L bits start

    local_bit_storage() = default;
    /** Creates multiple bits with storage allocated in \p bitvector. */
    local_bit_storage(LocalBitvector &bitvector, uint8_t bit_offset, uint8_t starting_lane)
        : bitvector_(&bitvector)
        , bit_offset_(bit_offset)
        , starting_lane_(starting_lane)
    {
        M_insist(bit_offset_ < 8, "offset out of bounds");
        M_insist(starting_lane_ + L <= 16, "starting lane out of bounds");
    }
};

/** Specialization for a *single* bit. */
template<>
class local_bit_storage<1>
{
    friend struct LocalBit<1>; // to be usable by the respective LocalBit

    friend void swap(local_bit_storage &first, local_bit_storage &second) {
        using std::swap;
        swap(first.bitmap_,     second.bitmap_);
        swap(first.bit_offset_, second.bit_offset_);
    }

    LocalBitmap *bitmap_ = nullptr; ///< the bitmap in which the *single* bit is contained
    uint8_t bit_offset_; ///< the offset of the *single* bit

    local_bit_storage() = default;
    /** Creates a single bit with storage allocated in \p bitmap. */
    local_bit_storage(LocalBitmap &bitmap, uint8_t bit_offset) : bitmap_(&bitmap), bit_offset_(bit_offset)
    {
        M_insist(bit_offset_ < CHAR_BIT * sizeof(uint64_t), "offset out of bounds");
    }
};

}

/**
 * A scalar bit or a vector of bits that is managed by the current function's stack.
 *
 * 0 ⇔ false ⇔ NOT NULL
 * 1 ⇔ true  ⇔ NULL
 */
template<std::size_t L>
requires (L > 0) and (L <= 16)
struct LocalBit<L> : Bit
{
    friend void swap(LocalBit &first, LocalBit &second) {
        using std::swap;
        swap(first.storage_, second.storage_);
    }

    friend struct Module; // to construct LocalBit

    private:
    using storage_type = detail::local_bit_storage<L>;
    storage_type storage_;

    LocalBit() = default;

    template<typename... Us>
    requires requires (Us&&... us) { storage_type(std::forward<Us>(us)...); }
    LocalBit(Us&&... us) : storage_(std::forward<Us>(us)...) { }

    public:
    LocalBit(const LocalBit&) = delete;
    LocalBit(LocalBit &&other) : LocalBit() { swap(*this, other); }

    /** Must not be defined out-of-line due to a LLVM clang issue, see https://bugs.llvm.org/show_bug.cgi?id=46979#c1. */
    ~LocalBit() {
        if constexpr (L == 1) {
            if (storage_.bitmap_) {
                M_insist((storage_.bitmap_->bitmask bitand mask()) == 0, "bit must still be allocated");

                if (storage_.bitmap_->bitmask == 0) // empty bitmap
                    Module::Get().local_bitmaps_stack_.back().emplace_back(storage_.bitmap_); // make discoverable again

                storage_.bitmap_->bitmask |= mask(); // deallocate bit
            }
        } else {
            if (storage_.bitvector_) {
                constexpr uint16_t MASK = (1U << L) - 1U;
                M_insist((storage_.bitvector_->bitmask_per_offset[offset()] bitand (MASK << starting_lane())) == 0,
                         "bits must still be allocated");

                const auto &bitmasks = storage_.bitvector_->bitmask_per_offset;
                auto pred = [](auto bitmask){ return bitmask == 0; };
                if (std::all_of(bitmasks.cbegin(), bitmasks.cend(), pred)) // empty bitvector
                    Module::Get().local_bitvectors_stack_.back().emplace_back(storage_.bitvector_); // make discoverable again

                storage_.bitvector_->bitmask_per_offset[offset()] |= MASK << starting_lane(); // deallocate bits
            }
        }
    }

    LocalBit & operator=(LocalBit &&other) { swap(*this, other); return *this; }

    private:
    ///> Returns the offset of the bits.
    std::conditional_t<L == 1, uint64_t, uint8_t> offset() const { return storage_.bit_offset_; }
    ///> Returns a mask with a single bit set at offset `offset()`.
    uint64_t mask() const requires (L == 1) { return 1UL << offset(); }
    ///> Returns a mask with a single bit set per lane at offset `offset()`.
    U8x16 mask() const requires (L > 1) { return U8x16(1U << offset()); }
    ///> Returns a mask with a single bit unset per lane at offset `offset()`.
    U8x16 mask_inverted() const requires (L > 1) { return U8x16(~(1U << offset())); }
    ///> Returns the lane index at which the L bits start.
    uint8_t starting_lane() const requires (L > 1) { return storage_.starting_lane_; }

    public:
    /** Returns the boolean expression that evaluates to `true` if the respective bits are set, `false` otherwise. */
    PrimitiveExpr<bool, L> is_set() const {
        if constexpr (L == 1) {
            return (storage_.bitmap_->u64 bitand mask()).template to<bool>();
        } else {
            if constexpr (L == 16) { // all lanes used
                M_insist(starting_lane() == 0);
                return (storage_.bitvector_->u8x16 bitand mask()).template to<bool>();
            } else if (starting_lane()) { // swizzle lanes to correct starting point
                std::array<uint8_t, L> indices;
                std::iota(indices.begin(), indices.end(), starting_lane()); // fill with [starting_lane(), starting_lane() + L)
                return (storage_.bitvector_->u8x16 bitand mask()).swizzle_bytes(indices).template to<bool>();
            } else { // no swizzling needed, but explicitly convert s.t. only first L lanes are used
                return PrimitiveExpr<bool, L>((storage_.bitvector_->u8x16 bitand mask()).template to<bool>().move());
            }
        }
    }

    /** Sets all bits. */
    void set() {
        if constexpr (L == 1)
            storage_.bitmap_->u64 |= mask();
        else
            storage_.bitvector_->u8x16 |= mask();
    }
    /** Clears all bits. */
    void clear() {
        if constexpr (L == 1)
            storage_.bitmap_->u64 &= ~mask();
        else
            storage_.bitvector_->u8x16 &= mask_inverted();
    }

    /** Sets these bits to the boolean values of \p value. */
    void set(PrimitiveExpr<bool, L> value) {
        if constexpr (L == 1) {
            storage_.bitmap_->u64 =
                (storage_.bitmap_->u64 bitand ~mask()) bitor (value.template to<uint64_t>() << offset());
        } else {
            if constexpr (L == 16) { // all lanes used
                M_insist(starting_lane() == 0);
                storage_.bitvector_->u8x16 = (storage_.bitvector_->u8x16 bitand mask_inverted()) bitor
                                             (value.template to<uint8_t>() << offset());
            } else if (starting_lane()) { // swizzle lanes to correct starting point, other lanes are 0
                std::array<uint8_t, 16> indices;
                auto it = std::fill_n(indices.begin(), starting_lane(), L); // fill range [0, starting_lane()) with L
                std::iota(it, it + L, 0); // fill range [starting_lane(), starting_lane() + L) with [0, L)
                std::fill(it + L, indices.end(), L); // fill range [starting_lane() + L, 16) with L
                storage_.bitvector_->u8x16 = (storage_.bitvector_->u8x16 bitand mask_inverted()) bitor
                                             (value.swizzle_bytes(indices).template to<uint8_t>() << offset());
            } else { // no swizzling needed, but explicitly mask s.t. last (unused) 16-L lanes are 0
                Boolx16 value_masked((PrimitiveExpr<bool, L>(true) and value).move());
                storage_.bitvector_->u8x16 = (storage_.bitvector_->u8x16 bitand mask_inverted()) bitor
                                             (value_masked.template to<uint8_t>() << offset());
            }
        }
    }

    /** Sets the bits of `this` to the values of the bits of \p other.  Cleverly computes required shift width at
     * compile time to use only a single shift operation. */
    LocalBit & operator=(const LocalBit &other) {
        if constexpr (L == 1) {
            auto other_bit = other.storage_.bitmap_->u64 bitand other.mask();
            Var<U64x1> this_bit;

            if (this->offset() > other.offset()) {
                const auto shift_width = this->offset() - other.offset();
                this_bit = other_bit << shift_width;
            } else if (other.offset() > this->offset()) {
                const auto shift_width = other.offset() - this->offset();
                this_bit = other_bit >> shift_width;
            } else {
                this_bit = other_bit;
            }

            this->storage_.bitmap_->u64 =
                (this->storage_.bitmap_->u64 bitand ~this->mask()) bitor this_bit; // clear, then set bit

            return *this;
        } else {
            auto other_bits = other.storage_.bitvector_->u8x16 bitand other.mask();
            Var<U8x16> this_bits;

            if (this->starting_lane() != other.starting_lane()) {
                std::array<uint8_t, 16> indices;
                auto it = std::fill_n(indices.begin(), starting_lane(), L); // fill range [0, starting_lane()) with L
                std::iota(it, it + L, 0); // fill range [starting_lane(), starting_lane() + L) with [0, L)
                std::fill(it + L, indices.end(), L); // fill range [starting_lane() + L, 16) with L
                this_bits = other_bits.swizzle_bytes(indices);
            } else {
                this_bits = other_bits;
            }

            if (this->offset() > other.offset()) {
                const auto shift_width = this->offset() - other.offset();
                this_bits = other_bits << shift_width;
            } else if (other.offset() > this->offset()) {
                const auto shift_width = other.offset() - this->offset();
                this_bits = other_bits >> shift_width;
            } else {
                this_bits = other_bits;
            }

            this->storage_.bitvector_->u8x16 =
                (this->storage_.bitvector_->u8x16 bitand this->mask_inverted()) bitor this_bits; // clear, then set bit

            return *this;
        }
    }

    /** Converts this `LocalBit` to `PrimitiveExpr<bool, L>`, which is `true` iff this `LocalBit` is set. */
    operator PrimitiveExpr<bool, L>() const { return is_set(); }
};


/*======================================================================================================================
 * Control flow
 *====================================================================================================================*/

/*----- Return unsafe, i.e. without static type checking -------------------------------------------------------------*/

inline void RETURN_UNSAFE() { Module::Get().emit_return(); }

template<primitive_convertible T>
inline void RETURN_UNSAFE(T &&t) { Module::Get().emit_return(primitive_expr_t<T>(std::forward<T>(t))); }

template<expr_convertible T>
requires (not primitive_convertible<T>)
inline void RETURN_UNSAFE(T &&t) { Module::Get().emit_return(expr_t<T>(std::forward<T>(t))); }

/*----- BREAK --------------------------------------------------------------------------------------------------------*/

inline void BREAK(std::size_t level = 1) { Module::Get().emit_break(level); }
template<primitive_convertible C>
requires requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
inline void BREAK(C &&_cond, std::size_t level = 1)
{
    PrimitiveExpr<bool, 1> cond(std::forward<C>(_cond));
    Module::Get().emit_break(cond, level);
}

/*----- CONTINUE -----------------------------------------------------------------------------------------------------*/

inline void CONTINUE(std::size_t level = 1) { Module::Get().emit_continue(level); }
template<primitive_convertible C>
requires requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
inline void CONTINUE(C &&_cond, std::size_t level = 1)
{
    PrimitiveExpr<bool, 1> cond(std::forward<C>(_cond));
    Module::Get().emit_continue(cond, level);
}

/*----- GOTO ---------------------------------------------------------------------------------------------------------*/

/** Jumps to the end of \p block. */
inline void GOTO(const Block &block) { block.go_to(); }
template<primitive_convertible C>
requires requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
/** Jumps to the end of \p block iff \p _cond is fulfilled. */
inline void GOTO(C &&_cond, const Block &block)
{
    PrimitiveExpr<bool, 1> cond(std::forward<C>(_cond));
    block.go_to(cond);
}

/*----- Select -------------------------------------------------------------------------------------------------------*/

template<primitive_convertible C, primitive_convertible T, primitive_convertible U>
requires have_common_type<typename primitive_expr_t<T>::type, typename primitive_expr_t<U>::type> and
         (primitive_expr_t<T>::num_simd_lanes == primitive_expr_t<U>::num_simd_lanes) and
requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
inline auto Select(C &&_cond, T &&_tru, U &&_fals)
{
    primitive_expr_t<T> tru(std::forward<T>(_tru));
    primitive_expr_t<U> fals(std::forward<U>(_fals));

    using To = common_type_t<typename decltype(tru)::type, typename decltype(fals)::type>;
    constexpr std::size_t L = decltype(tru)::num_simd_lanes;

    PrimitiveExpr<bool, 1> cond(std::forward<C>(_cond));
    return Module::Get().emit_select<To, L>(cond, tru.template to<To, L>(), fals.template to<To, L>());
}

template<primitive_convertible C, expr_convertible T, expr_convertible U>
requires (not primitive_convertible<T> or not primitive_convertible<U>) and
         have_common_type<typename expr_t<T>::type, typename expr_t<U>::type> and
         (expr_t<T>::num_simd_lanes == expr_t<U>::num_simd_lanes) and
requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
inline auto Select(C &&_cond, T &&_tru, U &&_fals)
{
    expr_t<T> tru(std::forward<T>(_tru));
    expr_t<U> fals(std::forward<U>(_fals));

    using To = common_type_t<typename decltype(tru)::type, typename decltype(fals)::type>;
    constexpr std::size_t L = decltype(tru)::num_simd_lanes;

    PrimitiveExpr<bool, 1> cond(std::forward<C>(_cond));
    return Module::Get().emit_select<To, L>(cond, tru.template to<To, L>(), fals.template to<To, L>());
}

template<primitive_convertible C, primitive_convertible T, primitive_convertible U>
requires have_common_type<typename primitive_expr_t<T>::type, typename primitive_expr_t<U>::type> and
         (primitive_expr_t<T>::num_simd_lanes == primitive_expr_t<U>::num_simd_lanes) and
         (primitive_expr_t<T>::num_simd_lanes > 1) and
requires (C &&c) { PrimitiveExpr<bool, primitive_expr_t<T>::num_simd_lanes>(std::forward<C>(c)); }
inline auto Select(C &&_cond, T &&_tru, U &&_fals)
{
    primitive_expr_t<T> tru(std::forward<T>(_tru));
    primitive_expr_t<U> fals(std::forward<U>(_fals));

    using To = common_type_t<typename decltype(tru)::type, typename decltype(fals)::type>;
    constexpr std::size_t L = decltype(tru)::num_simd_lanes;

    PrimitiveExpr<bool, L> cond(std::forward<C>(_cond));
    return Module::Get().emit_select<To, L>(cond, tru.template to<To, L>(), fals.template to<To, L>());
}

template<primitive_convertible C, expr_convertible T, expr_convertible U>
requires (not primitive_convertible<T> or not primitive_convertible<U>) and
         have_common_type<typename expr_t<T>::type, typename expr_t<U>::type> and
         (expr_t<T>::num_simd_lanes == expr_t<U>::num_simd_lanes) and (expr_t<T>::num_simd_lanes > 1) and
requires (C &&c) { PrimitiveExpr<bool, expr_t<T>::num_simd_lanes>(std::forward<C>(c)); }
inline auto Select(C &&_cond, T &&_tru, U &&_fals)
{
    expr_t<T> tru(std::forward<T>(_tru));
    expr_t<U> fals(std::forward<U>(_fals));

    using To = common_type_t<typename decltype(tru)::type, typename decltype(fals)::type>;
    constexpr std::size_t L = decltype(tru)::num_simd_lanes;

    PrimitiveExpr<bool, L> cond(std::forward<C>(_cond));
    return Module::Get().emit_select<To, L>(cond, tru.template to<To, L>(), fals.template to<To, L>());
}


/*----- Shuffle ------------------------------------------------------------------------------------------------------*/

template<primitive_convertible T, primitive_convertible U, std::size_t M>
requires have_common_type<typename primitive_expr_t<T>::type, typename primitive_expr_t<U>::type> and
         (primitive_expr_t<T>::num_simd_lanes == primitive_expr_t<U>::num_simd_lanes) and
requires (PrimitiveExpr<common_type_t<typename primitive_expr_t<T>::type, typename primitive_expr_t<U>::type>,
                        primitive_expr_t<T>::num_simd_lanes> e,
          const std::array<uint8_t, M> &a)
         { Module::Get().emit_shuffle_bytes(e, e, a); }
inline auto ShuffleBytes(T &&_first, U &&_second, const std::array<uint8_t, M> &indices)
{
    primitive_expr_t<T> first(std::forward<T>(_first));
    primitive_expr_t<U> second(std::forward<U>(_second));

    using To = common_type_t<typename decltype(first)::type, typename decltype(second)::type>;
    constexpr std::size_t L = decltype(first)::num_simd_lanes;

    return Module::Get().emit_shuffle_bytes<To, L>(first.template to<To, L>(), second.template to<To, L>(), indices);
}

template<primitive_convertible T, primitive_convertible U, std::size_t M>
requires have_common_type<typename primitive_expr_t<T>::type, typename primitive_expr_t<U>::type> and
         (primitive_expr_t<T>::num_simd_lanes == primitive_expr_t<U>::num_simd_lanes) and
requires (PrimitiveExpr<common_type_t<typename primitive_expr_t<T>::type, typename primitive_expr_t<U>::type>,
                        primitive_expr_t<T>::num_simd_lanes> e,
          const std::array<uint8_t, M> &a)
         { Module::Get().emit_shuffle_lanes(e, e, a); }
inline auto ShuffleLanes(T &&_first, U &&_second, const std::array<uint8_t, M> &indices)
{
    primitive_expr_t<T> first(std::forward<T>(_first));
    primitive_expr_t<U> second(std::forward<U>(_second));

    using To = common_type_t<typename decltype(first)::type, typename decltype(second)::type>;
    constexpr std::size_t L = decltype(first)::num_simd_lanes;

    return Module::Get().emit_shuffle_lanes<To, L>(first.template to<To, L>(), second.template to<To, L>(), indices);
}

template<expr_convertible T, expr_convertible U, std::size_t M>
requires (not primitive_convertible<T> or not primitive_convertible<U>) and
         have_common_type<typename expr_t<T>::type, typename expr_t<U>::type> and
         (expr_t<T>::num_simd_lanes == expr_t<U>::num_simd_lanes) and
requires (Expr<common_type_t<typename expr_t<T>::type, typename expr_t<U>::type>, expr_t<T>::num_simd_lanes> e,
          const std::array<uint8_t, M> &a)
         { Module::Get().emit_shuffle_bytes(e, e, a); }
inline auto ShuffleBytes(T &&_first, U &&_second, const std::array<uint8_t, M> &indices)
{
    expr_t<T> first(std::forward<T>(_first));
    expr_t<U> second(std::forward<U>(_second));

    using To = common_type_t<typename decltype(first)::type, typename decltype(second)::type>;
    constexpr std::size_t L = decltype(first)::num_simd_lanes;

    return Module::Get().emit_shuffle_bytes<To, L>(first.template to<To, L>(), second.template to<To, L>(), indices);
}

template<expr_convertible T, expr_convertible U, std::size_t M>
requires (not primitive_convertible<T> or not primitive_convertible<U>) and
         have_common_type<typename expr_t<T>::type, typename expr_t<U>::type> and
         (expr_t<T>::num_simd_lanes == expr_t<U>::num_simd_lanes) and
requires (Expr<common_type_t<typename expr_t<T>::type, typename expr_t<U>::type>, expr_t<T>::num_simd_lanes> e,
          const std::array<uint8_t, M> &a)
         { Module::Get().emit_shuffle_lanes(e, e, a); }
inline auto ShuffleLanes(T &&_first, U &&_second, const std::array<uint8_t, M> &indices)
{
    expr_t<T> first(std::forward<T>(_first));
    expr_t<U> second(std::forward<U>(_second));

    using To = common_type_t<typename decltype(first)::type, typename decltype(second)::type>;
    constexpr std::size_t L = decltype(first)::num_simd_lanes;

    return Module::Get().emit_shuffle_lanes<To, L>(first.template to<To, L>(), second.template to<To, L>(), indices);
}


/*----- If -----------------------------------------------------------------------------------------------------------*/

struct If
{
    using continuation_t = std::function<void(void)>;

    private:
    PrimitiveExpr<bool, 1> cond_;
    std::string name_;

    public:
    continuation_t Then, Else;

    template<primitive_convertible C>
    requires requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
    explicit If(C &&cond)
        : cond_(std::forward<C>(cond))
        , name_(Module::Unique_If_Name())
    { }

    If(const If&) = delete;
    If(If&&) = delete;

    ~If();
};

/*----- Loop ---------------------------------------------------------------------------------------------------------*/

/** Implements a loop which iterates exactly once unless explicitly `continue`-ed.  The loop may be exited by
 * explicitly `break`-ing out of it. */
struct Loop
{
    friend void swap(Loop &first, Loop &second) {
        using std::swap;
        swap(first.body_, second.body_);
        swap(first.loop_, second.loop_);
    }

    private:
    Block body_; ///< the loop body
    ::wasm::Loop *loop_ = nullptr; ///< the Binaryen loop

    private:
    /** Convenience c'tor accessible via tag-dispatching.  Expects an already unique \p name. */
    Loop(std::string name, tag<int>)
        : body_(name + ".body", false)
        , loop_(M_notnull(Module::Builder().makeLoop(name, &body_.get())))
    {
        Module::Get().push_branch_targets(
            /* brk=     */ body_.get().name,
            /* continu= */ loop_->name
        );
    }

    public:
    explicit Loop(std::string name) : Loop(Module::Unique_Loop_Name(name), tag<int>{}) { }
    explicit Loop(const char *name) : Loop(std::string(name)) { }

    Loop(const Loop&) = delete;
    Loop(Loop &&other) { swap(*this, other); }

    ~Loop() {
        if (loop_) {
            Module::Get().pop_branch_targets();
            Module::Block().list.push_back(loop_);
        }
    }

    Loop & operator=(Loop &&other) { swap(*this, other); return *this; }

    std::string name() const { return loop_->name.toString(); }

    Block & body() { return body_; }
    const Block & body() const { return body_; }
};

struct DoWhile : Loop
{
    template<primitive_convertible C>
    requires requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
    DoWhile(std::string name, C &&_cond)
        : Loop(name)
    {
        PrimitiveExpr<bool, 1> cond(std::forward<C>(_cond));

        /*----- Update condition in branch targets. -----*/
        auto branch_targets = Module::Get().pop_branch_targets();
        Module::Get().push_branch_targets(branch_targets.brk, branch_targets.continu, cond);
    }

    template<primitive_convertible C>
    requires requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
    DoWhile(const char *name, C &&cond) : DoWhile(std::string(name), cond) { }

    DoWhile(const DoWhile&) = delete;
    DoWhile(DoWhile&&) = default;

    ~DoWhile();
};

struct While
{
    private:
    PrimitiveExpr<bool, 1> cond_;
    std::unique_ptr<DoWhile> do_while_;

    public:
    While(std::string name, PrimitiveExpr<bool, 1> cond)
        : cond_(cond.clone())
        , do_while_(std::make_unique<DoWhile>(name + ".do-while", cond))
    { }

    template<primitive_convertible C>
    requires requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
    While(std::string name, C &&cond) : While(name, PrimitiveExpr<bool, 1>(std::forward<C>(cond))) { }

    template<primitive_convertible C>
    requires requires (C &&c) { PrimitiveExpr<bool, 1>(std::forward<C>(c)); }
    While(const char *name, C &&cond) : While(std::string(name), cond) { }

    While(const While&) = delete;
    While(While&&) = default;

    ~While();

    Block & body() { return do_while_->body(); }
    const Block & body() const { return do_while_->body(); }
};


/*======================================================================================================================
 * Allocator
 *====================================================================================================================*/

struct Allocator
{
    public:
    virtual ~Allocator() { }

    public:
    /** Pre-allocates memory for \p bytes consecutive bytes with alignment requirement \p align and returns a pointer
     * to the beginning of this memory. */
    virtual Ptr<void> pre_allocate(uint32_t bytes, uint32_t align = 1) = 0;
    /** Allocates memory for \p bytes consecutive bytes with alignment requirement \p align and returns a pointer to the
     * beginning of this memory. */
    virtual Var<Ptr<void>> allocate(U32x1 bytes, uint32_t align = 1) = 0;
    /** Deallocates the `bytes` consecutive bytes of allocated memory at address `ptr`. */
    virtual void deallocate(Ptr<void> ptr, U32x1 bytes) = 0;

    /** Performs the actual pre-allocations.  Must be called exactly **once** **after** the last pre-allocation was
     * requested. */
    virtual void perform_pre_allocations() = 0;

    Var<Ptr<void>> allocate(uint32_t bytes, uint32_t align = 1) { return allocate(U32x1(bytes), align); }
    void deallocate(Ptr<void> ptr, uint32_t bytes) { return deallocate(ptr, U32x1(bytes)); }

    /** Pre-allocates memory for exactly one value of type \tparam T and number of SIMD lanes \tparam L.  Returns a
     * pointer to this memory. */
    template<dsl_primitive T, std::size_t L = 1>
    Ptr<PrimitiveExpr<T, L>> pre_malloc() { return pre_malloc<T, L>(1U); }
    /** Allocates memory for exactly one value of type \tparam T and number of SIMD lanes \tparam L.  Returns a
     * pointer to this memory. */
    template<dsl_primitive T, std::size_t L = 1>
    Var<Ptr<PrimitiveExpr<T, L>>> malloc() { return malloc<T, L>(1U); }

    /** Pre-allocates memory for an array of \p count consecutive values of type \tparam T and number of SIMD lanes
     * \tparam L.  Returns a pointer to this memory. */
    template<dsl_primitive T, std::size_t L = 1>
    Ptr<PrimitiveExpr<T, L>> pre_malloc(uint32_t count) {
        if constexpr (L == 1)
            return pre_allocate(sizeof(T) * count, alignof(T)).template to<T*, L>();
        else if constexpr (L * sizeof(T) <= 16)
            return pre_allocate(16 * count, alignof(T)).template to<T*, L>();
        else
            return pre_allocate(16 * PrimitiveExpr<T, L>::num_vectors * count, alignof(T)).template to<T*, L>();
    }
    /** Allocates memory for an array of \p count consecutive values of type \tparam T and number of SIMD lanes
     * \tparam L.  Returns a pointer to this memory. */
    template<dsl_primitive T, std::size_t L = 1, typename U>
    requires requires (U &&u) { U32x1(std::forward<U>(u)); }
    Var<Ptr<PrimitiveExpr<T, L>>> malloc(U &&count) {
        if constexpr (L == 1) {
            Var<Ptr<PrimitiveExpr<T, L>>> ptr(
                allocate(uint32_t(sizeof(T)) * std::forward<U>(count), alignof(T)).template to<T*, L>()
            );
            return ptr;
        } else if constexpr (L * sizeof(T) <= 16) {
            Var<Ptr<PrimitiveExpr<T, L>>> ptr(
                allocate(16U * std::forward<U>(count), alignof(T)).template to<T*, L>()
            );
            return ptr;
        } else {
            Var<Ptr<PrimitiveExpr<T, L>>> ptr(
                allocate(
                    uint32_t(16 * PrimitiveExpr<T, L>::num_vectors) * std::forward<U>(count), alignof(T)
                ).template to<T*, L>()
            );
            return ptr;
        }
    }

    /** Frees exactly one value of type \tparam T of allocated memory pointed by \p ptr. */
    template<primitive_convertible T>
    requires requires (T &&t) { primitive_expr_t<T>(std::forward<T>(t)).template to<void*>(); }
    void free(T &&ptr) { free(std::forward<T>(ptr), 1U); }

    /** Frees \p count consecutive values of type \tparam T of allocated memory pointed by \p ptr. */
    template<primitive_convertible T, typename U>
    requires requires (U &&u) { U32x1(std::forward<U>(u)); } and
             requires (T &&t) { primitive_expr_t<T>(std::forward<T>(t)).template to<void*>(); }
    void free(T &&ptr, U &&count) {
        primitive_expr_t<T> _ptr(std::forward<T>(ptr));
        using pointed_type = typename decltype(_ptr)::pointed_type;
        constexpr std::size_t L = decltype(_ptr)::num_simd_lanes;
        if constexpr (L == 1)
            deallocate(_ptr.template to<void*>(), uint32_t(sizeof(pointed_type)) * std::forward<U>(count));
        else if constexpr (L * sizeof(T) <= 16)
            deallocate(_ptr.template to<void*>(), 16U * std::forward<U>(count));
        else
            deallocate(_ptr.template to<void*>(),
                       uint32_t(16 * PrimitiveExpr<T, L>::num_vectors) * std::forward<U>(count));
    }
};


/*######################################################################################################################
 * DELAYED DEFINITIONS
 *####################################################################################################################*/

/*======================================================================================================================
 * Module
 *====================================================================================================================*/

inline void Module::create_local_bitmap_stack()
{
    local_bitmaps_stack_.emplace_back();
}

inline void Module::create_local_bitvector_stack()
{
    local_bitvectors_stack_.emplace_back();
}

inline void Module::dispose_local_bitmap_stack()
{
    auto &local_bitmaps = local_bitmaps_stack_.back();
    for (LocalBitmap *bitmap : local_bitmaps) {
        M_insist(~bitmap->bitmask == 0, "all bits must have been deallocated");
        delete bitmap;
    }
    local_bitmaps_stack_.pop_back();
}

inline void Module::dispose_local_bitvector_stack()
{
    auto &local_bitvectors = local_bitvectors_stack_.back();
    for (LocalBitvector *bitvector : local_bitvectors) {
#ifndef NDEBUG
        for (auto bitmask : bitvector->bitmask_per_offset)
            M_insist(uint16_t(~bitmask) == 0, "all bits must have been deallocated");
#endif
        delete bitvector;
    }
    local_bitvectors_stack_.pop_back();
}

template<std::size_t L>
requires (L > 0) and (L <= 16)
inline LocalBit<L> Module::allocate_bit()
{
    if constexpr (L == 1) {
        auto &local_bitmaps = local_bitmaps_stack_.back();

        if (local_bitmaps.empty())
            local_bitmaps.emplace_back(new LocalBitmap()); // allocate new local bitmap in current function

        LocalBitmap &bitmap = *local_bitmaps.back();
        M_insist(bitmap.bitmask, "bitmap must have at least one bit unoccupied");

        uint8_t bit_offset = std::countr_zero(bitmap.bitmask);
        bitmap.bitmask ^= 1UL << bit_offset; // clear allocated bit

        LocalBit<L> bit(bitmap, bit_offset);

        if (bitmap.bitmask == 0) // all bits have been allocated
            local_bitmaps.pop_back(); // remove bitmap entry, ownership transitions to *all* referencing `LocalBit`s

        return bit;
    } else {
        bool fresh_bitvector = false;

        auto &local_bitvectors = local_bitvectors_stack_.back();

        if (local_bitvectors.empty()) {
allocate_bitvector:
            local_bitvectors.emplace_back(new LocalBitvector()); // allocate new local bitvector in current function
            fresh_bitvector = true;
        }

        LocalBitvector &bitvector = *local_bitvectors.back();

        uint8_t bit_offset;
        uint8_t starting_lane = uint8_t(-1U);
        constexpr uint16_t MASK = (1U << L) - 1U;
        for (uint8_t lane = 0; lane <= 16 - L; ++lane) {
            bit_offset = 0;
            for (uint16_t bitmask : bitvector.bitmask_per_offset) {
                const uint16_t masked = bitmask bitand (MASK << lane);
                if (masked == (MASK << lane)) { // `bit_offset`-th bit is free in L consecutive lanes
                    starting_lane = lane;
                    goto found_bits;
                }
                ++bit_offset;
            }
        }
found_bits:
        if (starting_lane == uint8_t(-1U)) {
            M_insist(not fresh_bitvector, "fresh bitvector must have at least L consecutive bits unoccupied");
            goto allocate_bitvector; // no bits found, retry with fresh bitvector
        }

        bitvector.bitmask_per_offset[bit_offset] ^= MASK << starting_lane; // clear allocated bits

        LocalBit<L> bit(bitvector, bit_offset, starting_lane);

        const auto &bitmasks = bitvector.bitmask_per_offset;
        auto pred = [](auto bitmask){ return bitmask == 0; };
        if (std::all_of(bitmasks.cbegin(), bitmasks.cend(), pred)) // all bits have been allocated
            local_bitvectors.pop_back(); // remove bitvector entry, ownership transitions to *all* referencing `LocalBit`s

        return bit;
    }
}

template<typename T, std::size_t L>
requires (L * sizeof(T) <= 16)
inline PrimitiveExpr<T, L> Module::get_global(const char *name)
{
    return PrimitiveExpr<T, L>(builder_.makeGlobalGet(name, wasm_type<T, L>()));
}

template<typename ReturnType, typename... ParamTypes, std::size_t... ParamLs>
requires std::is_void_v<ReturnType>
inline void Module::emit_call(const char *fn, PrimitiveExpr<ParamTypes, ParamLs>... args)
{
    active_block_->list.push_back(
        builder_.makeCall(fn, { args.expr()... }, wasm_type<ReturnType, 1>())
    );
}

template<typename ReturnType, std::size_t ReturnL, typename... ParamTypes, std::size_t... ParamLs>
requires dsl_primitive<ReturnType> or dsl_pointer_to_primitive<ReturnType>
inline PrimitiveExpr<ReturnType, ReturnL> Module::emit_call(const char *fn, PrimitiveExpr<ParamTypes, ParamLs>... args)
{
    return PrimitiveExpr<ReturnType, ReturnL>(
        builder_.makeCall(fn, { args.expr()... }, wasm_type<ReturnType, ReturnL>())
    );
}

inline void Module::emit_return()
{
    active_block_->list.push_back(builder_.makeReturn());
}

template<typename T, std::size_t L>
inline void Module::emit_return(PrimitiveExpr<T, L> value)
{
    active_block_->list.push_back(builder_.makeReturn(value.expr()));
}

template<typename T, std::size_t L>
inline void Module::emit_return(Expr<T, L> value)
{
    emit_return(value.insist_not_null());
}

/** Emit an unconditional break, breaking \p level levels. */
inline void Module::emit_break(std::size_t level)
{
    M_insist(level > 0);
    M_insist(branch_target_stack_.size() >= level);
    auto &branch_targets = branch_target_stack_[branch_target_stack_.size() - level];
    active_block_->list.push_back(builder_.makeBreak(branch_targets.brk));
}

/** Emit a conditional break, breaking if \p cond is `true` and breaking \p level levels. */
inline void Module::emit_break(PrimitiveExpr<bool, 1> cond, std::size_t level)
{
    M_insist(level > 0);
    M_insist(branch_target_stack_.size() >= level);
    auto &branch_targets = branch_target_stack_[branch_target_stack_.size() - level];
    active_block_->list.push_back(builder_.makeBreak(branch_targets.brk, nullptr, cond.expr()));
}

template<typename T, std::size_t L>
PrimitiveExpr<T, L> Module::emit_select(PrimitiveExpr<bool, 1> cond, PrimitiveExpr<T, L> tru, PrimitiveExpr<T, L> fals)
{
    if constexpr (L * sizeof(T) <= 16) {
        auto referenced_bits = cond.referenced_bits(); // moved
        referenced_bits.splice(referenced_bits.end(), tru.referenced_bits());
        referenced_bits.splice(referenced_bits.end(), fals.referenced_bits());
        return PrimitiveExpr<T, L>(
            /* expr=            */ builder_.makeSelect(cond.expr(), tru.expr(), fals.expr()),
            /* referenced_bits= */ std::move(referenced_bits)
        );
    } else {
        using ResT = PrimitiveExpr<T, L>;
        std::array<typename ResT::vector_type, ResT::num_vectors> vectors;
        auto vectors_tru  = tru.vectors();
        auto vectors_fals = fals.vectors();
        for (std::size_t idx = 0; idx < ResT::num_vectors; ++idx) {
            auto cond_cpy = cond.clone();
            auto referenced_bits = cond_cpy.referenced_bits(); // moved
            referenced_bits.splice(referenced_bits.end(), vectors_tru[idx].referenced_bits());
            referenced_bits.splice(referenced_bits.end(), vectors_fals[idx].referenced_bits());
            vectors[idx] = typename ResT::vector_type(
                /* expr=            */ builder_.makeSelect(cond_cpy.expr(), vectors_tru[idx].expr(),
                                                           vectors_fals[idx].expr()),
                /* referenced_bits= */ std::move(referenced_bits)
            );
        }
        cond.discard(); // since it was always cloned
        return ResT(std::move(vectors));
    }
}

template<typename T, std::size_t L>
Expr<T, L> Module::emit_select(PrimitiveExpr<bool, 1> cond, Expr<T, L> tru, Expr<T, L> fals)
{
    if (tru.can_be_null() or fals.can_be_null()) {
        auto [tru_val, tru_is_null] = tru.split();
        auto [fals_val, fals_is_null] = fals.split();
        auto cond_cpy = cond.clone();
        return Expr<T, L>(
            /* value=   */ emit_select(cond, tru_val, fals_val),
            /* is_null= */ emit_select(cond_cpy, tru_is_null, fals_is_null)
        );
    } else {
        return Expr<T, L>(
            /* value= */ emit_select(cond, tru.insist_not_null(), fals.insist_not_null())
        );
    }
}

template<typename T, std::size_t L>
requires (L > 1) and requires (PrimitiveExpr<int8_t, L> e) { e.template to<int_t<sizeof(T)>, L>(); }
PrimitiveExpr<T, L> Module::emit_select(PrimitiveExpr<bool, L> cond, PrimitiveExpr<T, L> tru, PrimitiveExpr<T, L> fals)
{
    using To = int_t<sizeof(T)>;

    PrimitiveExpr<int8_t, L> mask_i8(cond.move()); // convert without transforming `true`, i.e. 0xff, to 1
    PrimitiveExpr<To, L> mask = mask_i8.template to<To, L>(); // convert (w/ sign extension!) to same bit width as values

    if constexpr (L * sizeof(T) <= 16) {
        auto referenced_bits = mask.referenced_bits(); // moved
        referenced_bits.splice(referenced_bits.end(), tru.referenced_bits());
        referenced_bits.splice(referenced_bits.end(), fals.referenced_bits());
        return PrimitiveExpr<T, L>(
            /* expr=            */ builder_.makeSIMDTernary(::wasm::SIMDTernaryOp::Bitselect, tru.expr(), fals.expr(),
                                                            mask.expr()),
            /* referenced_bits= */ std::move(referenced_bits)
        );
    } else {
        using ResT = PrimitiveExpr<T, L>;
        std::array<typename ResT::vector_type, ResT::num_vectors> vectors;
        auto vectors_mask = mask.vectors();
        static_assert(ResT::num_vectors == vectors_mask.size());
        auto vectors_tru  = tru.vectors();
        auto vectors_fals = fals.vectors();
        for (std::size_t idx = 0; idx < ResT::num_vectors; ++idx) {
            auto referenced_bits = vectors_mask[idx].referenced_bits(); // moved
            referenced_bits.splice(referenced_bits.end(), vectors_tru[idx].referenced_bits());
            referenced_bits.splice(referenced_bits.end(), vectors_fals[idx].referenced_bits());
            vectors[idx] = typename ResT::vector_type(
                /* expr=            */ builder_.makeSIMDTernary(::wasm::SIMDTernaryOp::Bitselect,
                                                                vectors_tru[idx].expr(), vectors_fals[idx].expr(),
                                                                vectors_mask[idx].expr()),
                /* referenced_bits= */ std::move(referenced_bits)
            );
        }
        return ResT(std::move(vectors));
    }
}

template<typename T, std::size_t L>
requires (L > 1) and requires (PrimitiveExpr<int8_t, L> e) { e.template to<int_t<sizeof(T)>, L>(); }
Expr<T, L> Module::emit_select(PrimitiveExpr<bool, L> cond, Expr<T, L> tru, Expr<T, L> fals)
{
    if (tru.can_be_null() or fals.can_be_null()) {
        auto [tru_val, tru_is_null] = tru.split();
        auto [fals_val, fals_is_null] = fals.split();
        auto cond_cpy = cond.clone();
        return Expr<T, L>(
            /* value=   */ emit_select(cond, tru_val, fals_val),
            /* is_null= */ emit_select(cond_cpy, tru_is_null, fals_is_null)
        );
    } else {
        return Expr<T, L>(
            /* value= */ emit_select(cond, tru.insist_not_null(), fals.insist_not_null())
        );
    }
}

template<typename T, std::size_t L, std::size_t M>
requires (L > 1) and (L * sizeof(T) <= 16) and (M > 0) and (M <= 16) and (M % sizeof(T) == 0)
inline PrimitiveExpr<T, M / sizeof(T)>
Module::emit_shuffle_bytes(PrimitiveExpr<T, L> first, PrimitiveExpr<T, L> second,
                           const std::array<uint8_t, M> &_indices)
{
    std::array<uint8_t, 16> indices;
    for (std::size_t idx = 0; idx < M; ++idx) {
        if (_indices[idx] < L * sizeof(T))
            indices[idx] = _indices[idx];  // given byte of `first`
        else if (_indices[idx] < 2 * L * sizeof(T))
            indices[idx] = _indices[idx] + (16 - L * sizeof(T)); // shift to given byte of `second`
        else
            indices[idx] = 15; // last byte of `first`
    }
    std::fill(indices.begin() + M, indices.end(), 15); // last byte of `first`

    auto referenced_bits = first.referenced_bits(); // moved
    referenced_bits.splice(referenced_bits.end(), second.referenced_bits());
    auto vec = PrimitiveExpr<T, M / sizeof(T)>(
        /* expr=            */ builder_.makeSIMDShuffle(first.expr(), second.expr(), indices),
        /* referenced_bits= */ std::move(referenced_bits)
    );
    return M_CONSTEXPR_COND(decltype(vec)::num_simd_lanes == 1,
                            vec.template extract_unsafe<0>(), // extract a single value from vector to scalar
                            vec);
}

template<typename T, std::size_t L, std::size_t M>
requires (L > 1) and (L * sizeof(T) <= 16) and (M > 0) and (is_pow_2(M)) and (M * sizeof(T) <= 16)
inline PrimitiveExpr<T, M>
Module::emit_shuffle_lanes(PrimitiveExpr<T, L> first, PrimitiveExpr<T, L> second,
                           const std::array<uint8_t, M> &_indices)
{
    std::array<uint8_t, 16> indices;
    for (std::size_t idx = 0; idx < M; ++idx) {
        for (std::size_t byte = 0; byte < sizeof(T); ++byte) {
            if (_indices[idx] < L)
                indices[idx * sizeof(T) + byte] = _indices[idx] * sizeof(T) + byte;  // given lane of `first`
            else if (_indices[idx] < 2 * L)
                indices[idx * sizeof(T) + byte] =
                    _indices[idx] * sizeof(T) + byte + (16 - L * sizeof(T)); // shift to given lane of `second`
            else
                indices[idx * sizeof(T) + byte] = 15; // last byte of `first`
        }
    }
    std::fill(indices.begin() + M * sizeof(T), indices.end(), 15); // last byte of `first`

    auto referenced_bits = first.referenced_bits(); // moved
    referenced_bits.splice(referenced_bits.end(), second.referenced_bits());
    auto vec = PrimitiveExpr<T, M>(
        /* expr=            */ builder_.makeSIMDShuffle(first.expr(), second.expr(), indices),
        /* referenced_bits= */ std::move(referenced_bits)
    );
    return M_CONSTEXPR_COND(decltype(vec)::num_simd_lanes == 1,
                            vec.template extract_unsafe<0>(), // extract a single value from vector to scalar
                            vec);
}

template<typename T, std::size_t L, std::size_t M>
requires (L > 1) and (L * sizeof(T) <= 16) and (M > 0) and (M <= 16) and (M % sizeof(T) == 0) and (sizeof(T) == 1)
inline Expr<T, M / sizeof(T)>
Module::emit_shuffle_bytes(Expr<T, L> first, Expr<T, L> second, const std::array<uint8_t, M> &indices)
{
    if (first.can_be_null() or second.can_be_null()) {
        auto [first_val, first_is_null] = first.split();
        auto [second_val, second_is_null] = second.split();
        return Expr<T, M / sizeof(T)>(
            /* value=   */ emit_shuffle_bytes(first_val, second_val, indices),
            /* is_null= */ emit_shuffle_bytes(first_is_null, second_is_null, indices)
        );
    } else {
        return Expr<T, M / sizeof(T)>(
            /* value= */ emit_shuffle_bytes(first.insist_not_null(), second.insist_not_null(), indices)
        );
    }
}

template<typename T, std::size_t L, std::size_t M>
requires (L > 1) and (L * sizeof(T) <= 16) and (M > 0) and (is_pow_2(M)) and (M * sizeof(T) <= 16)
inline Expr<T, M>
Module::emit_shuffle_lanes(Expr<T, L> first, Expr<T, L> second, const std::array<uint8_t, M> &indices)
{
    if (first.can_be_null() or second.can_be_null()) {
        auto [first_val, first_is_null] = first.split();
        auto [second_val, second_is_null] = second.split();
        return Expr<T, M>(
            /* value=   */ emit_shuffle_lanes(first_val, second_val, indices),
            /* is_null= */ emit_shuffle_lanes(first_is_null, second_is_null, indices)
        );
    } else {
        return Expr<T, M>(
            /* value= */ emit_shuffle_lanes(first.insist_not_null(), second.insist_not_null(), indices)
        );
    }
}

inline void Module::push_branch_targets(::wasm::Name brk, ::wasm::Name continu, PrimitiveExpr<bool, 1> condition)
{
    branch_target_stack_.emplace_back(brk, continu, condition.expr());
}


/*======================================================================================================================
 * Block
 *====================================================================================================================*/

inline void Block::go_to(PrimitiveExpr<bool, 1> cond) const
{
    Module::Block().list.push_back(Module::Builder().makeBreak(get().name, nullptr, cond.expr()));
}


/*======================================================================================================================
 * Specialization of `variable_storage` for local, non-`NULL` boolean.
 *====================================================================================================================*/

namespace detail {

/*----- Specialization for local variables of boolean type that *cannot* be `NULL`. ----------------------------------*/

template<std::size_t L>
inline variable_storage<bool, VariableKind::Local, false, L>::variable_storage()
    : values_([](){
        std::array<std::shared_ptr<LocalBit<bit_num_simd_lanes>>, num_locals> values;
        for (std::size_t idx = 0; idx < num_locals; ++idx)
            values[idx] = std::make_shared<LocalBit<bit_num_simd_lanes>>(
                Module::Get().allocate_bit<bit_num_simd_lanes>()
            );
        return values;
    }())
{ }

template<std::size_t L>
template<primitive_convertible U>
requires requires (U &&u) { PrimitiveExpr<bool, L>(primitive_expr_t<U>(std::forward<U>(u))); }
void variable_storage<bool, VariableKind::Local, false, L>::operator=(U &&_value)
{
    if constexpr (num_locals == 1) {
        PrimitiveExpr<bool, L> value(primitive_expr_t<U>(std::forward<U>(_value)));
        values_[0]->set(value);
    } else {
        PrimitiveExpr<bool, L> value(primitive_expr_t<U>(std::forward<U>(_value)));
        static_assert(num_locals == decltype(value)::num_vectors);
        auto vectors = value.vectors();
        for (std::size_t idx = 0; idx < num_locals; ++idx)
            values_[idx]->set(vectors[idx]);
    }
}

template<std::size_t L>
inline variable_storage<bool, VariableKind::Local, false, L>::operator PrimitiveExpr<bool, L>() const
{
    if constexpr (num_locals == 1) {
        return PrimitiveExpr<bool, L>(/* expr= */ values_[0]->is_set().expr(), /* referenced_bits= */ { values_[0] });
    } else {
        static_assert(num_locals == PrimitiveExpr<bool, L>::num_vectors);
        std::array<typename PrimitiveExpr<bool, L>::vector_type, num_locals> vectors;
        for (std::size_t idx = 0; idx < num_locals; ++idx)
            vectors[idx] = typename PrimitiveExpr<bool, L>::vector_type(
                /* expr=            */ values_[idx]->is_set().expr(),
                /* referenced_bits= */ { values_[idx] }
            );
        return PrimitiveExpr<bool, L>(std::move(vectors));
    }
}

}

#undef UNARY_LIST
#undef BINARY_LIST
#undef ASSIGNOP_LIST


/*======================================================================================================================
 * explicit instantiation declarations
 *====================================================================================================================*/

extern template void Module::emit_insist(PrimitiveExpr<bool, 2>,  const char*, unsigned, const char*);
extern template void Module::emit_insist(PrimitiveExpr<bool, 4>,  const char*, unsigned, const char*);
extern template void Module::emit_insist(PrimitiveExpr<bool, 8>,  const char*, unsigned, const char*);
extern template void Module::emit_insist(PrimitiveExpr<bool, 16>, const char*, unsigned, const char*);
extern template void Module::emit_insist(PrimitiveExpr<bool, 32>, const char*, unsigned, const char*);

}

}
