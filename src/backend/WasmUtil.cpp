#include "backend/WasmUtil.hpp"

#include "backend/Interpreter.hpp"
#include "backend/WasmMacro.hpp"
#include <optional>


using namespace m;
using namespace m::storage;
using namespace m::wasm;


/*======================================================================================================================
 * Helper functions
 *====================================================================================================================*/

template<typename T>
void convert_in_place(SQL_t &operand)
{
    std::visit(overloaded {
        [&operand](auto &&actual) -> void requires requires { actual.template to<T>(); } {
            auto v = actual.template to<T>();
            operand.~SQL_t();
            if constexpr (std::same_as<T, char*>)
                new (&operand) SQL_t(NChar(v, static_cast<NChar>(actual).can_be_null(),
                                           static_cast<NChar>(actual).length(),
                                           static_cast<NChar>(actual).guarantees_terminating_nul()));
            else
                new (&operand) SQL_t(v);
        },
        [](auto &actual) -> void requires (not requires { actual.template to<T>(); }) {
            M_unreachable("illegal conversion");
        },
        [](std::monostate) -> void { M_unreachable("invalid variant"); },
    }, operand);
}

void convert_in_place(SQL_t &operand, const Type *to_type)
{
    visit(overloaded {
        [&operand](const Boolean&) -> void { convert_in_place<bool>(operand); },
        [&operand](const Numeric &n) -> void {
            switch (n.kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal:
                    switch (n.size()) {
                        default:
                            M_unreachable("invalid integer size");
                        case 8:
                            convert_in_place<int8_t>(operand);
                            return;
                        case 16:
                            convert_in_place<int16_t>(operand);
                            return;
                        case 32:
                            convert_in_place<int32_t>(operand);
                            return;
                        case 64:
                            convert_in_place<int64_t>(operand);
                            return;
                    }
                    break;
                case Numeric::N_Float:
                    if (n.size() <= 32)
                        convert_in_place<float>(operand);
                    else
                        convert_in_place<double>(operand);
                    break;
            }
        },
        [&operand](const CharacterSequence&) -> void { convert_in_place<char*>(operand); },
        [&operand](const Date&) -> void { convert_in_place<int32_t>(operand); },
        [&operand](const DateTime&) -> void { convert_in_place<int64_t>(operand); },
        [](auto&&) -> void { M_unreachable("illegal conversion"); },
    }, *to_type);
}

template<bool CanBeNull>
std::conditional_t<CanBeNull, _Bool, Bool> compile_cnf(ExprCompiler &C, const cnf::CNF &cnf)
{
    using result_t = std::conditional_t<CanBeNull, _Bool, Bool>;

    std::optional<result_t> wasm_cnf, wasm_clause;
    for (auto &clause : cnf) {
        wasm_clause.reset();
        for (auto &pred : clause) {
            /* Generate code for the literal of the predicate. */
            M_insist(pred.expr().type()->is_boolean());
            auto compiled = M_CONSTEXPR_COND(CanBeNull, C.compile<_Bool>(pred.expr()),
                                                        C.compile<_Bool>(pred.expr()).insist_not_null());
            auto wasm_pred = pred.negative() ? not compiled : compiled;

            /* Add the predicate to the clause with an `or`. */
            if (wasm_clause)
                wasm_clause.emplace(*wasm_clause or wasm_pred);
            else
                wasm_clause.emplace(wasm_pred);
        }
        M_insist(bool(wasm_clause), "empty clause?");

        /* Add the clause to the CNF with an `and`. */
        if (wasm_cnf)
            wasm_cnf.emplace(*wasm_cnf and *wasm_clause);
        else
            wasm_cnf.emplace(*wasm_clause);
    }
    M_insist(bool(wasm_cnf), "empty CNF?");

    return *wasm_cnf;
}


/*======================================================================================================================
 * ExprCompiler
 *====================================================================================================================*/

void ExprCompiler::operator()(const ast::ErrorExpr&) { M_unreachable("no errors at this stage"); }

void ExprCompiler::operator()(const ast::Designator &e)
{
    if (e.type()->is_none()) {
        set(_I32::Null()); // create NULL
        return;
    }

    /* Search with fully qualified name. */
    Schema::Identifier id(e.table_name.text, e.attr_name.text);
    set(env_.get(id));
}

void ExprCompiler::operator()(const ast::Constant &e)
{
    if (e.type()->is_none()) {
        set(_I32::Null()); // create NULL
        return;
    }

    /* Interpret constant. */
    auto value = Interpreter::eval(e);

    visit(overloaded {
        [this, &value](const Boolean&) { set(_Bool(value.as_b())); },
        [this, &value](const Numeric &n) {
            switch (n.kind) {
                case Numeric::N_Int:
                case Numeric::N_Decimal:
                    switch (n.size()) {
                        default:
                            M_unreachable("invalid integer size");
                        case 8:
                            set(_I8(value.as_i()));
                            break;
                        case 16:
                            set(_I16(value.as_i()));
                            break;
                        case 32:
                            set(_I32(value.as_i()));
                            break;
                        case 64:
                            set(_I64(value.as_i()));
                            break;
                    }
                    break;
                case Numeric::N_Float:
                    if (n.size() <= 32)
                        set(_Float(value.as_f()));
                    else
                        set(_Double(value.as_d()));
            }
        },
        [this, &value](const CharacterSequence&) {
            set(CodeGenContext::Get().get_literal_address(value.as<const char*>()));
        },
        [this, &value](const Date&) { set(_I32(value.as_i())); },
        [this, &value](const DateTime&) { set(_I64(value.as_i())); },
        [](const NoneType&) { M_unreachable("should've been handled earlier"); },
        [](auto&&) { M_unreachable("invalid type"); },
    }, *e.type());
}

void ExprCompiler::operator()(const ast::UnaryExpr &e)
{
    /* This is a helper to apply unary operations to `Expr<T>`s.  It uses SFINAE within `overloaded` to only apply the
     * operation if it is well typed, e.g. `+42` is ok whereas `+true` is not. */
    auto apply_unop = [this, &e](auto unop) {
        (*this)(*e.expr);
        std::visit(overloaded {
            [](std::monostate&&) -> void { M_unreachable("illegal value"); },
            [this, &unop](auto &&expr) -> void requires requires { unop(expr); } { set(unop(expr)); },
            [](auto &&expr) -> void requires (not requires { unop(expr); }) { M_unreachable("illegal operation"); },
        }, get());
    };

#define UNOP(OP) apply_unop(overloaded { \
        [](auto &&expr) -> decltype(expr.operator OP()) { return expr.operator OP(); }, \
    }); \
    break

    switch (e.op().type) {
        default:
            M_unreachable("invalid operator");

        case TK_PLUS:   UNOP(+);
        case TK_MINUS:  UNOP(-);
        case TK_TILDE:  UNOP(~);
        case TK_Not:    UNOP(not);
    }
#undef UNOP
}

void ExprCompiler::operator()(const ast::BinaryExpr &e)
{
    /* This is a helper to apply binary operations to `Expr<T>`s.  It uses SFINAE within `overloaded` to only apply the
     * operation if it is well typed, e.g. `42 + 13` is ok whereas `true + 42` is not. */
    auto apply_binop = [this, &e](auto binop) {
        (*this)(*e.lhs);
        SQL_t lhs = get();

        (*this)(*e.rhs);
        SQL_t rhs = get();

        if (e.common_operand_type) {
            convert_in_place(lhs, e.common_operand_type); // convert in-place
            convert_in_place(rhs, e.common_operand_type); // convert in-place
        }

        std::visit(overloaded {
            [](std::monostate&&) -> void { M_unreachable("illegal value"); },
            [this, &binop, &rhs](auto &&expr_lhs) -> void {
                std::visit(overloaded {
                    [](std::monostate&&) -> void { M_unreachable("illegal value"); },
                    [this, expr_lhs, &binop](auto &&expr_rhs) mutable -> void
                    requires requires { binop(expr_lhs, expr_rhs); }
                    {
                        set(binop(expr_lhs, expr_rhs));
                    },
                    [](auto &&expr_rhs) -> void
                    requires (not requires { binop(expr_lhs, expr_rhs); })
                    {
                        M_unreachable("illegal operation");
                    },
                }, rhs);
            },
        }, lhs);
    };

#define BINOP(OP) apply_binop( \
        [](auto lhs, auto rhs) -> decltype(lhs.operator OP(rhs)) { return lhs.operator OP(rhs); } \
    ); break
#define CMPOP(OP, STRCMP_OP) { \
        if (e.lhs->type()->is_character_sequence()) { \
            M_insist(e.rhs->type()->is_character_sequence()); \
            apply_binop( \
                [](NChar lhs, NChar rhs) -> _Bool { \
                    return strcmp(lhs, rhs, STRCMP_OP); \
                } \
            ); break; \
        } else { \
            BINOP(OP); \
        } \
    }

    switch (e.op().type) {
        default:
            M_unreachable("illegal token type");

        /*----- Arithmetic operations --------------------------------------------------------------------------------*/
        case TK_PLUS:           BINOP(+);
        case TK_MINUS:          BINOP(-);
        case TK_ASTERISK:       BINOP(*);
        case TK_SLASH:          BINOP(/);
        case TK_PERCENT:        BINOP(%);

        /*----- Comparison operations --------------------------------------------------------------------------------*/
        case TK_EQUAL:          CMPOP(==, EQ);
        case TK_BANG_EQUAL:     CMPOP(!=, NE);
        case TK_LESS:           CMPOP(<,  LT);
        case TK_LESS_EQUAL:     CMPOP(<=, LE);
        case TK_GREATER:        CMPOP(>,  GT);
        case TK_GREATER_EQUAL:  CMPOP(>=, GE);

        /*----- CharacterSequence operations -------------------------------------------------------------------------*/
        case TK_Like: {
            M_insist(e.lhs->type()->is_character_sequence());
            M_insist(e.rhs->type()->is_character_sequence());
            (*this)(*e.lhs);
            NChar str = get<NChar>();
            (*this)(*e.rhs);
            NChar pattern = get<NChar>();
            set(like(str, pattern));
            break;
        }

        case TK_DOTDOT: {
            M_insist(e.lhs->type()->is_character_sequence());
            M_insist(e.rhs->type()->is_character_sequence());
            (*this)(*e.lhs);
            NChar lhs = get<NChar>();
            (*this)(*e.rhs);
            NChar rhs = get<NChar>();

            M_insist(e.lhs->can_be_null() == lhs.can_be_null());
            M_insist(e.rhs->can_be_null() == rhs.can_be_null());

            Var<Ptr<Char>> res; // always set here
            bool res_can_be_null = lhs.can_be_null() or rhs.can_be_null();
            std::size_t res_length = lhs.length() + rhs.length() + 1; // allocate space for terminating NUL byte

            if (res_can_be_null) {
                auto [_ptr_lhs, is_nullptr_lhs] = lhs.split();
                auto [_ptr_rhs, is_nullptr_rhs] = rhs.split();
                Ptr<Char> ptr_lhs(_ptr_lhs), ptr_rhs(_ptr_rhs); // since structured bindings cannot be used in lambda capture

                IF (is_nullptr_lhs or is_nullptr_rhs) {
                    res = Ptr<Char>::Nullptr();
                } ELSE {
                    res = Module::Allocator().pre_malloc<char>(res_length); // create pre-allocation for result
                    Var<Ptr<Char>> ptr(strncpy(res, ptr_lhs, U32(lhs.length()))); // since res must not be changed
                    strncpy(ptr, ptr_rhs, U32(rhs.size_in_bytes())).discard(); // copy with possible terminating NUL byte
                    if (not rhs.guarantees_terminating_nul())
                        *ptr = '\0'; // terminate with NUL byte
                };
            } else {
                res = Module::Allocator().pre_malloc<char>(res_length); // create pre-allocation for result
                Var<Ptr<Char>> ptr(strncpy(res, lhs, U32(lhs.length()))); // since res must not be changed
                strncpy(ptr, rhs, U32(rhs.size_in_bytes())).discard(); // copy with possible terminating NUL byte
                if (not rhs.guarantees_terminating_nul())
                    *ptr = '\0'; // terminate with NUL byte
            }

            set(SQL_t(NChar(res, res_can_be_null, res_length, /* guarantees_terminating_nul= */ true)));
            break;
        }

        /*----- Logical operations -----------------------------------------------------------------------------------*/
        case TK_And:
        case TK_Or: {
            M_insist(e.lhs->type()->is_boolean());
            M_insist(e.rhs->type()->is_boolean());

            (*this)(*e.lhs);
            _Bool lhs = get<_Bool>();
            (*this)(*e.rhs);
            _Bool rhs = get<_Bool>();

            if (e.op().type == TK_And)
                set(lhs and rhs);
            else
                set(lhs or rhs);

            break;
        }
    }
#undef CMPOP
#undef BINOP
}

void ExprCompiler::operator()(const ast::FnApplicationExpr &e)
{
    switch (e.get_function().fnid) {
        default:
            M_unreachable("function kind not implemented");

        case m::Function::FN_UDF:
            M_unreachable("UDFs not yet supported");

        /*----- NULL check -------------------------------------------------------------------------------------------*/
        case m::Function::FN_ISNULL: {
            (*this)(*e.args[0]);
            auto arg = get();
            set(_Bool(is_null(arg)));
            break;
        }

        /*----- Type cast --------------------------------------------------------------------------------------------*/
        case m::Function::FN_INT: {
            (*this)(*e.args[0]);
            auto arg = get();
            set(convert<_I32>(arg));
            break;
        }

        /*----- Aggregate functions ----------------------------------------------------------------------------------*/
        case m::Function::FN_COUNT:
        case m::Function::FN_MIN:
        case m::Function::FN_MAX:
        case m::Function::FN_SUM:
        case m::Function::FN_AVG: {
            std::ostringstream oss;
            oss << e;
            Schema::Identifier id(Catalog::Get().pool(oss.str().c_str()));
            set(env_.get(id));
        }
    }
}

void ExprCompiler::operator()(const ast::QueryExpr &e)
{
    /* Search with fully qualified name. */
    Schema::Identifier id(e.alias(), Catalog::Get().pool("$res"));
    set(env_.get(id));
}

_Bool ExprCompiler::compile(const cnf::CNF &cnf)
{
    if (cnf.can_be_null())
        return compile_cnf<true>(*this, cnf);
    else
        return compile_cnf<false>(*this, cnf);
}



/*======================================================================================================================
 * Environment
 *====================================================================================================================*/

M_LCOV_EXCL_START
void Environment::dump(std::ostream &out) const
{
    out << "WasmEnvironment\n` entries: { ";
    for (auto it = exprs_.begin(), end = exprs_.end(); it != end; ++it) {
        if (it != exprs_.begin()) out << ", ";
        out << it->first;
    }
    out << " }" << std::endl;
}

void Environment::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * CodeGenContext
 *====================================================================================================================*/

thread_local std::unique_ptr<CodeGenContext> CodeGenContext::the_context_;


/*======================================================================================================================
 * compile data layout
 *====================================================================================================================*/

namespace m {

namespace wasm {

/** Compiles the data layout \p layout containing tuples of schema \p layout_schema such that it sequentially
 * stores/loads (depending on \tparam IsStore) tuples of schema \p tuple_schema starting at memory address \p
 * base_address and tuple ID \p initial_tuple_id.  If \tparam SinglePass, the store has to be done in a single pass,
 * i.e. the execution of the returned code must *not* be split among multiple function calls.  Otherwise, the store
 * does *not* have to be done in a single pass, i.e. the returned code may be emitted into a function which can be
 * called multiple times and each call starts storing at exactly the point where it has ended in the last call.
 * The caller has to provide a variable \p tuple_id which must be initialized to \p initial_tuple_id and will be
 * incremented automatically after storing/loading each tuple (i.e. code for this will be emitted at the end of the
 * block returned as second element).  Predication is supported and emitted respectively for storing tuples.
 *
 * Does not emit any code but returns three `wasm::Block`s containing code: the first one initializes all needed
 * variables, the second one stores/loads one tuple, and the third one advances to the next tuple. */
template<bool IsStore, bool SinglePass, VariableKind Kind>
std::tuple<Block, Block, Block>
compile_data_layout_sequential(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                               const Schema &layout_schema, Variable<uint32_t, Kind, false> &tuple_id,
                               uint32_t initial_tuple_id = 0)
{
    M_insist(tuple_schema.num_entries() != 0, "sequential access must access at least one tuple schema entry");

    /** code blocks for pointer/mask initialization, stores/loads of values, and stride jumps for pointers / updates
     * of masks */
    Block inits("inits", false), stores("stores", false), loads("loads", false), jumps("jumps", false);
    ///> the values loaded for the entries in `tuple_schema`
    SQL_t values[tuple_schema.num_entries()];
    ///> the NULL information loaded for the entries in `tuple_schema`
    Bool *null_bits;
    if constexpr (not IsStore)
        null_bits = static_cast<Bool*>(alloca(sizeof(Bool) * tuple_schema.num_entries()));

    using key_t = std::pair<uint8_t, uint64_t>;
    ///> variable type for pointers dependent on whether access should be done using a single pass
    using ptr_t = std::conditional_t<SinglePass, Var<Ptr<void>>, Global<Ptr<void>>>;
    ///> variable type for pointers dependent on whether access should be done using a single pass
    using mask_t = std::conditional_t<SinglePass, Var<U32>, Global<U32>>;
    struct value_t
    {
        ptr_t ptr;
        std::optional<mask_t> mask;
    };
    /** a map from bit offset (mod 8) and stride in bits to runtime pointer and mask; reset for each leaf; used to
     * share pointers between attributes of the same leaf that have equal stride and to share masks between attributes
     * of the same leaf that have equal offset (mod 8) */
    std::unordered_map<key_t, value_t> loading_context;

    auto &env = CodeGenContext::Get().env(); // the current codegen environment

    BLOCK_OPEN(inits) {
        Wasm_insist(tuple_id == initial_tuple_id, "initial value of tuple ID must be equal `initial_tuple_id`");
    }

    /*----- Check whether any of the entries in `tuple_schema` can be NULL, so that we need the NULL bitmap. -----*/
    const bool needs_null_bitmap = [&]() {
        for (auto &tuple_entry : tuple_schema) {
            M_insist(tuple_entry.nullable() == layout_schema[tuple_entry.id].second.nullable());
            if (tuple_entry.nullable()) return true; // found an entry in `tuple_schema` that can be NULL
        }
        return false; // no attribute in `schema` can be NULL
    }();
    bool has_null_bitmap = false; // indicates whether the data layout specifies a NULL bitmap

    /*----- If predication is used, introduce predication variable and update it before storing a tuple. -----*/
    const bool is_predicated = env.predicated();
    M_insist(not is_predicated or IsStore, "predication only supported for storing tuples");
    std::optional<Var<Bool>> pred;
    if (is_predicated) {
        BLOCK_OPEN(stores) {
            pred = env.extract_predicate().is_true_and_not_null();
        }
    }

    /*----- Visit the data layout. -----*/
    layout.for_sibling_leaves([&](const std::vector<DataLayout::leaf_info_t> &leaves,
                                  const DataLayout::level_info_stack_t &levels, uint64_t inode_offset_in_bits)
    {
        M_insist(inode_offset_in_bits % 8 == 0, "inode offset must be byte aligned");

        /*----- Clear the per-leaf data structure. -----*/
        loading_context.clear();

        /*----- Remember whether and where we found the NULL bitmap. -----*/
        std::optional<ptr_t> null_bitmap_ptr;
        std::optional<mask_t> null_bitmap_mask;
        uint8_t null_bitmap_bit_offset;
        uint64_t null_bitmap_stride_in_bits;

        /*----- Compute additional initial INode offset in bits depending on the given initial tuple ID. -----*/
        auto current_tuple_id = initial_tuple_id;
        uint64_t additional_inode_offset_in_bits = 0;
        for (auto &level : levels) {
            const auto child_iter = current_tuple_id / level.num_tuples;
            current_tuple_id = current_tuple_id % level.num_tuples;
            additional_inode_offset_in_bits += child_iter * level.stride_in_bits;
        }

        /*----- Iterate over sibling leaves, i.e. leaf children of a common parent INode, to emit code. -----*/
        for (auto &leaf_info : leaves) {
            const uint8_t bit_offset  = (additional_inode_offset_in_bits + leaf_info.offset_in_bits) % 8;
            const int32_t byte_offset = (additional_inode_offset_in_bits + leaf_info.offset_in_bits) / 8;

            const uint8_t bit_stride  = leaf_info.stride_in_bits % 8; // need byte stride later for the stride jumps

            if (leaf_info.leaf.index() == layout_schema.num_entries()) { // NULL bitmap
                if (not needs_null_bitmap)
                    continue;

                M_insist(not has_null_bitmap, "at most one bitmap may be specified");
                has_null_bitmap = true;
                if (bit_stride) { // NULL bitmap with bit stride requires dynamic masking
                    null_bitmap_bit_offset = bit_offset;
                    null_bitmap_stride_in_bits = leaf_info.stride_in_bits;
                    BLOCK_OPEN(inits) {
                        /*----- Initialize pointer and mask. -----*/
                        null_bitmap_ptr.emplace(); // default-construct for globals to be able to use assignment below
                        *null_bitmap_ptr = base_address.clone() + inode_offset_in_bits / 8 + byte_offset;
                        null_bitmap_mask = 1U << bit_offset;
                    }

                    /*----- Iterate over layout entries in *ascending* order. -----*/
                    std::size_t prev_layout_idx = 0; ///< remember the bit offset of the previously accessed NULL bit
                    for (std::size_t layout_idx = 0; layout_idx < layout_schema.num_entries(); ++layout_idx) {
                        auto &layout_entry = layout_schema[layout_idx];
                        if (layout_entry.nullable()) { // layout entry may be NULL
                            auto tuple_it = tuple_schema.find(layout_entry.id);
                            if (tuple_it == tuple_schema.end())
                                continue; // entry not contained in tuple schema
                            M_insist(prev_layout_idx == 0 or layout_idx > prev_layout_idx,
                                     "layout entries not processed in ascending order");
                            M_insist(*tuple_it->type == *layout_entry.type);
                            M_insist(tuple_it->nullable() == layout_entry.nullable());
                            const auto delta = layout_idx - prev_layout_idx;
                            const uint8_t bit_delta  = delta % 8;
                            const int32_t byte_delta = delta / 8;

                            auto advance_to_next_bit = [&]() {
                                if (bit_delta) {
                                    if (is_predicated) {
                                        M_insist(bool(pred));
                                        *null_bitmap_mask <<=
                                            Select(*pred, bit_delta, uint8_t(0)); // possibly advance mask
                                    } else {
                                        *null_bitmap_mask <<= bit_delta; // advance mask
                                    }
                                    /* If the mask surpasses the first byte, advance pointer to the next byte... */
                                    *null_bitmap_ptr += (*null_bitmap_mask bitand 0xffU).eqz().template to<int32_t>();
                                    /* ... and remove lowest byte from the mask. */
                                    *null_bitmap_mask = Select((*null_bitmap_mask bitand 0xffU).eqz(),
                                                               *null_bitmap_mask >> 8U, *null_bitmap_mask);
                                }
                                if (byte_delta) {
                                    if (is_predicated) {
                                        M_insist(bool(pred));
                                        *null_bitmap_ptr +=
                                            Select(*pred, byte_delta, 0); // possibly advance pointer
                                    } else {
                                        *null_bitmap_ptr += byte_delta; // advance pointer
                                    }
                                }
                            };

                            if constexpr (IsStore) {
                                /*----- Store NULL bit depending on its type. -----*/
                                auto store = [&]<typename T>() {
                                    BLOCK_OPEN(stores) {
                                        advance_to_next_bit();

                                        auto [value, is_null] = env.get<T>(tuple_it->id).split(); // get value
                                        value.discard(); // handled at entry leaf
                                        setbit(null_bitmap_ptr->template to<uint8_t*>(), is_null,
                                               null_bitmap_mask->template to<uint8_t>()); // update bit
                                    }
                                };
                                visit(overloaded{
                                    [&](const Boolean&) { store.template operator()<_Bool>(); },
                                    [&](const Numeric &n) {
                                        switch (n.kind) {
                                            case Numeric::N_Int:
                                            case Numeric::N_Decimal:
                                                switch (n.size()) {
                                                    default: M_unreachable("invalid size");
                                                    case  8: store.template operator()<_I8 >(); break;
                                                    case 16: store.template operator()<_I16>(); break;
                                                    case 32: store.template operator()<_I32>(); break;
                                                    case 64: store.template operator()<_I64>(); break;
                                                }
                                                break;
                                            case Numeric::N_Float:
                                                if (n.size() <= 32)
                                                    store.template operator()<_Float>();
                                                else
                                                    store.template operator()<_Double>();
                                        }
                                    },
                                    [&](const CharacterSequence&) {
                                        BLOCK_OPEN(stores) {
                                            advance_to_next_bit();

                                            auto value = env.get<NChar>(tuple_it->id); // get value
                                            M_insist(tuple_it->nullable() == value.can_be_null());
                                            setbit(null_bitmap_ptr->template to<uint8_t*>(), value.is_null(),
                                                   null_bitmap_mask->template to<uint8_t>()); // update bit
                                        }
                                    },
                                    [&](const Date&) { store.template operator()<_I32>(); },
                                    [&](const DateTime&) { store.template operator()<_I64>(); },
                                    [](auto&&) { M_unreachable("invalid type"); },
                                }, *tuple_it->type);
                            } else {
                                const auto tuple_idx = std::distance(tuple_schema.begin(), tuple_it);
                                BLOCK_OPEN(loads) {
                                    advance_to_next_bit();

                                    U8 byte = *null_bitmap_ptr->template to<uint8_t*>(); // load the byte
                                    Var<Bool> value(
                                        (byte bitand *null_bitmap_mask).template to<bool>()
                                    ); // mask bit with dynamic mask
                                    new (&null_bits[tuple_idx]) Bool(value);
                                }
                            }

                            prev_layout_idx = layout_idx;
                        } else { // layout entry must not be NULL
#ifndef NDEBUG
                            if constexpr (IsStore) {
                                /*----- Check that value is also not NULL. -----*/
                                auto check = [&]<typename T>() {
                                    Wasm_insist(env.get<T>(layout_entry.id).not_null(),
                                                "value of non-nullable entry must not be nullable");
                                };
                                visit(overloaded{
                                    [&](const Boolean&) { check.template operator()<_Bool>(); },
                                    [&](const Numeric &n) {
                                        switch (n.kind) {
                                            case Numeric::N_Int:
                                            case Numeric::N_Decimal:
                                                switch (n.size()) {
                                                    default: M_unreachable("invalid size");
                                                    case  8: check.template operator()<_I8 >(); break;
                                                    case 16: check.template operator()<_I16>(); break;
                                                    case 32: check.template operator()<_I32>(); break;
                                                    case 64: check.template operator()<_I64>(); break;
                                                }
                                                break;
                                            case Numeric::N_Float:
                                                if (n.size() <= 32)
                                                    check.template operator()<_Float>();
                                                else
                                                    check.template operator()<_Double>();
                                        }
                                    },
                                    [&](const CharacterSequence&) { check.template operator()<NChar>(); },
                                    [&](const Date&) { check.template operator()<_I32>(); },
                                    [&](const DateTime&) { check.template operator()<_I64>(); },
                                    [](auto&&) { M_unreachable("invalid type"); },
                                }, *layout_entry.type);
                            }
#endif
                        }
                    }

                    /*----- Final advancement of the pointer and mask to match the leaf's stride. -----*/
                    /* This is done here (and not together with the other stride jumps further below) since we only need
                     * to advance by `delta` bits since we already have advanced by `prev_layout_idx` bits. */
                    const auto delta = leaf_info.stride_in_bits - prev_layout_idx;
                    const uint8_t bit_delta  = delta % 8;
                    const int32_t byte_delta = delta / 8;
                    if (bit_delta) {
                        BLOCK_OPEN(jumps) {
                            if (is_predicated) {
                                M_insist(bool(pred));
                                *null_bitmap_mask <<= Select(*pred, bit_delta, uint8_t(0)); // possibly advance mask
                            } else {
                                *null_bitmap_mask <<= bit_delta; // advance mask
                            }
                            /* If the mask surpasses the first byte, advance pointer to the next byte... */
                            *null_bitmap_ptr += (*null_bitmap_mask bitand 0xffU).eqz().template to<int32_t>();
                            /* ... and remove the lowest byte from the mask. */
                            *null_bitmap_mask = Select((*null_bitmap_mask bitand 0xffU).eqz(),
                                                       *null_bitmap_mask >> 8U, *null_bitmap_mask);
                        }
                    }
                    if (byte_delta) {
                        BLOCK_OPEN(jumps) {
                            if (is_predicated) {
                                M_insist(bool(pred));
                                *null_bitmap_ptr += Select(*pred, byte_delta, 0); // possibly advance pointer
                            } else {
                                *null_bitmap_ptr += byte_delta; // advance pointer
                            }
                        }
                    }
                } else { // NULL bitmap without bit stride can benefit from static masking of NULL bits
                    auto [it, inserted] = loading_context.try_emplace(key_t(bit_offset, leaf_info.stride_in_bits));
                    if (inserted) {
                        BLOCK_OPEN(inits) {
                            it->second.ptr = base_address.clone() + inode_offset_in_bits / 8;
                        }
                    }
                    const auto &ptr = it->second.ptr;

                    /*----- For each tuple entry that can be NULL, create a store/load with static offset and mask. --*/
                    for (std::size_t tuple_idx = 0; tuple_idx != tuple_schema.num_entries(); ++tuple_idx) {
                        auto &tuple_entry = tuple_schema[tuple_idx];
                        M_insist(*tuple_entry.type == *layout_schema[tuple_entry.id].second.type);
                        M_insist(tuple_entry.nullable() == layout_schema[tuple_entry.id].second.nullable());
                        if (tuple_entry.nullable()) { // entry may be NULL
                            const auto &[layout_idx, layout_entry] = layout_schema[tuple_entry.id];
                            const uint8_t bit_offset =
                                (additional_inode_offset_in_bits + leaf_info.offset_in_bits + layout_idx) % 8;
                            const int32_t byte_offset =
                                (additional_inode_offset_in_bits + leaf_info.offset_in_bits + layout_idx) / 8;
                            if constexpr (IsStore) {
                                /*----- Store NULL bit depending on its type. -----*/
                                auto store = [&]<typename T>() {
                                    BLOCK_OPEN(stores) {
                                        auto [value, is_null] = env.get<T>(tuple_entry.id).split(); // get value
                                        value.discard(); // handled at entry leaf
                                        Ptr<U8> byte_ptr =
                                            (ptr + byte_offset).template to<uint8_t*>(); // compute byte address
                                        setbit<U8>(byte_ptr, is_null, bit_offset); // update bit
                                    }
                                };
                                visit(overloaded{
                                    [&](const Boolean&) { store.template operator()<_Bool>(); },
                                    [&](const Numeric &n) {
                                        switch (n.kind) {
                                            case Numeric::N_Int:
                                            case Numeric::N_Decimal:
                                                switch (n.size()) {
                                                    default: M_unreachable("invalid size");
                                                    case  8: store.template operator()<_I8 >(); break;
                                                    case 16: store.template operator()<_I16>(); break;
                                                    case 32: store.template operator()<_I32>(); break;
                                                    case 64: store.template operator()<_I64>(); break;
                                                }
                                                break;
                                            case Numeric::N_Float:
                                                if (n.size() <= 32)
                                                    store.template operator()<_Float>();
                                                else
                                                    store.template operator()<_Double>();
                                        }
                                    },
                                    [&](const CharacterSequence&) {
                                        BLOCK_OPEN(stores) {
                                            auto value = env.get<NChar>(tuple_entry.id); // get value
                                            M_insist(tuple_entry.nullable() == value.can_be_null());
                                            Ptr<U8> byte_ptr =
                                                (ptr + byte_offset).template to<uint8_t*>(); // compute byte address
                                            setbit<U8>(byte_ptr, value.is_null(), bit_offset); // update bit
                                        }
                                    },
                                    [&](const Date&) { store.template operator()<_I32>(); },
                                    [&](const DateTime&) { store.template operator()<_I64>(); },
                                    [](auto&&) { M_unreachable("invalid type"); },
                                }, *tuple_entry.type);
                            } else {
                                /*----- Load NULL bit. -----*/
                                BLOCK_OPEN(loads) {
                                    U8 byte = *(ptr + byte_offset).template to<uint8_t*>(); // load the byte
                                    const uint8_t static_mask = 1U << bit_offset;
                                    Var<Bool> value((byte bitand static_mask).to<bool>()); // mask bit with static mask
                                    new (&null_bits[tuple_idx]) Bool(value);
                                }
                            }
                        } else { // entry must not be NULL
#ifndef NDEBUG
                            if constexpr (IsStore) {
                                /*----- Check that value is also not NULL. -----*/
                                auto check = [&]<typename T>() {
                                    Wasm_insist(env.get<T>(tuple_entry.id).not_null(),
                                                "value of non-nullable entry must not be nullable");
                                };
                                visit(overloaded{
                                    [&](const Boolean&) { check.template operator()<_Bool>(); },
                                    [&](const Numeric &n) {
                                        switch (n.kind) {
                                            case Numeric::N_Int:
                                            case Numeric::N_Decimal:
                                                switch (n.size()) {
                                                    default: M_unreachable("invalid size");
                                                    case  8: check.template operator()<_I8 >(); break;
                                                    case 16: check.template operator()<_I16>(); break;
                                                    case 32: check.template operator()<_I32>(); break;
                                                    case 64: check.template operator()<_I64>(); break;
                                                }
                                                break;
                                            case Numeric::N_Float:
                                                if (n.size() <= 32)
                                                    check.template operator()<_Float>();
                                                else
                                                    check.template operator()<_Double>();
                                        }
                                    },
                                    [&](const CharacterSequence&) { check.template operator()<NChar>(); },
                                    [&](const Date&) { check.template operator()<_I32>(); },
                                    [&](const DateTime&) { check.template operator()<_I64>(); },
                                    [](auto&&) { M_unreachable("invalid type"); },
                                }, *tuple_entry.type);
                            }
#endif
                        }
                    }
                }
            } else { // regular entry
                auto &layout_entry = layout_schema[leaf_info.leaf.index()];
                M_insist(*layout_entry.type == *leaf_info.leaf.type());
                auto tuple_it = tuple_schema.find(layout_entry.id);
                if (tuple_it == tuple_schema.end())
                    continue; // entry not contained in tuple schema
                M_insist(*tuple_it->type == *layout_entry.type);
                const auto tuple_idx = std::distance(tuple_schema.begin(), tuple_it);

                auto [it, inserted] = loading_context.try_emplace(key_t(bit_offset, leaf_info.stride_in_bits));
                if (inserted) {
                    BLOCK_OPEN(inits) {
                        it->second.ptr = base_address.clone() + inode_offset_in_bits / 8;
                    }
                }
                const auto &ptr = it->second.ptr;

                if (bit_stride) { // entry with bit stride requires dynamic masking
                    M_insist(tuple_it->type->is_boolean(),
                             "leaf bit stride currently only for `Boolean` supported");

                    M_insist(inserted == not it->second.mask);
                    if (inserted) {
                        BLOCK_OPEN(inits) {
                            it->second.mask = 1U << bit_offset; // init mask
                        }
                    }
                    const mask_t &mask = *it->second.mask;

                    if constexpr (IsStore) {
                        /*----- Store value. -----*/
                        BLOCK_OPEN(stores) {
                            auto [value, is_null] = env.get<_Bool>(tuple_it->id).split(); // get value
                            is_null.discard(); // handled at NULL bitmap leaf
                            Ptr<U8> byte_ptr = (ptr + byte_offset).template to<uint8_t*>(); // compute byte address
                            setbit(byte_ptr, value, mask.template to<uint8_t>()); // update bit
                        }
                    } else {
                        /*----- Load value. -----*/
                        BLOCK_OPEN(loads) {
                            U8 byte = *(ptr + byte_offset).template to<uint8_t*>(); // load byte
                            Var<Bool> value((byte bitand mask).template to<bool>()); // mask bit with dynamic mask
                            new (&values[tuple_idx]) SQL_t(_Bool(value));
                        }
                    }
                } else { // entry without bit stride; if masking is required, we can use a static mask
                    /*----- Store value depending on its type. -----*/
                    auto store = [&]<typename T>() {
                        using type = typename T::type;
                        M_insist(bit_offset == 0,
                                 "leaf offset of `Numeric`, `Date`, or `DateTime` must be byte aligned");
                        BLOCK_OPEN(stores) {
                            auto [value, is_null] = env.get<T>(tuple_it->id).split();
                            is_null.discard(); // handled at NULL bitmap leaf
                            *(ptr + byte_offset).template to<type*>() = value;
                        }
                    };
                    /*----- Load value depending on its type. -----*/
                    auto load = [&]<typename T>() {
                        using type = typename T::type;
                        M_insist(bit_offset == 0,
                                 "leaf offset of `Numeric`, `Date`, or `DateTime` must be byte aligned");
                        BLOCK_OPEN(loads) {
                            Var<PrimitiveExpr<type>> value(*(ptr + byte_offset).template to<type*>());
                            new (&values[tuple_idx]) SQL_t(T(value));
                        }
                    };
                    /*----- Select call target (store or load) and visit attribute type. -----*/
#define CALL(TYPE) if constexpr (IsStore) store.template operator()<TYPE>(); else load.template operator()<TYPE>()
                    visit(overloaded{
                        [&](const Boolean&) {
                            if constexpr (IsStore) {
                                /*----- Store value. -----*/
                                BLOCK_OPEN(stores) {
                                    auto [value, is_null] = env.get<_Bool>(tuple_it->id).split(); // get value
                                    is_null.discard(); // handled at NULL bitmap leaf
                                    Ptr<U8> byte_ptr =
                                        (ptr + byte_offset).template to<uint8_t*>(); // compute byte address
                                    setbit<U8>(byte_ptr, value, bit_offset); // update bit
                                }
                            } else {
                                /*----- Load value. -----*/
                                BLOCK_OPEN(loads) {
                                    /* TODO: load byte once, create values with respective mask */
                                    U8 byte = *(ptr + byte_offset).template to<uint8_t*>(); // load byte
                                    const uint8_t static_mask = 1U << bit_offset;
                                    Var<Bool> value((byte bitand static_mask).to<bool>()); // mask bit with static mask
                                    new (&values[tuple_idx]) SQL_t(_Bool(value));
                                }
                            }
                        },
                        [&](const Numeric &n) {
                            switch (n.kind) {
                                case Numeric::N_Int:
                                case Numeric::N_Decimal:
                                    switch (n.size()) {
                                        default: M_unreachable("invalid size");
                                        case  8: CALL(_I8 ); break;
                                        case 16: CALL(_I16); break;
                                        case 32: CALL(_I32); break;
                                        case 64: CALL(_I64); break;
                                    }
                                    break;
                                case Numeric::N_Float:
                                    if (n.size() <= 32)
                                        CALL(_Float);
                                    else
                                        CALL(_Double);
                            }
                        },
                        [&](const CharacterSequence &cs) {
                            M_insist(bit_offset == 0, "leaf offset of `CharacterSequence` must be byte aligned");
                            if constexpr (IsStore) {
                                /*----- Store value. -----*/
                                BLOCK_OPEN(stores) {
                                    auto value = env.get<NChar>(tuple_it->id);
                                    M_insist(tuple_it->nullable() == value.can_be_null());
                                    IF (value.clone().not_null()) {
                                        Ptr<Char> address((ptr + byte_offset).template to<char*>());
                                        strncpy(address, value, U32(cs.size() / 8)).discard();
                                    };
                                }
                            } else {
                                /*----- Load value. -----*/
                                BLOCK_OPEN(loads) {
                                    Ptr<Char> address((ptr + byte_offset).template to<char*>());
                                    new (&values[tuple_idx]) SQL_t(
                                        NChar(address, tuple_it->nullable(), cs.length, cs.is_varying)
                                    );
                                }
                            }
                        },
                        [&](const Date&) { CALL(_I32); },
                        [&](const DateTime&) { CALL(_I64); },
                        [](auto&&) { M_unreachable("invalid type"); },
                    }, *tuple_it->type);
#undef CALL
                }
            }
        }

        /*----- Recursive lambda to emit stride jumps by processing path from leaves (excluding) to the root. -----*/
        auto emit_stride_jumps = [&](decltype(levels.crbegin()) curr, const decltype(levels.crend()) end) -> void {
            auto rec = [&](decltype(levels.crbegin()) curr, const decltype(levels.crend()) end, auto rec) -> void {
                if (curr == end) return;

                const auto inner = std::prev(curr); // the child INode of `curr`
                M_insist(curr->num_tuples % inner->num_tuples == 0, "curr must be whole multiple of inner");

                /*----- Compute remaining stride for this level. -----*/
                const auto num_repetition_inner = curr->num_tuples / inner->num_tuples;
                const auto stride_remaining_in_bits = curr->stride_in_bits -
                                                      num_repetition_inner * inner->stride_in_bits;
                M_insist(stride_remaining_in_bits % 8 == 0,
                         "remaining stride of INodes must be whole multiple of a byte");

                /*----- If there is a remaining stride for this level, emit conditional stride jump. -----*/
                if (const int32_t remaining_stride_in_bytes = stride_remaining_in_bits / 8) [[likely]] {
                    M_insist(curr->num_tuples > 0);
                    if (curr->num_tuples != 1U) {
                        Bool cond_mod = (tuple_id % uint32_t(curr->num_tuples)).eqz();
                        Bool cond_and = (tuple_id bitand uint32_t(curr->num_tuples - 1U)).eqz();
                        Bool cond = is_pow_2(curr->num_tuples) ? cond_and : cond_mod; // select implementation to use...
                        (is_pow_2(curr->num_tuples) ? cond_mod : cond_and).discard(); // ... and discard the other

                        /*----- Emit conditional stride jumps. -----*/
                        IF (cond) {
                            for (auto &[_, value] : loading_context) {
                                if (is_predicated) {
                                    M_insist(bool(pred));
                                    value.ptr += Select(*pred, remaining_stride_in_bytes, 0); // possibly emit stride jump
                                } else {
                                    value.ptr += remaining_stride_in_bytes; // emit stride jump
                                }
                            }
                            if (null_bitmap_ptr) {
                                if (is_predicated) {
                                    M_insist(bool(pred));
                                    *null_bitmap_ptr +=
                                        Select(*pred, remaining_stride_in_bytes, 0); // possibly emit stride jump
                                } else {
                                    *null_bitmap_ptr += remaining_stride_in_bytes; // emit stride jump
                                }
                            }

                            /*----- Recurse within IF. -----*/
                            rec(std::next(curr), end, rec);
                        };
                    } else {
                        for (auto &[_, value] : loading_context) {
                            if (is_predicated) {
                                M_insist(bool(pred));
                                value.ptr += Select(*pred, remaining_stride_in_bytes, 0); // possibly emit stride jump
                            } else {
                                value.ptr += remaining_stride_in_bytes; // emit stride jump
                            }
                        }
                        if (null_bitmap_ptr) {
                            if (is_predicated) {
                                M_insist(bool(pred));
                                *null_bitmap_ptr +=
                                    Select(*pred, remaining_stride_in_bytes, 0); // possibly emit stride jump
                            } else {
                                *null_bitmap_ptr += remaining_stride_in_bytes; // emit stride jump
                            }
                        }

                        /*----- Recurse within IF. -----*/
                        rec(std::next(curr), end, rec);
                    }
                } else {
                    /*----- Recurse without IF. -----*/
                    rec(std::next(curr), end, rec);
                }

            };
            rec(curr, end, rec);
        };

        /*----- Process path from DataLayout leaves to the root to emit stride jumps. -----*/
        BLOCK_OPEN(jumps) {
            /*----- Emit the per-leaf stride jumps, i.e. from one instance of the leaf to the next. -----*/
            for (auto &[key, value] : loading_context) {
                const uint8_t bit_stride  = key.second % 8;
                const int32_t byte_stride = key.second / 8;
                if (bit_stride) {
                    M_insist(bool(value.mask));
                    if (is_predicated) {
                        M_insist(bool(pred));
                        *value.mask <<= Select(*pred, bit_stride, uint8_t(0)); // possibly advance mask
                    } else {
                        *value.mask <<= bit_stride; // advance mask
                    }
                    /* If the mask surpasses the first byte, advance pointer to the next byte... */
                    value.ptr += (*value.mask bitand 0xffU).eqz().template to<int32_t>();
                    /* ... and remove the lowest byte from the mask. */
                    *value.mask = Select((*value.mask bitand 0xffU).eqz(), *value.mask >> 8U, *value.mask);
                }
                if (byte_stride) {
                    if (is_predicated) {
                        M_insist(bool(pred));
                        value.ptr += Select(*pred, byte_stride, 0); // possibly advance pointer
                    } else {
                        value.ptr += byte_stride; // advance pointer
                    }
                }
            }
            /* Omit the leaf stride jump for the NULL bitmap as it is already done together with the loading. */

            if (not levels.empty()) {
                /*----- Emit the stride jumps between each leaf to the beginning of the parent INode. -----*/
                Block lowest_inode_jumps(false);
                for (auto &[key, value] : loading_context) {
                    M_insist(levels.back().stride_in_bits % 8 == 0,
                             "stride of INodes must be multiples of a whole byte");
                    const auto stride_remaining_in_bits = levels.back().stride_in_bits -
                                                          levels.back().num_tuples * key.second;
                    const uint8_t remaining_bit_stride  = stride_remaining_in_bits % 8;
                    const int32_t remaining_byte_stride = stride_remaining_in_bits / 8;
                    if (remaining_bit_stride) [[likely]] {
                        M_insist(bool(value.mask));
                        BLOCK_OPEN(lowest_inode_jumps) {
                            const uint8_t end_bit_offset = (key.first + levels.back().num_tuples * key.second) % 8;
                            M_insist(end_bit_offset != key.first);
                            /* Reset the mask to initial bit offset... */
                            if (is_predicated) {
                                M_insist(bool(pred));
                                Wasm_insist(*pred or *value.mask == 1U << key.first,
                                            "if the predicate is not fulfilled, the mask should not be advanced");
                            }
                            *value.mask = 1U << key.first;
                            /* ... and advance pointer to next byte if resetting of the mask surpasses the current byte. */
                            if (is_predicated) {
                                M_insist(bool(pred));
                                value.ptr += Select(*pred, int32_t(end_bit_offset > key.first), 0);
                            } else {
                                value.ptr += int32_t(end_bit_offset > key.first);
                            }
                        }
                    }
                    if (remaining_byte_stride) [[likely]] {
                        BLOCK_OPEN(lowest_inode_jumps) {
                            if (is_predicated) {
                                M_insist(bool(pred));
                                value.ptr +=
                                    Select(*pred, remaining_byte_stride, 0); // possibly advance pointer
                            } else {
                                value.ptr += remaining_byte_stride; // advance pointer
                            }
                        }
                    }
                }
                if (null_bitmap_ptr) {
                    M_insist(bool(null_bitmap_mask));
                    M_insist(levels.back().stride_in_bits % 8 == 0,
                             "stride of INodes must be multiples of a whole byte");
                    const auto stride_remaining_in_bits = levels.back().stride_in_bits -
                                                          levels.back().num_tuples * null_bitmap_stride_in_bits;
                    const uint8_t remaining_bit_stride  = stride_remaining_in_bits % 8;
                    const int32_t remaining_byte_stride = stride_remaining_in_bits / 8;
                    if (remaining_bit_stride) [[likely]] {
                        BLOCK_OPEN(lowest_inode_jumps) {
                            const uint8_t end_bit_offset =
                                (null_bitmap_bit_offset + levels.back().num_tuples * null_bitmap_stride_in_bits) % 8;
                            M_insist(end_bit_offset != null_bitmap_bit_offset);
                            /* Reset the mask to initial bit offset... */
                            if (is_predicated) {
                                M_insist(bool(pred));
                                Wasm_insist(*pred or *null_bitmap_mask == 1U << null_bitmap_bit_offset,
                                            "if the predicate is not fulfilled, the mask should not be advanced");
                            }
                            *null_bitmap_mask = 1U << null_bitmap_bit_offset;
                            /* ... and advance pointer to next byte if resetting of the mask surpasses the current byte. */
                            if (is_predicated) {
                                M_insist(bool(pred));
                                *null_bitmap_ptr +=
                                    Select(*pred, int32_t(end_bit_offset > null_bitmap_bit_offset), 0);
                            } else {
                                *null_bitmap_ptr += int32_t(end_bit_offset > null_bitmap_bit_offset);
                            }
                        }
                    }
                    if (remaining_byte_stride) [[likely]] {
                        BLOCK_OPEN(lowest_inode_jumps) {
                            if (is_predicated) {
                                M_insist(bool(pred));
                                *null_bitmap_ptr +=
                                    Select(*pred, remaining_byte_stride, 0); // possibly advance pointer
                            } else {
                                *null_bitmap_ptr += remaining_byte_stride; // advance pointer
                            }
                        }
                    }
                }

                /*----- Emit the stride jumps between all INodes starting at the parent of leaves to the root. -----*/
                if (not lowest_inode_jumps.empty()) [[likely]] {
                    M_insist(levels.back().num_tuples > 0);
                    if (levels.back().num_tuples != 1U) {
                        Bool cond_mod = (tuple_id % uint32_t(levels.back().num_tuples)).eqz();
                        Bool cond_and = (tuple_id bitand uint32_t(levels.back().num_tuples - 1U)).eqz();
                        /* Select implementation to use... */
                        Bool cond = is_pow_2(levels.back().num_tuples) ? cond_and : cond_mod;
                        /* ... and discard the other. */
                        (is_pow_2(levels.back().num_tuples) ? cond_mod : cond_and).discard();

                        /*----- Emit conditional stride jumps from outermost Block. -----*/
                        IF (cond) {
                            lowest_inode_jumps.attach_to_current();

                            /*----- Recurse within IF. -----*/
                            emit_stride_jumps(std::next(levels.crbegin()), levels.crend());
                        };
                    } else {
                        lowest_inode_jumps.attach_to_current();

                        /*----- Recurse within IF. -----*/
                        emit_stride_jumps(std::next(levels.crbegin()), levels.crend());
                    }
                } else {
                    /*----- Recurse without outermost IF block. -----*/
                    emit_stride_jumps(std::next(levels.crbegin()), levels.crend());
                }
            }
        }
    });

    if constexpr (not IsStore) {
        /*----- Combine actual values and possible NULL bits to a new `SQL_t` and add this to the environment. -----*/
        for (std::size_t idx = 0; idx != tuple_schema.num_entries(); ++idx) {
            auto &tuple_entry = tuple_schema[idx];
            std::visit(overloaded{
                [&]<typename T>(Expr<T> value) {
                    BLOCK_OPEN(loads) {
                        M_insist(tuple_entry.nullable() == layout_schema[tuple_entry.id].second.nullable());
                        if (has_null_bitmap and tuple_entry.nullable()) {
                            Expr<T> combined(value.insist_not_null(), null_bits[idx]);
                            env.add(tuple_entry.id, combined);
                        } else {
                            env.add(tuple_entry.id, value);
                        }
                    }
                },
                [&](NChar value) {
                    BLOCK_OPEN(loads) {
                        M_insist(tuple_entry.nullable() == layout_schema[tuple_entry.id].second.nullable());
                        if (has_null_bitmap and tuple_entry.nullable()) {
                            /* introduce variable s.t. uses only load from it*/
                            Var<Ptr<Char>> combined(Select(null_bits[idx], Ptr<Char>::Nullptr(), value.val()));
                            env.add(tuple_entry.id, NChar(combined, /* can_be_null=*/ true, value.length(),
                                                          value.guarantees_terminating_nul()));
                        } else {
                            Var<Ptr<Char>> _value(value.val()); // introduce variable s.t. uses only load from it
                            env.add(tuple_entry.id, NChar(_value, /* can_be_null=*/ false, value.length(),
                                                          value.guarantees_terminating_nul()));
                        }
                    }
                },
                [](std::monostate) { M_unreachable("value must be loaded beforehand"); },
            }, values[idx]);
        }
    }

    /*----- Increment tuple ID after storing/loading one tuple. -----*/
    if constexpr (IsStore) {
        BLOCK_OPEN(stores) {
            if (is_predicated) {
                M_insist(bool(pred));
                tuple_id += pred->to<uint32_t>();
            } else {
                tuple_id += 1U;
            }
        }
    } else {
        BLOCK_OPEN(loads) {
            tuple_id += 1U;
        }
    }

    /*----- Destroy created values. -----*/
    for (std::size_t idx = 0; idx < tuple_schema.num_entries(); ++idx)
        values[idx].~SQL_t();
    if constexpr (not IsStore) {
        /*----- Destroy created NULL bits. -----*/
        for (std::size_t idx = 0; idx != tuple_schema.num_entries(); ++idx) {
            M_insist(tuple_schema[idx].nullable() == layout_schema[tuple_schema[idx].id].second.nullable());
            if (has_null_bitmap and tuple_schema[idx].nullable())
                null_bits[idx].~Bool();
        }
    }
    base_address.discard(); // discard base address (as it was always cloned)

#ifndef NDEBUG
    if constexpr (IsStore)
        M_insist(loads.empty());
    else
        M_insist(stores.empty());
#endif

    return { std::move(inits), M_CONSTEXPR_COND(IsStore, std::move(stores), std::move(loads)), std::move(jumps) };
}

}

}

template<VariableKind Kind>
std::tuple<m::wasm::Block, m::wasm::Block, m::wasm::Block>
m::wasm::compile_store_sequential(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                                  const Schema &layout_schema, Variable<uint32_t, Kind, false> &tuple_id,
                                  uint32_t initial_tuple_id)
{
    return compile_data_layout_sequential<true, false>(tuple_schema, base_address, layout, layout_schema, tuple_id,
                                                       initial_tuple_id);
}

template<VariableKind Kind>
std::tuple<m::wasm::Block, m::wasm::Block, m::wasm::Block>
m::wasm::compile_store_sequential_single_pass(const Schema &tuple_schema, Ptr<void> base_address,
                                              const storage::DataLayout &layout, const Schema &layout_schema,
                                              Variable<uint32_t, Kind, false> &tuple_id, uint32_t initial_tuple_id)
{
    return compile_data_layout_sequential<true, true>(tuple_schema, base_address, layout, layout_schema, tuple_id,
                                                      initial_tuple_id);
}

template<VariableKind Kind>
std::tuple<m::wasm::Block, m::wasm::Block, m::wasm::Block>
m::wasm::compile_load_sequential(const Schema &tuple_schema, Ptr<void> base_address, const storage::DataLayout &layout,
                                 const Schema &layout_schema, Variable<uint32_t, Kind, false> &tuple_id,
                                 uint32_t initial_tuple_id)
{
    return compile_data_layout_sequential<false, true>(tuple_schema, base_address, layout, layout_schema, tuple_id,
                                                       initial_tuple_id);
}

namespace m {

namespace wasm {

/** Compiles the data layout \p layout starting at memory address \p base_address and containing tuples of schema
 * \p layout_schema such that it stores/loads the single tuple with schema \p tuple_schema and ID \p tuple_id.
 *
 * If \tparam IsStore, emits the storing code into the current block, otherwise, emits the loading code into the
 * current block and adds the loaded values into the current environment. */
template<bool IsStore>
void compile_data_layout_point_access(const Schema &tuple_schema, Ptr<void> base_address,
                                      const storage::DataLayout &layout, const Schema &layout_schema, U32 tuple_id)
{
    M_insist(tuple_schema.num_entries() != 0, "point access must access at least one tuple schema entry");

    ///> the values loaded for the entries in `tuple_schema`
    SQL_t values[tuple_schema.num_entries()];
    ///> the NULL information loaded for the entries in `tuple_schema`
    Bool *null_bits;
    if constexpr (not IsStore)
        null_bits = static_cast<Bool*>(alloca(sizeof(Bool) * tuple_schema.num_entries()));

    auto &env = CodeGenContext::Get().env(); // the current codegen environment

    /*----- Check whether any of the entries in `tuple_schema` can be NULL, so that we need the NULL bitmap. -----*/
    const bool needs_null_bitmap = [&]() {
        for (auto &tuple_entry : tuple_schema) {
            M_insist(tuple_entry.nullable() == layout_schema[tuple_entry.id].second.nullable());
            if (tuple_entry.nullable()) return true; // found an entry in `tuple_schema` that can be NULL
        }
        return false; // no attribute in `schema` can be NULL
    }();
    bool has_null_bitmap = false; // indicates whether the data layout specifies a NULL bitmap

    /*----- Visit the data layout. -----*/
    layout.for_sibling_leaves([&](const std::vector<DataLayout::leaf_info_t> &leaves,
                                  const DataLayout::level_info_stack_t &levels, uint64_t inode_offset_in_bits)
    {
        M_insist(inode_offset_in_bits % 8 == 0, "inode offset must be byte aligned");

        /*----- Compute additional initial INode offset in bits depending on the given initial tuple ID. -----*/
        auto compute_additional_offset = [&](U32 tuple_id) -> U64 {
            auto rec = [&](U32 curr_tuple_id, decltype(levels.cbegin()) curr, const decltype(levels.cend()) end,
                           auto rec) -> U64
            {
                if (curr == end) {
                    curr_tuple_id.discard();
                    return U64(0);
                }

                if (is_pow_2(curr->num_tuples)) {
                    U32 child_iter = curr_tuple_id.clone() >> uint32_t(__builtin_ctzl(curr->num_tuples));
                    U32 inner_tuple_id = curr_tuple_id bitand uint32_t(curr->num_tuples - 1U);
                    U64 offset_in_bits = child_iter * curr->stride_in_bits;
                    return offset_in_bits + rec(inner_tuple_id, std::next(curr), end, rec);
                } else {
                    U32 child_iter = curr_tuple_id.clone() / uint32_t(curr->num_tuples);
                    U32 inner_tuple_id = curr_tuple_id % uint32_t(curr->num_tuples);
                    U64 offset_in_bits = child_iter * curr->stride_in_bits;
                    return offset_in_bits + rec(inner_tuple_id, std::next(curr), end, rec);
                }
            };
            return rec(tuple_id, levels.cbegin(), levels.cend(), rec);
        };
        Var<U64> additional_inode_offset_in_bits(compute_additional_offset(tuple_id));

        /*----- Iterate over sibling leaves, i.e. leaf children of a common parent INode, to emit code. -----*/
        for (auto &leaf_info : leaves) {
            if (leaf_info.leaf.index() == layout_schema.num_entries()) { // NULL bitmap
                if (not needs_null_bitmap)
                    continue;

                M_insist(not has_null_bitmap, "at most one bitmap may be specified");
                has_null_bitmap = true;

                Var<Ptr<void>> ptr(base_address.clone() + inode_offset_in_bits / 8); // pointer to NULL bitmap

                /*----- For each tuple entry that can be NULL, create a store/load with offset and mask. --*/
                for (std::size_t tuple_idx = 0; tuple_idx != tuple_schema.num_entries(); ++tuple_idx) {
                    auto &tuple_entry = tuple_schema[tuple_idx];
                    M_insist(*tuple_entry.type == *layout_schema[tuple_entry.id].second.type);
                    M_insist(tuple_entry.nullable() == layout_schema[tuple_entry.id].second.nullable());
                    if (tuple_entry.nullable()) { // entry may be NULL
                        const auto &[layout_idx, layout_entry] = layout_schema[tuple_entry.id];
                        auto offset_in_bits = additional_inode_offset_in_bits + (leaf_info.offset_in_bits + layout_idx);
                        U8  bit_offset  = (offset_in_bits.clone() bitand uint64_t(7)).to<uint8_t>(); // mod 8
                        I32 byte_offset = (offset_in_bits >> uint64_t(3)).make_signed().to<int32_t>(); // div 8
                        if constexpr (IsStore) {
                            /*----- Store NULL bit depending on its type. -----*/
                            auto store = [&]<typename T>() {
                                auto [value, is_null] = env.get<T>(tuple_entry.id).split(); // get value
                                value.discard(); // handled at entry leaf
                                Ptr<U8> byte_ptr = (ptr + byte_offset).template to<uint8_t*>(); // compute byte address
                                setbit<U8>(byte_ptr, is_null, uint8_t(1) << bit_offset); // update bit
                            };
                            visit(overloaded{
                                [&](const Boolean&) { store.template operator()<_Bool>(); },
                                [&](const Numeric &n) {
                                    switch (n.kind) {
                                        case Numeric::N_Int:
                                        case Numeric::N_Decimal:
                                            switch (n.size()) {
                                                default: M_unreachable("invalid size");
                                                case  8: store.template operator()<_I8 >(); break;
                                                case 16: store.template operator()<_I16>(); break;
                                                case 32: store.template operator()<_I32>(); break;
                                                case 64: store.template operator()<_I64>(); break;
                                            }
                                            break;
                                        case Numeric::N_Float:
                                            if (n.size() <= 32)
                                                store.template operator()<_Float>();
                                            else
                                                store.template operator()<_Double>();
                                    }
                                },
                                [&](const CharacterSequence&) {
                                    auto value = env.get<NChar>(tuple_entry.id); // get value
                                    M_insist(tuple_entry.nullable() == value.can_be_null());
                                    Ptr<U8> byte_ptr =
                                        (ptr + byte_offset).template to<uint8_t*>(); // compute byte address
                                    setbit<U8>(byte_ptr, value.is_null(), uint8_t(1) << bit_offset); // update bit
                                },
                                [&](const Date&) { store.template operator()<_I32>(); },
                                [&](const DateTime&) { store.template operator()<_I64>(); },
                                [](auto&&) { M_unreachable("invalid type"); },
                            }, *tuple_entry.type);
                        } else {
                            /*----- Load NULL bit. -----*/
                            U8 byte = *(ptr + byte_offset).template to<uint8_t*>(); // load the byte
                            Var<Bool> value((byte bitand (uint8_t(1) << bit_offset)).to<bool>()); // mask bit
                            new (&null_bits[tuple_idx]) Bool(value);
                        }
                    } else { // entry must not be NULL
#ifndef NDEBUG
                        if constexpr (IsStore) {
                            /*----- Check that value is also not NULL. -----*/
                            auto check = [&]<typename T>() {
                                Wasm_insist(env.get<T>(tuple_entry.id).not_null(),
                                            "value of non-nullable entry must not be nullable");
                            };
                            visit(overloaded{
                                [&](const Boolean&) { check.template operator()<_Bool>(); },
                                [&](const Numeric &n) {
                                    switch (n.kind) {
                                        case Numeric::N_Int:
                                        case Numeric::N_Decimal:
                                            switch (n.size()) {
                                                default: M_unreachable("invalid size");
                                                case  8: check.template operator()<_I8 >(); break;
                                                case 16: check.template operator()<_I16>(); break;
                                                case 32: check.template operator()<_I32>(); break;
                                                case 64: check.template operator()<_I64>(); break;
                                            }
                                            break;
                                        case Numeric::N_Float:
                                            if (n.size() <= 32)
                                                check.template operator()<_Float>();
                                            else
                                                check.template operator()<_Double>();
                                    }
                                },
                                [&](const CharacterSequence&) { check.template operator()<NChar>(); },
                                [&](const Date&) { check.template operator()<_I32>(); },
                                [&](const DateTime&) { check.template operator()<_I64>(); },
                                [](auto&&) { M_unreachable("invalid type"); },
                            }, *tuple_entry.type);
                        }
#endif
                    }
                }
            } else { // regular entry
                auto &layout_entry = layout_schema[leaf_info.leaf.index()];
                M_insist(*layout_entry.type == *leaf_info.leaf.type());
                auto tuple_it = tuple_schema.find(layout_entry.id);
                if (tuple_it == tuple_schema.end())
                    continue; // entry not contained in tuple schema
                M_insist(*tuple_it->type == *layout_entry.type);
                const auto tuple_idx = std::distance(tuple_schema.begin(), tuple_it);

                auto offset_in_bits = additional_inode_offset_in_bits + leaf_info.offset_in_bits;
                U8  bit_offset  = (offset_in_bits.clone() bitand uint64_t(7)).to<uint8_t>(); // mod 8
                I32 byte_offset = (offset_in_bits >> uint64_t(3)).make_signed().to<int32_t>(); // div 8

                Ptr<void> ptr = base_address.clone() + byte_offset + inode_offset_in_bits / 8; // pointer to entry

                /*----- Store value depending on its type. -----*/
                auto store = [&]<typename T>() {
                    using type = typename T::type;
                    Wasm_insist(bit_offset == 0U,
                                "leaf offset of `Numeric`, `Date`, or `DateTime` must be byte aligned");
                    auto [value, is_null] = env.get<T>(tuple_it->id).split();
                    is_null.discard(); // handled at NULL bitmap leaf
                    *ptr.template to<type*>() = value;
                };
                /*----- Load value depending on its type. -----*/
                auto load = [&]<typename T>() {
                    using type = typename T::type;
                    Wasm_insist(bit_offset == 0U,
                                "leaf offset of `Numeric`, `Date`, or `DateTime` must be byte aligned");
                    Var<PrimitiveExpr<type>> value(*ptr.template to<type*>());
                    new (&values[tuple_idx]) SQL_t(T(value));
                };
                /*----- Select call target (store or load) and visit attribute type. -----*/
#define CALL(TYPE) if constexpr (IsStore) store.template operator()<TYPE>(); else load.template operator()<TYPE>()
                visit(overloaded{
                    [&](const Boolean&) {
                        if constexpr (IsStore) {
                            /*----- Store value. -----*/
                            auto [value, is_null] = env.get<_Bool>(tuple_it->id).split(); // get value
                            is_null.discard(); // handled at NULL bitmap leaf
                            setbit<U8>(ptr.template to<uint8_t*>(), value, uint8_t(1) << bit_offset); // update bit
                        } else {
                            /*----- Load value. -----*/
                            /* TODO: load byte once, create values with respective mask */
                            U8 byte = *ptr.template to<uint8_t*>(); // load byte
                            Var<Bool> value((byte bitand (uint8_t(1) << bit_offset)).to<bool>()); // mask bit
                            new (&values[tuple_idx]) SQL_t(_Bool(value));
                        }
                    },
                    [&](const Numeric &n) {
                        switch (n.kind) {
                            case Numeric::N_Int:
                            case Numeric::N_Decimal:
                                switch (n.size()) {
                                    default: M_unreachable("invalid size");
                                    case  8: CALL(_I8 ); break;
                                    case 16: CALL(_I16); break;
                                    case 32: CALL(_I32); break;
                                    case 64: CALL(_I64); break;
                                }
                                break;
                            case Numeric::N_Float:
                                if (n.size() <= 32)
                                    CALL(_Float);
                                else
                                    CALL(_Double);
                        }
                    },
                    [&](const CharacterSequence &cs) {
                        Wasm_insist(bit_offset == 0U, "leaf offset of `CharacterSequence` must be byte aligned");
                        if constexpr (IsStore) {
                            /*----- Store value. -----*/
                            auto value = env.get<NChar>(tuple_it->id);
                            M_insist(tuple_it->nullable() == value.can_be_null());
                            IF (value.clone().not_null()) {
                                strncpy(ptr.template to<char*>(), value, U32(cs.size() / 8)).discard();
                            };
                        } else {
                            /*----- Load value. -----*/
                            new (&values[tuple_idx]) SQL_t(
                                NChar(ptr.template to<char*>(), tuple_it->nullable(), cs.length, cs.is_varying)
                            );
                        }
                    },
                    [&](const Date&) { CALL(_I32); },
                    [&](const DateTime&) { CALL(_I64); },
                    [](auto&&) { M_unreachable("invalid type"); },
                }, *tuple_it->type);
#undef CALL
            }
        }
    });

    if constexpr (not IsStore) {
        /*----- Combine actual values and possible NULL bits to a new `SQL_t` and add this to the environment. -----*/
        for (std::size_t idx = 0; idx != tuple_schema.num_entries(); ++idx) {
            auto &tuple_entry = tuple_schema[idx];
            std::visit(overloaded{
                [&]<typename T>(Expr<T> value) {
                    M_insist(tuple_entry.nullable() == layout_schema[tuple_entry.id].second.nullable());
                    if (has_null_bitmap and tuple_entry.nullable()) {
                        Expr<T> combined(value.insist_not_null(), null_bits[idx]);
                        env.add(tuple_entry.id, combined);
                    } else {
                        env.add(tuple_entry.id, value);
                    }
                },
                [&](NChar value) {
                    M_insist(tuple_entry.nullable() == layout_schema[tuple_entry.id].second.nullable());
                    if (has_null_bitmap and tuple_entry.nullable()) {
                        /* introduce variable s.t. uses only load from it */
                        Var<Ptr<Char>> combined(Select(null_bits[idx], Ptr<Char>::Nullptr(), value.val()));
                        env.add(tuple_entry.id, NChar(combined, /* can_be_null=*/ true, value.length(),
                                                      value.guarantees_terminating_nul()));
                    } else {
                        Var<Ptr<Char>> _value(value.val()); // introduce variable s.t. uses only load from it
                        env.add(tuple_entry.id, NChar(_value, /* can_be_null=*/ false, value.length(),
                                                      value.guarantees_terminating_nul()));
                    }
                },
                [](std::monostate) { M_unreachable("value must be loaded beforehand"); },
            }, values[idx]);
        }
    }

    /*----- Destroy created values. -----*/
    for (std::size_t idx = 0; idx < tuple_schema.num_entries(); ++idx)
        values[idx].~SQL_t();
    if constexpr (not IsStore) {
        /*----- Destroy created NULL bits. -----*/
        for (std::size_t idx = 0; idx != tuple_schema.num_entries(); ++idx) {
            M_insist(tuple_schema[idx].nullable() == layout_schema[tuple_schema[idx].id].second.nullable());
            if (has_null_bitmap and tuple_schema[idx].nullable())
                null_bits[idx].~Bool();
        }
    }
    base_address.discard(); // discard base address (as it was always cloned)
}

}

}

void m::wasm::compile_store_point_access(const Schema &tuple_schema, Ptr<void> base_address, const DataLayout &layout,
                                         const Schema &layout_schema, U32 tuple_id)
{
    return compile_data_layout_point_access<true>(tuple_schema, base_address, layout, layout_schema, tuple_id);
}

void m::wasm::compile_load_point_access(const Schema &tuple_schema, Ptr<void> base_address, const DataLayout &layout,
                                        const Schema &layout_schema, U32 tuple_id)
{
    return compile_data_layout_point_access<false>(tuple_schema, base_address, layout, layout_schema, tuple_id);
}


/*======================================================================================================================
 * Buffer
 *====================================================================================================================*/

template<bool IsGlobal>
Buffer<IsGlobal>::Buffer(const Schema &schema, const DataLayoutFactory &factory, std::size_t num_tuples,
                         setup_t setup, pipeline_t pipeline, teardown_t teardown)
    : schema_(std::cref(schema))
    , layout_(factory.make(schema, num_tuples))
    , setup_(std::move(setup))
    , pipeline_(std::move(pipeline))
    , teardown_(std::move(teardown))
{
    M_insist(schema.num_entries() != 0, "buffer schema must not be empty");

    if constexpr (IsGlobal) {
        if (layout_.is_finite()) {
            /*----- Pre-allocate memory for entire buffer. Use maximal possible alignment requirement of 8 bytes. ----*/
            const uint32_t child_size_in_bytes = (layout_.stride_in_bits() + 7) / 8;
            const uint32_t num_children =
                (layout_.num_tuples() + layout_.child().num_tuples() - 1) / layout_.child().num_tuples();
            storage_.base_address_ =
                Module::Allocator().pre_allocate(num_children * child_size_in_bytes, /* alignment= */ 8);
        } else {
            storage_.capacity_.emplace(); // create global for capacity
        }
    }
}

template<bool IsGlobal>
Buffer<IsGlobal>::~Buffer()
{
    if constexpr (IsGlobal) { // free memory of global buffer when object is destroyed and no use may occur later
        if (not layout_.is_finite()) {
            /*----- Deallocate memory for buffer. -----*/
            M_insist(bool(storage_.capacity_));
            const uint32_t child_size_in_bytes = (layout_.stride_in_bits() + 7) / 8;
            auto buffer_size_in_bytes =
                (*storage_.capacity_ / uint32_t(layout_.child().num_tuples())) * child_size_in_bytes;
            Module::Allocator().deallocate(storage_.base_address_, buffer_size_in_bytes);
        }
    }
}

template<bool IsGlobal>
buffer_load_proxy_t<IsGlobal> Buffer<IsGlobal>::create_load_proxy(param_t tuple_schema) const
{
    return tuple_schema ? buffer_load_proxy_t(*this, *tuple_schema) : buffer_load_proxy_t(*this, schema_);
}

template<bool IsGlobal>
buffer_store_proxy_t<IsGlobal> Buffer<IsGlobal>::create_store_proxy(param_t tuple_schema) const
{
    return tuple_schema ? buffer_store_proxy_t(*this, *tuple_schema) : buffer_store_proxy_t(*this, schema_);
}

template<bool IsGlobal>
buffer_swap_proxy_t<IsGlobal> Buffer<IsGlobal>::create_swap_proxy(param_t tuple_schema) const
{
    return tuple_schema ? buffer_swap_proxy_t(*this, *tuple_schema) : buffer_swap_proxy_t(*this, schema_);
}

template<bool IsGlobal>
void Buffer<IsGlobal>::setup()
{
    M_insist(not base_address_, "must not call `setup()` twice");
    M_insist(not size_, "must not call `setup()` twice");
    M_insist(not layout_.is_finite() == not capacity_, "must not call `setup()` twice");
    M_insist(not layout_.is_finite() == not first_iteration_, "must not call `setup()` twice");

    /*----- Create local variables. -----*/
    base_address_.emplace();
    size_.emplace();
    if (not layout_.is_finite()) {
        capacity_.emplace();
        first_iteration_.emplace(true); // set to true
    }

    /*----- For global buffers, read values from global backups into local variables. -----*/
    if constexpr (IsGlobal) {
        /* omit assigning base address here as it will always be set below */
        *size_ = storage_.size_;
        if (not layout_.is_finite()) {
            M_insist(bool(storage_.capacity_));
            *capacity_ = *storage_.capacity_;
        }
    }

    if (layout_.is_finite()) {
        if constexpr (IsGlobal) {
            *base_address_ = storage_.base_address_; // buffer always already pre-allocated
        } else {
            /*----- Pre-allocate memory for entire buffer. Use maximal possible alignment requirement of 8 bytes. ----*/
            const uint32_t child_size_in_bytes = (layout_.stride_in_bits() + 7) / 8;
            const uint32_t num_children =
                (layout_.num_tuples() + layout_.child().num_tuples() - 1) / layout_.child().num_tuples();
            *base_address_ = Module::Allocator().pre_allocate(num_children * child_size_in_bytes, /* alignment= */ 8);
        }
    } else {
        if constexpr (IsGlobal) {
            IF (*capacity_ == 0U) { // buffer not yet allocated
                /*----- Set initial capacity. -----*/
                *capacity_ = uint32_t(layout_.child().num_tuples());

                /*----- Allocate memory for one child instance. Use max. possible alignment requirement of 8 bytes. --*/
                const uint32_t child_size_in_bytes = (layout_.stride_in_bits() + 7) / 8;
                *base_address_ = Module::Allocator().allocate(child_size_in_bytes, /* alignment= */ 8);
            } ELSE {
                *base_address_ = storage_.base_address_;
            };
        } else {
            /*----- Set initial capacity. -----*/
            *capacity_ = uint32_t(layout_.child().num_tuples());

            /*----- Allocate memory for one child instance. Use max. possible alignment requirement of 8 bytes. -----*/
            const uint32_t child_size_in_bytes = (layout_.stride_in_bits() + 7) / 8;
            *base_address_ = Module::Allocator().allocate(child_size_in_bytes, /* alignment= */ 8);
        }
    }
}

template<bool IsGlobal>
void Buffer<IsGlobal>::teardown()
{
    M_insist(bool(base_address_), "must call `setup()` before");
    M_insist(bool(size_), "must call `setup()` before");
    M_insist(not layout_.is_finite() == bool(capacity_), "must call `setup()` before");

    if constexpr (not IsGlobal) { // free memory of local buffer when user calls teardown method
        if (not layout_.is_finite()) {
            /*----- Deallocate memory for buffer. -----*/
            const uint32_t child_size_in_bytes = (layout_.stride_in_bits() + 7) / 8;
            auto buffer_size_in_bytes = (*capacity_ / uint32_t(layout_.child().num_tuples())) * child_size_in_bytes;
            Module::Allocator().deallocate(*base_address_, buffer_size_in_bytes);
        }
    }

    /*----- For global buffers, write values from local variables into global backups. -----*/
    if constexpr (IsGlobal) {
        storage_.base_address_ = *base_address_;
        storage_.size_ = *size_;
        if (not layout_.is_finite()) {
            M_insist(bool(storage_.capacity_));
            *storage_.capacity_ = *capacity_;
        }
    }

    /*----- Destroy local variables. -----*/
    base_address_.reset();
    size_.reset();
    if (not layout_.is_finite()) {
        capacity_.reset();
        first_iteration_->val().discard(); // artificial use to silence diagnostics if `consume()` is not called
        first_iteration_.reset();
    }
}

template<bool IsGlobal>
void Buffer<IsGlobal>::resume_pipeline(param_t tuple_schema_)
{
    M_insist(bool(pipeline_), "pipeline must not be empty");

    const auto &tuple_schema = tuple_schema_ ? *tuple_schema_ : schema_;

#ifndef NDEBUG
    for (auto &e : tuple_schema.get())
        M_insist(schema_.get().find(e.id) != schema_.get().cend(), "tuple schema entry not found");
#endif

    /*----- Create function on-demand to assert that all needed identifiers are already created. -----*/
    if (not resume_pipeline_) {
        /*----- Create function to resume the pipeline for each tuple contained in the buffer. -----*/
        FUNCTION(resume_pipeline, void(void*, uint32_t))
        {
            auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

            /*----- Access base address and size parameters. -----*/
            Ptr<void> base_address = PARAMETER(0);
            U32 size = PARAMETER(1);

            /*----- Compile data layout to generate sequential load from buffer. -----*/
            Var<U32> load_tuple_id; // default initialized to 0
            auto [load_inits, loads, load_jumps] =
                compile_load_sequential(tuple_schema, base_address, layout_, schema_, load_tuple_id);

            /*----- Generate loop for loading entire buffer, with the pipeline emitted into the loop body. -----*/
            setup_();
            load_inits.attach_to_current();
            WHILE (load_tuple_id < size) {
                loads.attach_to_current();
                pipeline_();
                load_jumps.attach_to_current();
            }
            teardown_();
        }
        resume_pipeline_ = std::move(resume_pipeline);
    }

    /*----- Call created function. -----*/
    M_insist(bool(resume_pipeline_));
    (*resume_pipeline_)(base_address(), size()); // base address and size as arguments
}

template<bool IsGlobal>
void Buffer<IsGlobal>::resume_pipeline_inline(param_t tuple_schema_) const
{
    M_insist(bool(pipeline_), "pipeline must not be empty");

    const auto &tuple_schema = tuple_schema_ ? *tuple_schema_ : schema_;

#ifndef NDEBUG
    for (auto &e : tuple_schema.get())
        M_insist(schema_.get().find(e.id) != schema_.get().cend(), "tuple schema entry not found");
#endif

    /*----- Access base address and size depending on whether they are globals or locals. -----*/
    Ptr<void> base_address =
        M_CONSTEXPR_COND(IsGlobal,
                         base_address_ ? base_address_->val() : Var<Ptr<void>>(storage_.base_address_.val()).val(),
                         ({ M_insist(bool(base_address_)); base_address_->val(); }));
    U32 size =
        M_CONSTEXPR_COND(IsGlobal,
                         size_ ? size_->val() : Var<U32>(storage_.size_.val()).val(),
                         ({ M_insist(bool(size_)); size_->val(); }));

    /*----- If predication is used, compute number of tuples to load from buffer depending on predicate. -----*/
    std::optional<Var<Bool>> pred; // use variable since WHILE loop will clone it (for IF and DO_WHILE)
    if (auto &env = CodeGenContext::Get().env(); env.predicated())
        pred = env.extract_predicate().is_true_and_not_null();
    U32 num_tuples = pred ? Select(*pred, size, 0U) : size;

    /*----- Compile data layout to generate sequential load from buffer. -----*/
    Var<U32> load_tuple_id(0); // explicitly (re-)set tuple ID to 0
    auto [load_inits, loads, load_jumps] =
        compile_load_sequential(tuple_schema, base_address, layout_, schema_, load_tuple_id);

    /*----- Generate loop for loading entire buffer, with the pipeline emitted into the loop body. -----*/
    setup_();
    load_inits.attach_to_current();
    WHILE (load_tuple_id < num_tuples) {
        loads.attach_to_current();
        pipeline_();
        load_jumps.attach_to_current();
    }
    teardown_();
}

template<bool IsGlobal>
void Buffer<IsGlobal>::consume()
{
    M_insist(bool(base_address_), "must call `setup()` before");
    M_insist(bool(size_), "must call `setup()` before");
    M_insist(not layout_.is_finite() == bool(capacity_), "must call `setup()` before");
    M_insist(not layout_.is_finite() == bool(first_iteration_), "must call `setup()` before");

    /*----- Compile data layout to generate sequential single-pass store into the buffer. -----*/
    /* We are able to use a single-pass store, i.e. *local* pointers and masks, since we explicitly save the needed
     * variables, i.e. base address and size, using *global* backups and restore them before performing the actual
     * store in the case of global buffers.  For local buffers, stores must be done in a single pass anyway. */
    auto [_store_inits, stores, _store_jumps] =
        compile_store_sequential_single_pass(schema_, *base_address_, layout_, schema_, *size_);
    Block store_inits(std::move(_store_inits)), store_jumps(std::move(_store_jumps));

    if (layout_.is_finite()) {
        IF (*size_ == 0U) { // buffer empty
            /*----- Emit initialization code for storing (i.e. (re-)set to first buffer slot). -----*/
            store_inits.attach_to_current();
        };
    } else {
        IF (*first_iteration_) {
            /*----- Emit initialization code for storing (i.e. set to current buffer slot). -----*/
            store_inits.attach_to_current();

            *first_iteration_ = false;
        };
    }

    /*----- Emit storing code and increment size of buffer. -----*/
    stores.attach_to_current();

    if (layout_.is_finite()) {
        IF (*size_ == uint32_t(layout_.num_tuples())) { // buffer full
            /*----- Resume pipeline for each tuple in buffer and reset size of buffer to 0. -----*/
            resume_pipeline();
            *size_ = 0U;
        } ELSE { // buffer not full
            /*----- Emit advancing code to next buffer slot. -----*/
            store_jumps.attach_to_current();
        };
    } else {
        IF (*size_ == *capacity_) { // buffer full
            /*----- Resize buffer by doubling its capacity. -----*/
            const uint32_t child_size_in_bytes = (layout_.stride_in_bits() + 7) / 8;
            auto buffer_size_in_bytes = (*capacity_ / uint32_t(layout_.child().num_tuples())) * child_size_in_bytes;
            auto ptr = Module::Allocator().allocate(buffer_size_in_bytes.clone());
            Wasm_insist(ptr == *base_address_ + buffer_size_in_bytes.make_signed(),
                        "buffer could not be resized sequentially in memory");
            *capacity_ *= 2U;
        };

        /*----- Emit advancing code to next buffer slot. -----*/
        store_jumps.attach_to_current();
    }
}

// explicit instantiations to prevent linker errors
template struct m::wasm::Buffer<false>;
template struct m::wasm::Buffer<true>;


/*======================================================================================================================
 * buffer accesses
 *====================================================================================================================*/

template<bool IsGlobal>
void buffer_swap_proxy_t<IsGlobal>::operator()(U32 first, U32 second)
{
    /*---- Swap each entry individually to reduce number of variables needed at once. -----*/
    for (auto &e : schema_.get()) {
        /*----- Create schema for single entry and load and store proxies for it. -----*/
        Schema entry_schema;
        entry_schema.add(e.id, e.type, e.constraints);
        auto load  = buffer_.get().create_load_proxy(entry_schema);
        auto store = buffer_.get().create_store_proxy(entry_schema);

        /*----- Load entry of first tuple into fresh environment. -----*/
        auto env = [&](){
            auto S = CodeGenContext::Get().scoped_environment();
            load(first.clone());
            return S.extract();
        }();

        /*----- Temporarily save entry of first tuple by creating variable or separate string buffer. -----*/
        std::visit(overloaded {
            [&](NChar value) -> void {
                Var<Ptr<Char>> ptr; // always set here
                IF (value.clone().is_null()) {
                    ptr = Ptr<Char>::Nullptr();
                } ELSE {
                    ptr = Module::Allocator().pre_malloc<char>(value.size_in_bytes());
                    strncpy(ptr, value, U32(value.size_in_bytes())).discard();
                };
                env.add(e.id, NChar(ptr, value.can_be_null(), value.length(), value.guarantees_terminating_nul()));
            },
            [&]<typename T>(Expr<T> value) -> void {
                if (value.can_be_null()) {
                    Var<Expr<T>> var(value);
                    env.add(e.id, var);
                } else {
                    Var<PrimitiveExpr<T>> var(value.insist_not_null());
                    env.add(e.id, Expr<T>(var));
                }
            },
            [](std::monostate) -> void { M_unreachable("value must be loaded beforehand"); }
        }, env.extract(e.id));

        /*----- Load entry of second tuple in scoped environment and store it directly at first tuples address. -----*/
        {
            auto S = CodeGenContext::Get().scoped_environment();
            load(second.clone());
            store(first.clone());
        }

        /*----- Store temporarily saved entry of first tuple at second tuples address. ----*/
        {
            auto S = CodeGenContext::Get().scoped_environment(std::move(env));
            store(second.clone());
        }
    }

    first.discard(); // since it was always cloned
    second.discard(); // since it was always cloned
}

// explicit instantiations to prevent linker errors
template struct m::wasm::buffer_swap_proxy_t<false>;
template struct m::wasm::buffer_swap_proxy_t<true>;


/*======================================================================================================================
 * string comparison
 *====================================================================================================================*/

_I32 m::wasm::strncmp(NChar _left, NChar _right, U32 len)
{
    static thread_local struct {} _; // unique caller handle
    struct data_t : GarbageCollectedData
    {
        public:
        using fn_t = int32_t(uint32_t, uint32_t, char*, char*, uint32_t);
        std::optional<FunctionProxy<fn_t>> strncmp_terminating_nul;
        std::optional<FunctionProxy<fn_t>> strncmp_no_terminating_nul;

        data_t(GarbageCollectedData &&d) : GarbageCollectedData(std::move(d)) { }
    };
    auto &d = Module::Get().add_garbage_collected_data<data_t>(&_); // garbage collect the `data_t` instance

    auto strncmp_non_null = [&d, &_left, &_right](Ptr<Char> left, Ptr<Char> right, U32 len) -> I32 {
        Wasm_insist(left.clone().not_null(), "left operand must not be NULL");
        Wasm_insist(right.clone().not_null(), "right operand must not be NULL");
        Wasm_insist(len.clone() != 0U, "length to compare must not be 0");

        if (_left.length() == 1 and _right.length() == 1) {
            /*----- Special handling of single char strings. -----*/
            len.discard();
            auto left_gt_right = *left.clone() > *right.clone();
            return left_gt_right.to<int32_t>() - (*left < *right).to<int32_t>();
        } else {
            if (_left.guarantees_terminating_nul() and _right.guarantees_terminating_nul()) {
                if (not d.strncmp_terminating_nul) {
                    /*----- Create function to compute the result for non-nullptr arguments character-wise. -----*/
                    FUNCTION(strncmp_terminating_nul, data_t::fn_t)
                    {
                        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

                        const auto len_ty_left  = PARAMETER(0);
                        const auto len_ty_right = PARAMETER(1);
                        auto left  = PARAMETER(2);
                        auto right = PARAMETER(3);
                        const auto len = PARAMETER(4);

                        Var<I32> result; // always set here

                        I32 len_left  = Select(len < len_ty_left,  len, len_ty_left) .make_signed();
                        I32 len_right = Select(len < len_ty_right, len, len_ty_right).make_signed();
                        Var<Ptr<Char>> end_left (left  + len_left);
                        Var<Ptr<Char>> end_right(right + len_right);

                        LOOP() {
                            /* Check whether one side is shorter than the other. */
                            result = (left != end_left).to<int32_t>() - (right != end_right).to<int32_t>();
                            BREAK(result != 0 or left == end_left); // at the end of either or both strings

                            /* Compare by current character. Loading is valid since we have not seen the terminating
                             * NUL byte yet. */
                            result = (*left > *right).to<int32_t>() - (*left < *right).to<int32_t>();
                            BREAK(result != 0); // found first position where strings differ
                            BREAK(*left == 0); // reached end of identical strings

                            /* Advance to next character. */
                            left += 1;
                            right += 1;
                            CONTINUE();
                        }

                        RETURN(result);
                    }
                    d.strncmp_terminating_nul = std::move(strncmp_terminating_nul);
                }

                /*----- Call strncmp_terminating_nul function. ------*/
                M_insist(bool(d.strncmp_terminating_nul));
                return (*d.strncmp_terminating_nul)(_left.length(), _right.length(), left, right, len);
            } else {
                if (not d.strncmp_no_terminating_nul) {
                    /*----- Create function to compute the result for non-nullptr arguments character-wise. -----*/
                    FUNCTION(strncmp_no_terminating_nul, data_t::fn_t)
                    {
                        auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

                        const auto len_ty_left  = PARAMETER(0);
                        const auto len_ty_right = PARAMETER(1);
                        auto left  = PARAMETER(2);
                        auto right = PARAMETER(3);
                        const auto len = PARAMETER(4);

                        Var<I32> result; // always set here

                        I32 len_left  = Select(len < len_ty_left,  len, len_ty_left) .make_signed();
                        I32 len_right = Select(len < len_ty_right, len, len_ty_right).make_signed();
                        Var<Ptr<Char>> end_left (left  + len_left);
                        Var<Ptr<Char>> end_right(right + len_right);

                        LOOP() {
                            /* Check whether one side is shorter than the other. Load next character with in-bounds
                             * checks since the strings may not be NUL byte terminated. */
                            Var<Char> val_left, val_right;
                            IF (left != end_left) {
                                val_left = *left;
                            } ELSE {
                                val_left = '\0';
                            };
                            IF (right != end_right) {
                                val_right = *right;
                            } ELSE {
                                val_right = '\0';
                            };

                            /* Compare by current character. */
                            result = (val_left > val_right).to<int32_t>() - (val_left < val_right).to<int32_t>();
                            BREAK(result != 0); // found first position where strings differ
                            BREAK(val_left == 0); // reached end of identical strings

                            /* Advance to next character. */
                            left += 1;
                            right += 1;
                            CONTINUE();
                        }

                        RETURN(result);
                    }
                    d.strncmp_no_terminating_nul = std::move(strncmp_no_terminating_nul);
                }

                /*----- Call strncmp_no_terminating_nul function. ------*/
                M_insist(bool(d.strncmp_no_terminating_nul));
                return (*d.strncmp_no_terminating_nul)(_left.length(), _right.length(), left, right, len);
            }
        }
    };

    const Var<Ptr<Char>> left(_left.val()), right(_right.val());
    if (_left.can_be_null() or _right.can_be_null()) {
        _Var<I32> result; // always set here
        IF (left.is_null() or right.is_null()) {
            result = _I32::Null();
        } ELSE {
            result = strncmp_non_null(left, right, len);
        };
        return result;
    } else {
        const Var<I32> result(strncmp_non_null(left, right, len)); // to prevent duplicated computation due to `clone()`
        return _I32(result);
    }
}

_I32 m::wasm::strcmp(NChar left, NChar right)
{
    /* Delegate to `strncmp` with length set to minimum of both string lengths **plus** 1 since we need to check if
     * one string is a prefix of the other, i.e. all of its characters are equal but it is shorter than the other. */
    U32 len(std::min<uint32_t>(left.length(), left.length()) + 1U);
    return strncmp(left, right, len);
}

_Bool m::wasm::strncmp(NChar left, NChar right, U32 len, cmp_op op)
{
    _I32 res = strncmp(left, right, len);

    switch (op) {
        case EQ: return res == 0;
        case NE: return res != 0;
        case LT: return res <  0;
        case LE: return res <= 0;
        case GT: return res >  0;
        case GE: return res >= 0;
    }
}

_Bool m::wasm::strcmp(NChar left, NChar right, cmp_op op)
{
    _I32 res = strcmp(left, right);

    switch (op) {
        case EQ: return res == 0;
        case NE: return res != 0;
        case LT: return res <  0;
        case LE: return res <= 0;
        case GT: return res >  0;
        case GE: return res >= 0;
    }
}


/*======================================================================================================================
 * string copy
 *====================================================================================================================*/

Ptr<Char> m::wasm::strncpy(Ptr<Char> dst, Ptr<Char> src, U32 count)
{
    static thread_local struct {} _; // unique caller handle
    struct data_t : GarbageCollectedData
    {
        public:
        std::optional<FunctionProxy<char*(char*, char*, uint32_t)>> strncpy;

        data_t(GarbageCollectedData &&d) : GarbageCollectedData(std::move(d)) { }
    };
    auto &d = Module::Get().add_garbage_collected_data<data_t>(&_); // garbage collect the `data_t` instance

    if (not d.strncpy) {
        /*----- Create function to compute the result. -----*/
        FUNCTION(strncpy, char*(char*, char*, uint32_t))
        {
            auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

            auto dst = PARAMETER(0);
            auto src = PARAMETER(1);
            const auto count = PARAMETER(2);

            Wasm_insist(not src.is_nullptr(), "source must not be nullptr");
            Wasm_insist(not dst.is_nullptr(), "destination must not be nullptr");

            Var<Ptr<Char>> src_end(src + count.make_signed());
            WHILE (src != src_end) {
                *dst = *src;
                BREAK(*src == '\0'); // break on terminating NUL byte
                src += 1;
                dst += 1;
            }

            RETURN(dst);
        }
        d.strncpy = std::move(strncpy);
    }

    /*----- Call strncpy function. ------*/
    M_insist(bool(d.strncpy));
    const Var<Ptr<Char>> result((*d.strncpy)(dst, src, count)); // to prevent duplicated computation due to `clone()`
    return result;
}


/*======================================================================================================================
 * WasmLike
 *====================================================================================================================*/

_Bool m::wasm::like(NChar _str, NChar _pattern, const char escape_char)
{
    static thread_local struct {} _; // unique caller handle
    struct data_t : GarbageCollectedData
    {
        public:
        std::optional<FunctionProxy<bool(int32_t, int32_t, char*, char*, char)>> like;

        data_t(GarbageCollectedData &&d) : GarbageCollectedData(std::move(d)) { }
    };
    auto &d = Module::Get().add_garbage_collected_data<data_t>(&_); // garbage collect the `data_t` instance

    M_insist('_' != escape_char and '%' != escape_char, "illegal escape character");

    if (_str.length() == 0 and _pattern.length() == 0) {
        _str.discard();
        _pattern.discard();
        return _Bool(true);
    }

    auto like_non_null = [&d, &_str, &_pattern, &escape_char](Ptr<Char> str, Ptr<Char> pattern) -> Bool {
        Wasm_insist(str.clone().not_null(), "string operand must not be NULL");
        Wasm_insist(pattern.clone().not_null(), "pattern operand must not be NULL");

        if (not d.like) {
            /*----- Create function to compute the result. -----*/
            FUNCTION(like, bool(int32_t, int32_t, char*, char*, char))
            {
                auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for this function

                const auto len_ty_str = PARAMETER(0);
                const auto len_ty_pattern = PARAMETER(1);
                const auto val_str = PARAMETER(2);
                const auto val_pattern = PARAMETER(3);
                const auto escape_char = PARAMETER(4);

                /*----- Allocate memory for the dynamic programming table. -----*/
                /* Invariant: dp[i][j] == true iff val_pattern[:i] contains val_str[:j]. Row i and column j is located
                 * at dp + (i - 1) * (`length_str` + 1) + (j - 1). */
                I32 num_entries = (len_ty_str + 1) * (len_ty_pattern + 1);
                const Var<Ptr<Bool>> dp = Module::Allocator().malloc<bool>(num_entries.clone().make_unsigned());

                /*----- Initialize table with all entries set to false. -----*/
                Var<Ptr<Bool>> entry(dp.val());
                WHILE (entry < dp + num_entries.clone()) {
                    *entry = false;
                    entry += 1;
                }

                /*----- Reset entry pointer to first entry. -----*/
                entry = dp.val();

                /*----- Create pointers to track locations of current characters of `val_str` and `val_pattern`. -----*/
                Var<Ptr<Char>> str(val_str);
                Var<Ptr<Char>> pattern(val_pattern);

                /*----- Compute ends of str and pattern. -----*/
                /* Create constant local variables to ensure correct pointers since `src` and `pattern` will change. */
                const Var<Ptr<Char>> end_str(str + len_ty_str);
                const Var<Ptr<Char>> end_pattern(pattern + len_ty_pattern);

                /*----- Create variables for the current byte of str and pattern. -----*/
                Var<Char> byte_str, byte_pattern; // always loaded before first access

                /*----- Initialize first column. -----*/
                /* Iterate until current byte of pattern is not a `%`-wildcard and set the respective entries to true. */
                DO_WHILE (byte_pattern == '%') {
                    byte_pattern = Select(pattern < end_pattern, *pattern, '\0');
                    *entry = true;
                    entry += len_ty_str + 1;
                    pattern += 1;
                }

                /*----- Compute entire table. -----*/
                /* Create variable for the actual length of str. */
                Var<I32> len_str(0);

                /* Create flag whether the current byte of pattern is not escaped. */
                Var<Bool> is_not_escaped(true);

                /* Reset entry pointer to second row and second column. */
                entry = dp + len_ty_str + 2;

                /* Reset pattern to first character. */
                pattern = val_pattern;

                /* Load first byte from pattern if in bounds. */
                byte_pattern = Select(pattern < end_pattern, *pattern, '\0');

                /* Create loop iterating as long as the current byte of pattern is not NUL. */
                WHILE (byte_pattern != '\0') {
                    /* If current byte of pattern is not escaped and equals `escape_char`, advance pattern to next
                     * byte and load it. Additionally, mark this byte as escaped and check for invalid escape
                     * sequences. */
                    IF (is_not_escaped and byte_pattern == escape_char) {
                        pattern += 1;
                        byte_pattern = Select(pattern < end_pattern, *pattern, '\0');

                        /* Check whether current byte of pattern is a validly escaped character, i.e. `_`, `%` or
                         * `escape_char`. If not, throw an exception. */
                        IF (byte_pattern != '_' and byte_pattern != '%' and byte_pattern != escape_char) {
                            Throw(exception::invalid_escape_sequence);
                        };

                        is_not_escaped = false;
                    };

                    /* Reset actual length of str. */
                    len_str = 0;

                    /* Load first byte from str if in bounds. */
                    byte_str = Select(str < end_str, *str, '\0');

                    /* Create loop iterating as long as the current byte of str is not NUL. */
                    WHILE (byte_str != '\0') {
                        /* Increment actual length of str. */
                        len_str += 1;

                        IF (is_not_escaped and byte_pattern == '%') {
                            /* Store disjunction of above and left entry. */
                            *entry = *(entry - (len_ty_str + 1)) or *(entry - 1);
                        } ELSE {
                            IF ((is_not_escaped and byte_pattern == '_') or byte_pattern == byte_str) {
                                /* Store above left entry. */
                                *entry = *(entry - (len_ty_str + 2));
                            };
                        };

                        /* Advance entry pointer to next entry, advance str to next byte, and load next byte from str
                         * if in bounds. */
                        entry += 1;
                        str += 1;
                        byte_str = Select(str < end_str, *str, '\0');
                    }

                    /* Advance entry pointer to second column in the next row, reset str to first character, advance
                     * pattern to next byte, load next byte from pattern if in bounds, and reset is_not_escaped to
                     * true. */
                    entry += len_ty_str + 1 - len_str;
                    str = val_str;
                    pattern += 1;
                    byte_pattern = Select(pattern < end_pattern, *pattern, '\0');
                    is_not_escaped = true;
                }

                /*----- Compute result. -----*/
                /* Entry pointer points currently to the second column in the first row after the pattern has ended.
                 * Therefore, we have to go one row up and len_str - 1 columns to the right, i.e. the result is
                 * located at entry - (`length_str` + 1) + len_str - 1 = entry + len_str - (`length_str` + 2). */
                const Var<Bool> result(*(entry + len_str - (len_ty_str + 2)));

                /*----- Free allocated space. -----*/
                Module::Allocator().free(dp, num_entries.make_unsigned());

                RETURN(result);
            }

            d.like = std::move(like);
        }

        /*----- Call like function. ------*/
        M_insist(bool(d.like));
        return (*d.like)(_str.length(), _pattern.length(), str, pattern, escape_char);
    };

    if (_str.can_be_null() or _pattern.can_be_null()) {
        auto [_val_str, is_null_str] = _str.split();
        auto [_val_pattern, is_null_pattern] = _pattern.split();
        Ptr<Char> val_str(_val_str), val_pattern(_val_pattern); // since structured bindings cannot be used in lambda capture

        _Var<Bool> result; // always set here
        IF (is_null_str or is_null_pattern) {
            result = _Bool::Null();
        } ELSE {
            result = like_non_null(val_str, val_pattern);
        };
        return result;
    } else {
        const Var<Bool> result(like_non_null(_str, _pattern)); // to prevent duplicated computation due to `clone()`
        return _Bool(result);
    }
}


/*======================================================================================================================
 * comparator
 *====================================================================================================================*/

I32 m::wasm::compare(const Environment &env_left, const Environment &env_right,
                     const std::vector<SortingOperator::order_type> &order)
{
    Var<I32> result(0); // explicitly (re-)set result to 0

    /*----- Compile ordering. -----*/
    for (auto &o : order) {
        /*----- Compile order expression for left tuple. -----*/
        SQL_t _val_left = env_left.template compile(o.first);

        std::visit(overloaded {
            [&]<typename T>(Expr<T> val_left) -> void {
                /*----- Compile order expression for right tuple. -----*/
                Expr<T> val_right = env_right.template compile<Expr<T>>(o.first);

                M_insist(val_left.can_be_null() == val_right.can_be_null(),
                         "either both or none of the value to compare must be nullable");
                if (val_left.can_be_null()) {
                    using type = std::conditional_t<std::is_same_v<T, bool>, _I32, Expr<T>>;
                    Var<type> left, right;
                    if constexpr (std::is_same_v<T, bool>) {
                        left  = val_left.template to<int32_t>();
                        right = val_right.template to<int32_t>();
                    } else {
                        left  = val_left;
                        right = val_right;
                    }

                    /*----- Compare both with current order expression and update result. -----*/
                    I32 cmp_null = right.is_null().template to<int32_t>() - left.is_null().template to<int32_t>();
                    _I32 _val_lt = (left < right).template to<int32_t>();
                    _I32 _val_gt = (left > right).template to<int32_t>();
                    _I32 _cmp_val = o.second ? _val_gt - _val_lt : _val_lt - _val_gt;
                    auto [cmp_val, cmp_is_null] = _cmp_val.split();
                    cmp_is_null.discard();
                    I32 cmp = (cmp_null << 1) + cmp_val; // potentially-null value of comparison is overruled by cmp_null
                    result <<= 1; // shift result s.t. first difference will determine order
                    result += cmp; // add current comparison to result
                } else {
                    using type = std::conditional_t<std::is_same_v<T, bool>, I32, PrimitiveExpr<T>>;
                    Var<type> left, right;
                    if constexpr (std::is_same_v<T, bool>) {
                        left  = val_left.insist_not_null().template to<int32_t>();
                        right = val_right.insist_not_null().template to<int32_t>();
                    } else {
                        left  = val_left.insist_not_null();
                        right = val_right.insist_not_null();
                    }

                    /*----- Compare both with current order expression and update result. -----*/
                    I32 val_lt = (left < right).template to<int32_t>();
                    I32 val_gt = (left > right).template to<int32_t>();
                    I32 cmp = o.second ? val_gt - val_lt : val_lt - val_gt;
                    result <<= 1; // shift result s.t. first difference will determine order
                    result += cmp; // add current comparison to result
                }
            },
            [&](NChar val_left) -> void {
                auto &cs = as<const CharacterSequence>(*o.first.get().type());

                /*----- Compile order expression for right tuple. -----*/
                NChar val_right = env_right.template compile<NChar>(o.first);

                Var<Ptr<Char>> _left(val_left.val()), _right(val_right.val());
                NChar left(_left, val_left.can_be_null(), val_left.length(), val_left.guarantees_terminating_nul()),
                      right(_right, val_right.can_be_null(), val_right.length(), val_right.guarantees_terminating_nul());

                M_insist(val_left.can_be_null() == val_right.can_be_null(),
                         "either both or none of the value to compare must be nullable");
                if (val_left.can_be_null()) {
                    /*----- Compare both with current order expression and update result. -----*/
                    I32 cmp_null = right.is_null().to<int32_t>() - left.is_null().to<int32_t>();
                    _I32 _delta = o.second ? strcmp(left, right) : strcmp(right, left);
                    auto [cmp_val, cmp_is_null] = signum(_delta).split();
                    cmp_is_null.discard();
                    I32 cmp = (cmp_null << 1) + cmp_val; // potentially-null value of comparison is overruled by cmp_null
                    result <<= 1; // shift result s.t. first difference will determine order
                    result += cmp; // add current comparison to result
                } else {
                    /*----- Compare both with current order expression and update result. -----*/
                    I32 delta = o.second ? strcmp(left, right).insist_not_null() : strcmp(right, left).insist_not_null();
                    result <<= 1; // shift result s.t. first difference will determine order
                    result += signum(delta); // add current comparison to result
                }
            },
            [](std::monostate) -> void { M_unreachable("invalid expression"); }
        }, _val_left);
    }

    return result;
}
