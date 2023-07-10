#include "backend/WasmDSL.hpp"

#include "backend/WasmMacro.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Schema.hpp>
#ifdef __BMI2__
#include <immintrin.h>
#endif


using namespace m;
using namespace m::wasm;


#if !defined(NDEBUG) && defined(M_ENABLE_SANITY_FIELDS)
bool m::options::insist_no_ternary_logic = false;
#endif

namespace {

__attribute__((constructor(201)))
static void add_wasm_dsl_args()
{
    Catalog &C = Catalog::Get();

#if !defined(NDEBUG) && defined(M_ENABLE_SANITY_FIELDS)
    /*----- Command-line arguments -----*/
    C.arg_parser().add<bool>(
        /* group=       */ "Wasm",
        /* short=       */ nullptr,
        /* long=        */ "--insist-no-ternary-logic",
        /* description= */ "insist that there is no ternary logic, i.e. NULL value computation",
        /* callback=    */ [](bool){ options::insist_no_ternary_logic = true; }
    );
#endif
}

}


/*======================================================================================================================
 * ConstantFolding
 *====================================================================================================================*/

#define CASE(TYPE, BLOCK) \
    case ::wasm::Expression::TYPE##Id: { \
        auto _expr = expr; \
        auto *expr = static_cast<const ::wasm::TYPE*>(_expr); \
        BLOCK \
        break; \
    }

ConstantFolding::boolean_result_t ConstantFolding::EvalBoolean(const ::wasm::Expression *expr)
{
    switch (expr->_id) {
        default:
            break;
        CASE(Const, {
            M_insist(expr->value.type == ::wasm::Type::i32, "invalid boolean expression");
            return expr->value.geti32() ? TRUE : FALSE;
        })
        CASE(Unary, {
            if (expr->op == ::wasm::UnaryOp::EqZInt32) { // negation
                switch (EvalBoolean(expr->value)) {
                    case UNDEF:
                        return UNDEF;
                    case TRUE:
                        return FALSE;
                    case FALSE:
                        return TRUE;
                }
            }
        })
        CASE(Binary, {
            if (expr->op == ::wasm::BinaryOp::AndInt32) { // conjunction
                switch (EvalBoolean(expr->left)) {
                    case UNDEF:
                        switch (EvalBoolean(expr->right)) {
                            case UNDEF:
                            case TRUE:
                                return UNDEF;
                            case FALSE: // dominating element
                                return FALSE;
                        }
                    case TRUE: // neutral element
                        return EvalBoolean(expr->right);
                    case FALSE: // dominating element
                        return FALSE;
                }
            } else if (expr->op == ::wasm::BinaryOp::OrInt32) { // disjunction
                switch (EvalBoolean(expr->left)) {
                    case UNDEF:
                        switch (EvalBoolean(expr->right)) {
                            case UNDEF:
                            case FALSE:
                                return UNDEF;
                            case TRUE: // dominating element
                                return TRUE;
                        }
                    case TRUE: // dominating element
                        return TRUE;
                    case FALSE: // neutral element
                        return EvalBoolean(expr->right);
                }
            }
        })
    }
    return UNDEF;
}

#undef CASE


/*======================================================================================================================
 * MockInterface
 *====================================================================================================================*/

struct MockInterface final : ::wasm::ModuleRunner::ExternalInterface
{
    using base_type = ::wasm::ModuleRunner::ExternalInterface;

    private:
    void *memory_;
    uint32_t size_;
    bool allocated_ = false; ///< indicates whether `memory_` was allocated by this interface

    public:
    MockInterface()
        : memory_(malloc(WasmEngine::WASM_MAX_MEMORY))
        , size_(WasmEngine::WASM_MAX_MEMORY)
        , allocated_(true)
    {
        M_insist(memory_, "memory allocation failed");
    }
    MockInterface(const memory::AddressSpace &memory)
        : memory_(memory.addr())
        , size_(memory.size())
    {
        M_insist(memory_, "given memory is invalid");
    }
    ~MockInterface() { if (allocated_) free(memory_); }

    void importGlobals(::wasm::GlobalValueSet &globals, ::wasm::Module&) override {
        M_insist(globals.empty(), "imports not supported");
    }
    ::wasm::Literals callImport(::wasm::Function *f, ::wasm::Literals &args) override {
        if (auto it = callback_functions.find(f->name); it != callback_functions.end())
            return it->second(args);
        else
            M_unreachable("callback function not found");
    }
    ::wasm::Literals callTable(::wasm::Name,
                               ::wasm::Index,
                               ::wasm::HeapType,
                               ::wasm::Literals&,
                               ::wasm::Type,
                               ::wasm::ModuleRunner&) override { M_unreachable("not supported"); }
    bool growMemory(::wasm::Name, ::wasm::Address, ::wasm::Address) override { M_unreachable("not supported"); }
    bool growTable(::wasm::Name,
                   const ::wasm::Literal&,
                   ::wasm::Index,
                   ::wasm::Index) override { M_unreachable("not implemented"); }
    ::wasm::Index tableSize(::wasm::Name) override { M_unreachable("not implemented"); }
    void trap(const char*) override { M_unreachable("not supported"); }
    void hostLimit(const char*) override { M_unreachable("not supported"); }
    void throwException(const ::wasm::WasmException&) override { M_unreachable("not supported"); }

    private:
    template<typename T = void>
    T _load(::wasm::Address addr) {
        M_insist(addr.addr < size_, "invalid address");
        M_insist(addr.addr % alignof(T) == 0, "misaligned address");
        return *reinterpret_cast<T*>(reinterpret_cast<uint8_t*>(memory_) + addr.addr);
    }
    template<>
    std::array<uint8_t, 16> _load<std::array<uint8_t, 16>>(::wasm::Address _addr) {
        M_insist(_addr.addr + 16 <= size_, "invalid address");
        auto addr = reinterpret_cast<uint8_t*>(memory_) + _addr.addr;
        return std::to_array<uint8_t, 16>(*reinterpret_cast<uint8_t(*)[16]>(addr));
    }
    template<typename T = void>
    void _store(::wasm::Address addr, T value) {
        M_insist(addr.addr < size_, "invalid address");
        M_insist(addr.addr % alignof(T) == 0, "misaligned address");
        *reinterpret_cast<T*>(reinterpret_cast<uint8_t*>(memory_) + addr.addr) = value;
    }
    template<>
    void _store<const std::array<uint8_t, 16>&>(::wasm::Address addr, const std::array<uint8_t, 16> &value) {
        M_insist(addr.addr + 16 <= size_, "invalid address");
        for (uint32_t idx = 0; idx < 16; ++idx)
            *(reinterpret_cast<uint8_t*>(memory_) + addr.addr + idx) = value[idx];
    }

    public:
#define DECLARE_LOAD(BINARYEN_TYPE, C_TYPE) \
    C_TYPE load##BINARYEN_TYPE(::wasm::Address addr, ::wasm::Name) override { \
        return _load<C_TYPE>(addr); \
    }
    DECLARE_LOAD(8s,  int8_t)
    DECLARE_LOAD(8u,  uint8_t)
    DECLARE_LOAD(16s, int16_t)
    DECLARE_LOAD(16u, uint16_t)
    DECLARE_LOAD(32s, int32_t)
    DECLARE_LOAD(32u, uint32_t)
    DECLARE_LOAD(64s, int64_t)
    DECLARE_LOAD(64u, uint64_t)
    DECLARE_LOAD(128, std::array<M_COMMA(uint8_t) 16>)
#undef DECLARE_LOAD

#define DECLARE_STORE(BINARYEN_TYPE, C_TYPE) \
    void store##BINARYEN_TYPE(::wasm::Address addr, C_TYPE value, ::wasm::Name) override { \
        return _store<C_TYPE>(addr, value); \
    }
    DECLARE_STORE(8,   int8_t)
    DECLARE_STORE(16,  int16_t)
    DECLARE_STORE(32,  int32_t)
    DECLARE_STORE(64,  int64_t)
    DECLARE_STORE(128, const std::array<M_COMMA(uint8_t) 16>&)
#undef DECLARE_STORE
};


/*======================================================================================================================
 * LinearAllocator
 *====================================================================================================================*/

/** A simple linear allocator which keeps a global pointer to the next free memory address and advances it for
 * allocation.  Deallocation can only reclaim memory if all chronologically later allocations have been deallocated
 * before.  If possible, deallocate memory in the inverse order of allocation. */
struct LinearAllocator : Allocator
{
    private:
    ///> flag whether pre-allocations were already performed, i.e. `perform_pre_allocations()` was already called
    bool pre_allocations_performed_ = false;
    ///> compile-time size of the currently used memory, used as pointer to next free pre-allocation
    uint32_t pre_alloc_addr_;
    ///> global size of the currently used memory, used as pointer to next free allocation
    Global<U32x1> alloc_addr_;

    public:
    LinearAllocator(uint32_t start_addr)
        : pre_alloc_addr_(start_addr)
    {
        M_insist(start_addr != 0, "memory address 0 is reserved as `nullptr`");
#ifdef M_ENABLE_SANITY_FIELDS
        alloc_addr_.val().discard();  // artificial use of `alloc_addr_` to silence diagnostics if allocator is not used
#endif
    }

    ~LinearAllocator() {
        M_insist(pre_allocations_performed_, "must call `perform_pre_allocations()` before destruction");
    }

    Ptr<void> pre_allocate(uint32_t bytes, uint32_t alignment) override {
        M_insist(not pre_allocations_performed_,
                 "must not request a pre-allocation after `perform_pre_allocations()` was already called");
        M_insist(alignment);
        M_insist(is_pow_2(alignment), "alignment must be a power of 2");
        if (alignment != 1U)
            align_pre_memory(alignment);
        Ptr<void> ptr(U32x1(pre_alloc_addr_).template to<void*>());
        pre_alloc_addr_ += bytes; // advance memory size by bytes
        return ptr;
    }
    Var<Ptr<void>> allocate(U32x1 bytes, uint32_t alignment) override {
        M_insist(alignment);
        M_insist(is_pow_2(alignment), "alignment must be a power of 2");
        if (alignment != 1U)
            align_memory(alignment);
        Var<Ptr<void>> ptr(alloc_addr_.template to<void*>());
        alloc_addr_ += bytes; // advance memory size by bytes
        return ptr;
    }

    void deallocate(Ptr<void> ptr, U32x1 bytes) override {
        Wasm_insist(ptr.clone().template to<uint32_t>() < alloc_addr_, "must not try to free unallocated memory");
        IF (ptr.template to<uint32_t>() + bytes.clone() == alloc_addr_) { // last allocation can be freed
            alloc_addr_ -= bytes; // free by decreasing memory size
        };
    }

    virtual void perform_pre_allocations() override {
        M_insist(not pre_allocations_performed_,
                 "must not call `perform_pre_allocations()` multiple times");
        alloc_addr_.init(pre_alloc_addr_);
        pre_allocations_performed_ = true;
    }

    private:
    /** Aligns the memory for pre-allocations with alignment requirement `align`. */
    void align_pre_memory(uint32_t alignment) {
        M_insist(is_pow_2(alignment));
        pre_alloc_addr_ = (pre_alloc_addr_ + (alignment - 1U)) bitand ~(alignment - 1U);
    }
    /** Aligns the memory for allocations with alignment requirement `align`. */
    void align_memory(uint32_t alignment) {
        M_insist(is_pow_2(alignment));
        alloc_addr_ = (alloc_addr_ + (alignment - 1U)) bitand ~(alignment - 1U);
    }
};


/*======================================================================================================================
 * Module
 *====================================================================================================================*/

thread_local std::unique_ptr<Module> Module::the_module_;

Module::Module()
    : id_(NEXT_MODULE_ID_.fetch_add(1U, std::memory_order_relaxed))
    , module_()
    , builder_(module_)
{
    /*----- Import functions from the environment. -----*/
    emit_function_import<void(uint64_t)>("insist"); // to implement insist at Wasm site
    emit_function_import<void(uint64_t, uint64_t)>("throw"); // to throw exceptions from Wasm site

    /*----- Create module memory. -----*/
    ::wasm::Name memory_name = std::to_string(id_);
    {
        auto mem = std::make_unique<::wasm::Memory>();
        mem->name = memory_name;
        module_.addMemory(std::move(mem));
    }

    /*----- Export the Wasm linear memory, s.t. it can be accessed from the environment (JavaScript). -----*/
    memory_ = module_.getMemory(memory_name);
    memory_->initial = 1; // otherwise the Binaryen interpreter traps
    memory_->max = int32_t(WasmEngine::WASM_MAX_MEMORY / WasmEngine::WASM_PAGE_SIZE);
    module_.exports.emplace_back(
        builder_.makeExport("memory", memory_->name, ::wasm::ExternalKind::Memory)
    );

    /*----- Set features. -----*/
    module_.features.setBulkMemory(true);
    module_.features.setSIMD(true);
}

::wasm::ModuleRunner::ExternalInterface * Module::get_mock_interface()
{
    if (not interface_) [[unlikely]] {
        if (WasmEngine::Has_Wasm_Context(id_))
            interface_ = std::make_unique<MockInterface>(WasmEngine::Get_Wasm_Context_By_ID(id_).vm);
        else
            interface_ = std::make_unique<MockInterface>();
    }
    return interface_.get();
}

bool Module::Validate(bool verbose, bool global)
{
    ::wasm::WasmValidator::Flags flags(0);
    if (not verbose) flags |= ::wasm::WasmValidator::Quiet;
    if (global) flags |= ::wasm::WasmValidator::Globally;
    return ::wasm::WasmValidator{}.validate(Get().module_, flags);
}

void Module::Optimize(int optimization_level)
{
    ::wasm::PassOptions options;
    options.optimizeLevel = optimization_level;
    options.shrinkLevel = 0; // shrinking not required
    ::wasm::PassRunner runner(&Get().module_, options);
    runner.addDefaultOptimizationPasses();
    runner.run();
}

std::pair<uint8_t*, std::size_t> Module::binary()
{
    ::wasm::BufferWithRandomAccess buffer;
    ::wasm::WasmBinaryWriter writer(&module_, buffer);
    writer.setNamesSection(false);
    writer.write();
    void *binary = malloc(buffer.size());
    std::copy_n(buffer.begin(), buffer.size(), static_cast<char*>(binary));
    return std::make_pair(reinterpret_cast<uint8_t*>(binary), buffer.size());
}

template<std::size_t L>
void Module::emit_insist(PrimitiveExpr<bool, L> cond, const char *filename, unsigned line, const char *msg)
{
    emit_insist(cond.all_true(), filename, line, msg);
}

// explicit instantiations to prevent linker errors
template void Module::emit_insist(PrimitiveExpr<bool, 2>,  const char*, unsigned, const char*);
template void Module::emit_insist(PrimitiveExpr<bool, 4>,  const char*, unsigned, const char*);
template void Module::emit_insist(PrimitiveExpr<bool, 8>,  const char*, unsigned, const char*);
template void Module::emit_insist(PrimitiveExpr<bool, 16>, const char*, unsigned, const char*);
template void Module::emit_insist(PrimitiveExpr<bool, 32>, const char*, unsigned, const char*);

template<>
void Module::emit_insist(PrimitiveExpr<bool, 1> cond, const char *filename, unsigned line, const char *msg)
{
    static thread_local struct {} _; // unique caller handle
    struct data_t : GarbageCollectedData
    {
        public:
        std::optional<FunctionProxy<void(uint64_t)>> delegate_insist;

        data_t(GarbageCollectedData &&d) : GarbageCollectedData(std::move(d)) { }
    };
    auto &d = add_garbage_collected_data<data_t>(&_); // garbage collect the `data_t` instance

    if (not d.delegate_insist) {
        /*----- Create function to delegate to host (used for easier debugging since one can break in here). -----*/
        FUNCTION(delegate_insist, void(uint64_t))
        {
            emit_call<void>("insist", PARAMETER(0).val());
        }
        d.delegate_insist = std::move(delegate_insist);
    }

    uint64_t idx = messages_.size();
    messages_.emplace_back(filename, line, msg);

    /*----- Check condition and possibly delegate to host. --*/
    M_insist(bool(d.delegate_insist));
    IF (not cond) {
        (*d.delegate_insist)(idx);
    };
}

void Module::emit_throw(exception::exception_t type, const char *filename, unsigned line, const char *msg)
{
    uint64_t idx = messages_.size();
    messages_.emplace_back(filename, line, msg);
    std::vector<::wasm::Expression*> args = {
        builder_.makeConst(::wasm::Literal(type)), // type id
        builder_.makeConst(::wasm::Literal(idx))   // message index
    };
    active_block_->list.push_back(builder_.makeCall("throw", args, wasm_type<void, 1>()));
}

/** Emit an unconditional continue, continuing \p level levels above. */
void Module::emit_continue(std::size_t level)
{
    M_insist(level > 0);
    M_insist(branch_target_stack_.size() >= level);
    auto &branch_targets = branch_target_stack_[branch_target_stack_.size() - level];
    if (branch_targets.condition) {
        PrimitiveExpr<bool, 1> condition(
            ::wasm::ExpressionManipulator::copy(branch_targets.condition, Module::Get().module_)
        );
        /* Continue if condition is satisfied, break otherwise. */
        IF (condition) {
            active_block_->list.push_back(builder_.makeBreak(branch_targets.continu));
        } ELSE {
            BREAK();
        };
    } else {
        /* Continue unconditionally. */
        active_block_->list.push_back(builder_.makeBreak(branch_targets.continu));
    }
}

/** Emit an *conditional* continue, continuing \p level levels above if \p cond evaluates to `true` *and* the original
 * continue condition evaluates to `true`. */
void Module::emit_continue(PrimitiveExpr<bool, 1> cond, std::size_t level)
{
    M_insist(level > 0);
    M_insist(branch_target_stack_.size() >= level);
    IF (cond) {
        emit_continue(level);
    };
}

Allocator & Module::Allocator()
{
    if (not Get().allocator_) [[unlikely]] {
        if (WasmEngine::Has_Wasm_Context(ID()))
            Get().allocator_ = std::make_unique<LinearAllocator>(WasmEngine::Get_Wasm_Context_By_ID(ID()).heap);
        else
            Get().allocator_ = std::make_unique<LinearAllocator>(1); // reserve address 0 for `nullptr`
    }
    return *Get().allocator_;
}


/*======================================================================================================================
 * Callback functions
 *====================================================================================================================*/

::wasm::Literals m::wasm::insist_interpreter(::wasm::Literals &args)
{
    M_insist(args.size() == 1);
    auto idx = args[0].getUnsigned();
    auto [filename, line, msg] = Module::Get().get_message(idx);

    std::cout.flush();
    std::cerr << filename << ':' << line << ": Wasm_insist failed.";
    if (msg)
        std::cerr << "  " << msg << '.';
    std::cerr << std::endl;

    abort();
}

::wasm::Literals m::wasm::throw_interpreter(::wasm::Literals &args)
{
    M_insist(args.size() == 2);
    auto type = static_cast<exception::exception_t>(args[0].getUnsigned());
    auto idx = args[1].getUnsigned();
    auto [filename, line, msg] = Module::Get().get_message(idx);

    std::ostringstream oss;
    oss << filename << ':' << line << ": Exception `" << exception::names_[type] << "` thrown.";
    if (*msg)
        oss << "  " << msg << '.';
    oss << std::endl;

    throw exception(type, oss.str());
}


/*======================================================================================================================
 * Control flow
 *====================================================================================================================*/

/*----- If -----------------------------------------------------------------------------------------------------------*/

If::~If()
{
    M_insist(bool(Then), "If must have a Then");

    Block then_block(name_ + ".then", false);
    Block else_block(name_ + ".else", false);

    BLOCK_OPEN(then_block) {
        Then();
    }

    if (Else) {
        BLOCK_OPEN(else_block) {
            Else();
        }
    }

    auto cond = cond_.expr();
    switch (ConstantFolding::EvalBoolean(cond)) {
        case ConstantFolding::UNDEF:
            /*----- If -----*/
            Module::Block().list.push_back(
                Module::Builder().makeIf(
                    cond,
                    &then_block.get(),
                    Else ? &else_block.get() : nullptr
                )
            );
            break;
        case ConstantFolding::TRUE:
            /*----- Then-block -----*/
            Module::Block().list.push_back(&then_block.get());
            break;
        case ConstantFolding::FALSE:
            /*----- Else-block -----*/
            if (Else)
                Module::Block().list.push_back(&else_block.get());
            break;
    }
}

/*----- Do-While -----------------------------------------------------------------------------------------------------*/

DoWhile::~DoWhile() {
    BLOCK_OPEN(body()) {
        CONTINUE(); // emit conditional branch back to loop header at the end of the loop's body
    }
}

/*----- While --------------------------------------------------------------------------------------------------------*/

While::~While() {
    IF (cond_) {
        do_while_.reset(); // emit do-while code within IF
    };
}
