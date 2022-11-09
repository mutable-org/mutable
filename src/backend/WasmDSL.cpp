#include "backend/WasmDSL.hpp"

#include "backend/WasmMacro.hpp"
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Schema.hpp>
#include <immintrin.h>


using namespace m;
using namespace m::wasm;


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
        : memory_(malloc(WasmPlatform::WASM_MAX_MEMORY))
        , size_(WasmPlatform::WASM_MAX_MEMORY)
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
    bool growMemory(::wasm::Address, ::wasm::Address) override { M_unreachable("not supported"); }
    bool growTable(::wasm::Name,
                   const ::wasm::Literal&,
                   ::wasm::Index,
                   ::wasm::Index) override { M_unreachable("not implemented"); }
    ::wasm::Index tableSize(::wasm::Name) override { M_unreachable("not implemented"); }
    void trap(const char*) override { M_unreachable("not supported"); }
    void hostLimit(const char*) override { M_unreachable("not supported"); }
    void throwException(const ::wasm::WasmException&) override { M_unreachable("not supported"); }

    private:
    template<typename T>
    T load(::wasm::Address addr) {
        M_insist(addr.addr < size_, "invalid address");
        return *reinterpret_cast<T*>(reinterpret_cast<uint8_t*>(memory_) + addr.addr);
    }
    template<typename T>
    void store(::wasm::Address addr, T value) {
        M_insist(addr.addr < size_, "invalid address");
        *reinterpret_cast<T*>(reinterpret_cast<uint8_t*>(memory_) + addr.addr) = value;
    }

    public:
#define DECLARE_LOAD(NAME) \
    decltype(std::declval<base_type>().NAME(std::declval<::wasm::Address>())) NAME(::wasm::Address addr) override { \
        return load<decltype(std::declval<base_type>().NAME(std::declval<::wasm::Address>()))>(addr); \
    }
DECLARE_LOAD(load8s)
DECLARE_LOAD(load8u)
DECLARE_LOAD(load16s)
DECLARE_LOAD(load16u)
DECLARE_LOAD(load32s)
DECLARE_LOAD(load32u)
DECLARE_LOAD(load64s)
DECLARE_LOAD(load64u)
#undef DECLARE_LOAD

#define DECLARE_STORE(SIZE) \
    void store ## SIZE(::wasm::Address addr, int ## SIZE ## _t value) override { \
        return store<int ## SIZE ## _t>(addr, value); \
    }
DECLARE_STORE(8)
DECLARE_STORE(16)
DECLARE_STORE(32)
DECLARE_STORE(64)
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
    Global<U32> alloc_addr_;

    public:
    LinearAllocator(uint32_t start_addr)
        : pre_alloc_addr_(start_addr)
    {
        M_insist(start_addr != 0, "memory address 0 is reserved as `nullptr`");
    }

    ~LinearAllocator() {
        M_insist(pre_allocations_performed_, "must call `perform_pre_allocations()` before destruction");
    }

    Ptr<void> pre_allocate(uint32_t bytes, uint32_t alignment) override {
        M_insist(not pre_allocations_performed_,
                 "must not request a pre-allocation after `perform_pre_allocations()` was already called");
        M_insist(alignment);
        if (alignment != 1U)
            align_pre_memory(alignment);
        Ptr<void> ptr(U32(pre_alloc_addr_).template to<void*>());
        pre_alloc_addr_ += bytes; // advance memory size by bytes
        return ptr;
    }
    Var<Ptr<void>> allocate(U32 bytes, uint32_t alignment) override {
        M_insist(alignment);
        if (alignment != 1U)
            align_memory(alignment);
        Var<Ptr<void>> ptr(alloc_addr_.template to<void*>());
        alloc_addr_ += bytes; // advance memory size by bytes
        return ptr;
    }

    void deallocate(Ptr<void> ptr, U32 bytes) override {
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

Module::Module()
    : id_(NEXT_MODULE_ID_.fetch_add(1U, std::memory_order_relaxed))
    , module_()
    , builder_(module_)
{
    /*----- Import functions from the environment. -----*/
    emit_function_import<void(uint64_t)>("insist"); // to implement insist at Wasm site
    emit_function_import<void(uint64_t, uint64_t)>("throw"); // to throw exceptions from Wasm site

    /*----- Export the Wasm linear memory, s.t. it can be accessed from the environment (JavaScript). -----*/
    auto &mem = module_.memory;
    mem.exists = true;
    mem.initial = 1; // otherwise the Binaryen interpreter traps
    mem.max = int32_t(WasmPlatform::WASM_MAX_MEMORY / WasmPlatform::WASM_PAGE_SIZE);
    module_.exports.emplace_back(
        builder_.makeExport("memory", mem.name, ::wasm::ExternalKind::Memory)
    );

    /*----- Set features. -----*/
    module_.features.setBulkMemory(true);
}

::wasm::ModuleRunner::ExternalInterface * Module::get_mock_interface()
{
    if (not interface_) [[unlikely]] {
        if (WasmPlatform::Has_Wasm_Context(id_))
            interface_ = std::make_unique<MockInterface>(WasmPlatform::Get_Wasm_Context_By_ID(id_).vm);
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

void Module::emit_insist(PrimitiveExpr<bool> condition, const char *filename, unsigned line, const char *msg)
{
    uint64_t idx = messages_.size();
    messages_.emplace_back(filename, line, msg);
    auto call = builder_.makeCall("insist", { builder_.makeConst(::wasm::Literal(idx)) }, wasm_type<void>());
    active_block_->list.push_back(
        builder_.makeIf(
            /* condition= */ (not condition).expr(),
            /* ifTrue=    */ call
        )
    );
}

void Module::emit_insist(Expr<bool> condition, const char *filename, unsigned line, const char *msg)
{
    // TODO propagate info whether \p condition is NULL to host's `insist()`
    emit_insist(condition.is_true_and_not_null(), filename, line, msg);
}

void Module::emit_throw(exception::exception_t type, const char *filename, unsigned line, const char *msg)
{
    uint64_t idx = messages_.size();
    messages_.emplace_back(filename, line, msg);
    std::vector<::wasm::Expression*> args = {
        builder_.makeConst(::wasm::Literal(type)), // type id
        builder_.makeConst(::wasm::Literal(idx))   // message index
    };
    active_block_->list.push_back(builder_.makeCall("throw", args, wasm_type<void>()));
}

/** Emit an unconditional continue, continuing \p level levels above. */
void Module::emit_continue(std::size_t level)
{
    M_insist(level > 0);
    M_insist(branch_target_stack_.size() >= level);
    auto &branch_targets = branch_target_stack_[branch_target_stack_.size() - level];
    if (branch_targets.condition) {
        PrimitiveExpr<bool> condition(
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
void Module::emit_continue(PrimitiveExpr<bool> cond, std::size_t level)
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
        if (WasmPlatform::Has_Wasm_Context(ID()))
            Get().allocator_ = std::make_unique<LinearAllocator>(WasmPlatform::Get_Wasm_Context_By_ID(ID()).heap);
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
    if (*msg)
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

    /*----- If -----*/
    Module::Block().list.push_back(
        Module::Builder().makeIf(
            cond_.expr(),
            &then_block.get(),
            &else_block.get()
        )
    );
}

/*----- Do-While -----------------------------------------------------------------------------------------------------*/

DoWhile::~DoWhile() {
    BLOCK_OPEN(body()) {
        CONTINUE(); // emit conditional branch back to loop header at the end of the loop's body
    }
}

/*----- While --------------------------------------------------------------------------------------------------------*/

While::~While() {
    IF (condition_) {
        do_while_.reset(); // emit do-while code within IF
    };
}
