#pragma once

#include "backend/Backend.hpp"
#include "catalog/Schema.hpp"
#include "IR/Operator.hpp"
#include "IR/OperatorVisitor.hpp"
#include "parse/ASTVisitor.hpp"
#include "util/macro.hpp"
#include <unordered_map>


namespace wasm {

struct Module;
struct Expression;

}

namespace db {

struct WASMCodeGen;

struct WASMModule
{
    friend struct WASMCodeGen;

    private:
    wasm::Module *ref_;

    public:
    WASMModule();
    ~WASMModule();
    WASMModule(const WASMModule&) = delete;
    WASMModule(WASMModule&&) = default;

    /** Returns the binary representation of this module in a freshly allocated memory.  The caller must dispose of this
     * memory. */
    std::pair<uint8_t*, std::size_t> binary() const;

    /** Print module in human-readable text format. */
    friend std::ostream & operator<<(std::ostream &out, const WASMModule &module);

    void dump(std::ostream &out) const;
    void dump() const;
};

/** Compiles a physical plan to WebAssembly. */
struct WASMCodeGen : ConstOperatorVisitor, ConstASTVisitor {
    private:
    wasm::Module *module_;
    wasm::Expression *expr_;

    WASMCodeGen(const WASMModule &module) : module_(module.ref_) { } // private c'tor

    public:
    static WASMModule compile(const Operator &op);

    private:
    using ConstOperatorVisitor::operator();
#define DECLARE(CLASS) void operator()(ConstOperatorVisitor::Const<CLASS> &op) override
    DECLARE(ScanOperator);
    DECLARE(CallbackOperator);
    DECLARE(FilterOperator);
    DECLARE(JoinOperator);
    DECLARE(ProjectionOperator);
    DECLARE(LimitOperator);
    DECLARE(GroupingOperator);
    DECLARE(SortingOperator);
#undef DECLARE

    using ConstASTVisitor::operator();
    using ConstASTVisitor::Const;

    /* Expressions */
    void operator()(Const<ErrorExpr>&) override { unreachable(""); }
    void operator()(Const<Designator> &e) override;
    void operator()(Const<Constant> &e) override;
    void operator()(Const<FnApplicationExpr> &e) override;
    void operator()(Const<UnaryExpr> &e) override;
    void operator()(Const<BinaryExpr> &e) override;

    /* Clauses */
    void operator()(Const<ErrorClause>&) override { unreachable(""); }
    void operator()(Const<SelectClause>&) override { unreachable(""); }
    void operator()(Const<FromClause>&) override { unreachable(""); }
    void operator()(Const<WhereClause>&) override { unreachable(""); }
    void operator()(Const<GroupByClause>&) override { unreachable(""); }
    void operator()(Const<HavingClause>&) override { unreachable(""); }
    void operator()(Const<OrderByClause>&) override { unreachable(""); }
    void operator()(Const<LimitClause>&) override { unreachable(""); }

    /* Statements */
    void operator()(Const<ErrorStmt>&) override { unreachable(""); }
    void operator()(Const<EmptyStmt>&) override { unreachable(""); }
    void operator()(Const<CreateDatabaseStmt>&) override { unreachable(""); }
    void operator()(Const<UseDatabaseStmt>&) override { unreachable(""); }
    void operator()(Const<CreateTableStmt>&) override { unreachable(""); }
    void operator()(Const<SelectStmt>&) override { unreachable(""); }
    void operator()(Const<InsertStmt>&) override { unreachable(""); }
    void operator()(Const<UpdateStmt>&) override { unreachable(""); }
    void operator()(Const<DeleteStmt>&) override { unreachable(""); }
    void operator()(Const<DSVImportStmt>&) override { unreachable(""); }
};

/** A platform to execute WASM modules. */
struct WasmPlatform
{
    static constexpr std::size_t WASM_PAGE_SIZE = 1UL << 16; // 64 KiB pages
    static constexpr std::size_t WASM_MAX_MEMORY = (1UL << 31) - (1UL << 16); // 2^32 - 2^16 bytes, approx 2 GiB

    struct WasmContext
    {
        uint32_t id;
        rewire::AddressSpace vm;

        WasmContext(uint32_t id, std::size_t size) : id(id), vm(size) { }
    };

    private:
    static uint32_t wasm_counter_;
    static std::unordered_map<uint32_t, WasmContext> contexts_;

    protected:
    static WasmContext & Create_Wasm_Context(std::size_t size) {
        auto res = contexts_.emplace(wasm_counter_, WasmContext(wasm_counter_, size));
        insist(res.second, "WasmContext with that ID already exists");
        ++wasm_counter_;
        return res.first->second;
    }

    static void Dispose_Wasm_Context(uint32_t id) {
        auto res = contexts_.erase(id);
        (void) res;
        insist(res == 1, "There is no context with the given ID to erase");
    }

    public:
    static WasmContext & Get_Wasm_Context_By_ID(uint32_t id) {
        auto it = contexts_.find(id);
        insist(it != contexts_.end(), "There is no context with the given ID");
        return it->second;
    }

    virtual ~WasmPlatform() { }
    virtual void execute(const WASMModule &module) = 0;
};

/** A backend to execute WASM modules on a specific platform. */
struct WasmBackend : Backend
{
    private:
    std::unique_ptr<WasmPlatform> platform_;

    public:
    WasmBackend(std::unique_ptr<WasmPlatform> platform) : platform_(std::move(platform)) { }

    void execute(const Operator &plan) const override {
        auto module = WASMCodeGen::compile(plan);
        platform_->execute(module);
    }
};

}
