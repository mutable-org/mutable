/* vim: set filetype=cpp: */
#include "backend/WasmOperator.hpp"
#include "backend/WasmUtil.hpp"

#ifndef BACKEND_NAME
#error "must define BACKEND_NAME before including this file"
#endif


using namespace m;
using namespace m::memory;
using namespace m::storage;
using namespace m::wasm;


/*======================================================================================================================
 * DummyStore
 *====================================================================================================================*/

/** This class implements a dummy store. It only allocates memory and provides the `Store` interface but should later
 * be removed once `Table`s allocate their own memory. */
struct DummyStore : m::Store
{
    static constexpr std::size_t ALLOCATION_SIZE = 1UL << 30; ///< 1 GiB

    private:
    LinearAllocator allocator_; ///< the memory allocator
    Memory data_; ///< the underlying memory containing the data
    std::size_t num_rows_ = 0; ///< the number of rows in use

    public:
    DummyStore(const m::Table &table)
        : m::Store(table)
    {
        data_ = allocator_.allocate(ALLOCATION_SIZE);
    }
    ~DummyStore() { }

    const Memory & memory() const override { return data_; }
    std::size_t num_rows() const override { return num_rows_; }

    void append() override { ++num_rows_; }
    void drop() override { --num_rows_; }

    void dump(std::ostream &out) const override { out << "DummyStore with data at:" << std::endl; data_.dump(); }
    using m::Store::dump;
};


/*======================================================================================================================
 * Test cases
 *====================================================================================================================*/

TEST_CASE("Wasm/" BACKEND_NAME "/Scan", "[core][wasm]")
{
    Module::Init(); // fresh module
    auto &wasm_context = m::WasmEngine::Create_Wasm_Context_For_ID(Module::ID()); // create fresh wasm context

    /* Create table. */
    m::Table table("dummy_table");
    table.push_back("f", m::Type::Get_Float(m::Type::TY_Vector));
    table.push_back("i64", m::Type::Get_Integer(m::Type::TY_Vector, 8));
    table.push_back("b1", m::Type::Get_Boolean(m::Type::TY_Vector));
    table.push_back("b2", m::Type::Get_Boolean(m::Type::TY_Vector));
    table.push_back("c", m::Type::Get_Char(m::Type::TY_Vector, 10));
    table.push_back("i8", m::Type::Get_Integer(m::Type::TY_Vector, 1));
    table.store(std::make_unique<DummyStore>(table));

    constexpr std::size_t max_string_length = 10;
    struct
    {
        std::vector<float> f;
        std::vector<int64_t> i64;
        std::vector<bool> b1;
        std::vector<bool> b2;
        std::vector<const char*> c;
        std::vector<int8_t> i8;
        std::vector<uint64_t> is_null;
    } data;

    /* Lambda to invoke test *inside* section to get entire test case name. */
    auto invoke = [&](){
        /* Create match for the scan operator for the table. */
        m::ScanOperator scan(table.store(), table.name);
        m::Match<Scan<false>> M(&scan, {});

        /* Map table into wasm memory and add the mapped address and the number of rows as global variables. */
        auto off = wasm_context.map_table(table); // without faulting guard pages
        std::ostringstream oss;
        oss << table.name << "_mem";
        Module::Get().emit_global<void*>(oss.str(), false, off);
        oss.str("");
        oss << table.name << "_num_rows";
        Module::Get().emit_global<uint32_t>(oss.str(), false, table.store().num_rows());

        CodeGenContext::Init(); // create fresh codegen context
        FUNCTION(scan_code, void(void)) {
            auto S = CodeGenContext::Get().scoped_environment(); // create scoped environment for function

            /* Create return lambda which checks all the scanned data and throws an exception is a mismatch is found. */
            auto schema = table.schema();
            Var<U8x1> iteration(0U);
            auto Return = [&]() {
                auto &env = CodeGenContext::Get().env();
                for (std::size_t idx = 0; idx < table.store().num_rows(); ++idx) {
                    auto msg = std::string("mismatch at tuple id ").append(std::to_string(idx)).append(", attribute ");
                    IF (iteration == idx) {
                        if (data.is_null.at(idx) bitand 0b000001)
                            WASM_CHECK(env.get<_Floatx1>(schema[0].id).is_null(), (msg + "f").c_str());
                        else
                            WASM_CHECK(data.f.at(idx) == env.get<_Floatx1>(schema[0].id), (msg + "f").c_str());
                        if (data.is_null.at(idx) bitand 0b000010)
                            WASM_CHECK(env.get<_I64x1>(schema[1].id).is_null(), (msg + "i64").c_str());
                        else
                            WASM_CHECK(data.i64.at(idx) == env.get<_I64x1>(schema[1].id), (msg + "i64").c_str());
                        if (data.is_null.at(idx) bitand 0b000100)
                            WASM_CHECK(env.get<_Boolx1>(schema[2].id).is_null(), (msg + "b1").c_str());
                        else
                            WASM_CHECK(bool(data.b1.at(idx)) == env.get<_Boolx1>(schema[2].id), (msg + "b1").c_str());
                        if (data.is_null.at(idx) bitand 0b001000)
                            WASM_CHECK(env.get<_Boolx1>(schema[3].id).is_null(), (msg + "b2").c_str());
                        else
                            WASM_CHECK(bool(data.b2.at(idx)) == env.get<_Boolx1>(schema[3].id), (msg + "b2").c_str());
                        if (data.is_null.at(idx) bitand 0b010000)
                            WASM_CHECK(env.get<NChar>(schema[4].id).is_null(), (msg + "c").c_str());
                        else
                            check_string(data.c.at(idx), env.get<NChar>(schema[4].id),
                                         std::min(strlen(data.c.at(idx)) + 1, max_string_length), msg + "c");
                        if (data.is_null.at(idx) bitand 0b100000)
                            WASM_CHECK(env.get<_I8x1>(schema[5].id).is_null(), (msg + "i8").c_str());
                        else
                            WASM_CHECK(data.i8.at(idx) == env.get<_I8x1>(schema[5].id), (msg + "i8").c_str());
                    };
                }
                iteration += uint8_t(1);
            };

            /* Execute the scan operator. */
            Scan<false>::execute(M, setup_t::Make_Without_Parent(), Return, teardown_t::Make_Without_Parent());

        }
        CodeGenContext::Dispose(); // dispose codegen context

        INVOKE(scan_code);
    };

    SECTION("unaligned row layout")
    {
        /* Create C struct be able to store the data directly into memory. */
        struct Row
        {
            float f;
            /* 32 bits padding */
            int64_t i64;
            /* no padding */
            bool b1:1;
            /* no padding */
            bool b2:1;
            /* 6 bits padding */
            char c[max_string_length];
            /* no padding */
            int8_t i8;
            /* no padding */
            uint8_t is_null:6; // 6 attributes
        };

        /* Create data layout. */
        DataLayout layout;
        auto &row = layout.add_inode(1, sizeof(Row) * 8);
        uint64_t offset_in_bits = 0;
        for (std::size_t idx = 0; idx != table.num_attrs(); ++idx) {
            auto type = table[idx].type;
            if (auto rem = offset_in_bits % type->alignment())
                offset_in_bits += type->alignment() - rem; // padding
            row.add_leaf(type, idx, offset_in_bits, 0);
            offset_in_bits += table[idx].type->size();
        }
        row.add_leaf(m::Type::Get_Bitmap(m::Type::TY_Vector, table.num_attrs()), table.num_attrs(), offset_in_bits, 0);
        table.layout(std::move(layout));

        /* Create data. */
        data.f       = { 3.14f, -2.71f, -0.42f };
        data.i64     = { -123456789, 4321, 999 };
        data.b1      = { true, false, false };
        data.b2      = { false, true, false };
        data.c       = { "test", "success", "failed" };
        data.i8      = { 42, 17, -29 };
        data.is_null = { 0b110000, 0b000101, 0b001010 };

        /* Store data directly into memory of table through C struct. */
        Row *row_ptr = table.store().memory().as<Row*>();
        for (std::size_t idx = 0; idx < data.f.size(); ++idx, ++row_ptr) {
            row_ptr->f = data.f.at(idx);
            row_ptr->i64 = data.i64.at(idx);
            row_ptr->b1 = data.b1.at(idx);
            row_ptr->b2 = data.b2.at(idx);
            strncpy(row_ptr->c, data.c.at(idx), max_string_length);
            row_ptr->i8 = data.i8.at(idx);
            row_ptr->is_null = data.is_null.at(idx);
            table.store().append();
        }

        REQUIRE_NOTHROW(invoke());
    }

    SECTION("minimal-padding row layout")
    {
        /* Create C struct be able to store the data directly into memory. */
        struct Row
        {
            int64_t i64; // alignment 64 bits
            float f; // alignment 32 bits
            char c[max_string_length]; // alignment 8 bits
            int8_t i8; // alignment 8 bits
            bool b1:1; // alignment 8 bits
            bool b2:1; // alignment 1 bit
            uint8_t is_null:6; // alignment 1 bit
        };

        /* Create data layout (using our standard row layout factory). */
        RowLayoutFactory factory;
        table.layout(factory); // FIXME: does not only test a single unit, more like an integration test

        /* Create data. */
        data.f       = { 3.14f, -2.71f, -0.42f };
        data.i64     = { -123456789, 4321, 999 };
        data.b1      = { true, false, false };
        data.b2      = { false, true, false };
        data.c       = { "test", "success", "failed" };
        data.i8      = { 42, 17, -29 };
        data.is_null = { 0b110000, 0b000101, 0b001010 };

        /* Store data directly into memory of table through C struct. */
        Row *row_ptr = table.store().memory().as<Row*>();
        for (std::size_t idx = 0; idx < data.f.size(); ++idx, ++row_ptr) {
            row_ptr->f = data.f.at(idx);
            row_ptr->i64 = data.i64.at(idx);
            row_ptr->b1 = data.b1.at(idx);
            row_ptr->b2 = data.b2.at(idx);
            strncpy(row_ptr->c, data.c.at(idx), max_string_length);
            row_ptr->i8 = data.i8.at(idx);
            row_ptr->is_null = data.is_null.at(idx);
            table.store().append();
        }

        REQUIRE_NOTHROW(invoke());
    }

    SECTION("unaligned PAX layout")
    {
        /* Create C struct be able to store the data directly into memory. */
        constexpr std::size_t num_tuples_per_block = 2;
        static_assert(num_tuples_per_block <= 8,
                      "more than 12 tuples per block would exceed bool and NULL bitmap column's data types");
        struct Block
        {
            float f[num_tuples_per_block];
            /* at most 32 bits padding (depending on number of tuples per block) */
            int64_t i64[num_tuples_per_block];
            /* no padding */
            uint16_t b1:(1 * num_tuples_per_block);
            /* no padding */
            uint16_t b2:(1 * num_tuples_per_block);
            /* at most 7 bits padding (depending on number of tuples per block) */
            char c[num_tuples_per_block][max_string_length];
            /* no padding */
            int8_t i8[num_tuples_per_block];
            uint64_t :0; // padding to next multiple of 8 bytes to prevent optimizations if `is_null` needs less space
            uint64_t is_null:(6 * num_tuples_per_block);
        };

        /* Create data layout. */
        DataLayout layout;
        auto &pax_block = layout.add_inode(num_tuples_per_block, sizeof(Block) * 8);
        uint64_t offset_in_bits = 0;
        for (std::size_t idx = 0; idx != table.num_attrs(); ++idx) {
            auto type = table[idx].type;
            if (auto rem = offset_in_bits % type->alignment())
                offset_in_bits += type->alignment() - rem; // padding
            pax_block.add_leaf(type, idx, offset_in_bits, type->size());
            offset_in_bits += table[idx].type->size() * num_tuples_per_block;
        }
        if (auto rem = offset_in_bits % 64)
            offset_in_bits += 64 - rem; // padding to next multiple of 8 bytes
        pax_block.add_leaf(m::Type::Get_Bitmap(m::Type::TY_Vector, table.num_attrs()), table.num_attrs(), offset_in_bits, table.num_attrs());
        table.layout(std::move(layout));

        /* Create data. */
        data.f       = { 3.14f, -2.71f, -0.42f, 1.23f, 29.09f };
        data.i64     = { -123456789, 4321, 999, -10, 314 };
        data.b1      = { true, false, false, true, true };
        data.b2      = { false, true, false, false, true };
        data.c       = { "test", "success", "failed", "mutable", "UdS" };
        data.i8      = { 42, 17, -29, -1, 7 };
        data.is_null = { 0b110000, 0b000101, 0b001010, 0b001100, 0b100110 };

        /* Store data directly into memory of table through C struct. */
        Block* const mem_ptr = table.store().memory().as<Block*>();
        for (std::size_t idx = 0; idx < data.f.size(); ++idx) {
            auto block_ptr = mem_ptr + (idx / num_tuples_per_block);
            auto idx_in_block = idx % num_tuples_per_block;
            block_ptr->f[idx_in_block] = data.f.at(idx);
            block_ptr->i64[idx_in_block] = data.i64.at(idx);
            block_ptr->b1 ^= (-uint16_t(data.b1.at(idx)) ^ block_ptr->b1) & (uint16_t(1) << idx_in_block);
            block_ptr->b2 ^= (-uint16_t(data.b2.at(idx)) ^ block_ptr->b2) & (uint16_t(1) << idx_in_block);
            strncpy(block_ptr->c[idx_in_block], data.c.at(idx), max_string_length);
            block_ptr->i8[idx_in_block] = data.i8.at(idx);
            block_ptr->is_null = (block_ptr->is_null & ~(uint64_t(0b111111) << (idx_in_block * 6))) |
                                 (data.is_null.at(idx) << (idx_in_block * 6));
            table.store().append();
        }

        REQUIRE_NOTHROW(invoke());
    }

    SECTION("minimal-padding PAX layout")
    {
        /* Create C struct be able to store the data directly into memory. */
        constexpr std::size_t num_tuples_per_block = 2;
        static_assert(num_tuples_per_block <= 8,
                      "more than 8 tuples per block would exceed bool column's data types");
        static_assert(
            num_tuples_per_block % 2 == 0 and               // to let `f` end at a 64-bit-aligned address
            (
                num_tuples_per_block * max_string_length +  // size of `c`, in bytes
                num_tuples_per_block +                      // size of `i8`, in bytes
                2                                           // size of `b1` and `b2`, in bytes
            ) % 8 == 0,                                     // to let `is_null` start at a 64-bit-aligned address
            "NULL bitmap column is not 64 bit aligned and padding is added in contrast to PAX layout factory"
        );
        struct Block
        {
            int64_t i64[num_tuples_per_block]; // alignment 64 bits
            float f[num_tuples_per_block]; // alignment 32 bits
            char c[num_tuples_per_block][max_string_length]; // alignment 8 bits
            int8_t i8[num_tuples_per_block]; // alignment 8 bits
            uint8_t b1:(1 * num_tuples_per_block); // alignment 8 bits
            uint8_t :0; // padding to next byte
            uint8_t b2:(1 * num_tuples_per_block); // alignment 8 bits
            uint8_t :0; // padding to next byte
            uint8_t is_null[num_tuples_per_block]; // alignment 8 bits
        };

        /* Create data layout (using our standard PAX layout factory). */
        PAXLayoutFactory factory(PAXLayoutFactory::NTuples, num_tuples_per_block);
        table.layout(factory); // FIXME: does not only test a single unit, more like an integration test

        /* Create data. */
        data.f       = { 3.14f, -2.71f, -0.42f, 1.23f, 29.09f };
        data.i64     = { -123456789, 4321, 999, -10, 314 };
        data.b1      = { true, false, false, true, true };
        data.b2      = { false, true, false, false, true };
        data.c       = { "test", "success", "failed", "mutable", "UdS" };
        data.i8      = { 42, 17, -29, -1, 7 };
        data.is_null = { 0b110000, 0b000101, 0b001010, 0b001100, 0b100110 };

        /* Store data directly into memory of table through C struct. */
        Block* const mem_ptr = table.store().memory().as<Block*>();
        for (std::size_t idx = 0; idx < data.f.size(); ++idx) {
            auto block_ptr = mem_ptr + (idx / num_tuples_per_block);
            auto idx_in_block = idx % num_tuples_per_block;
            block_ptr->f[idx_in_block] = data.f.at(idx);
            block_ptr->i64[idx_in_block] = data.i64.at(idx);
            block_ptr->b1 ^= (-uint16_t(data.b1.at(idx)) ^ block_ptr->b1) & (uint16_t(1) << idx_in_block);
            block_ptr->b2 ^= (-uint16_t(data.b2.at(idx)) ^ block_ptr->b2) & (uint16_t(1) << idx_in_block);
            strncpy(block_ptr->c[idx_in_block], data.c.at(idx), max_string_length);
            block_ptr->i8[idx_in_block] = data.i8.at(idx);
            block_ptr->is_null[idx_in_block] = data.is_null.at(idx);
            table.store().append();
        }

        REQUIRE_NOTHROW(invoke());
    }

    SECTION("PAX in PAX layout")
    {
        /* Create C struct be able to store the data directly into memory. */
        constexpr std::size_t num_tuples_per_outer_block = 6;
        constexpr std::size_t num_tuples_per_left_inner_block = 3;
        constexpr std::size_t num_tuples_per_right_inner_block = 2;
        static_assert(num_tuples_per_outer_block % num_tuples_per_left_inner_block == 0,
                      "number of tuples in outer block must be a whole multiple of number of tuples in inner block");
        static_assert(num_tuples_per_outer_block % num_tuples_per_right_inner_block == 0,
                      "number of tuples in outer block must be a whole multiple of number of tuples in inner block");
        static_assert(num_tuples_per_left_inner_block <= 64,
                      "more than 12 tuples per block would exceed bool column's data types");
        static_assert(num_tuples_per_right_inner_block <= 10,
                      "more than 10 tuples per block would exceed NULL bitmap column's data type");
        struct LeftInnerBlock
        {
            float f[num_tuples_per_left_inner_block];
            /* 32 bits padding */
            int64_t i64[num_tuples_per_left_inner_block];
            /* no padding */
            uint64_t b1:(1 * num_tuples_per_left_inner_block);
            /* no padding */
            uint64_t b2:(1 * num_tuples_per_left_inner_block);
        };
        struct RightInnerBlock
        {
            char c[num_tuples_per_right_inner_block][max_string_length];
            /* no padding */
            int8_t i8[num_tuples_per_right_inner_block];
            uint64_t :0; // padding to next multiple of 8 bytes to prevent optimizations if `is_null` needs less space
            uint64_t is_null:(6 * num_tuples_per_right_inner_block);
        };
        struct OuterBlock
        {
            LeftInnerBlock left_block[num_tuples_per_outer_block / num_tuples_per_left_inner_block];
            /* no padding (because right block has alignment requirement of 8 bytes but size of left block must be a
             * multiple of 8 byte due to self alignment) */
            RightInnerBlock right_block[num_tuples_per_outer_block / num_tuples_per_right_inner_block];
        };

        /* Create data layout. */
        constexpr uint64_t offset_right_inner_block_in_bits =
            sizeof(LeftInnerBlock) * 8 * (num_tuples_per_outer_block / num_tuples_per_left_inner_block);
        DataLayout layout;
        auto &outer_block = layout.add_inode(num_tuples_per_outer_block, sizeof(OuterBlock) * 8);
        auto &left_block = outer_block.add_inode(num_tuples_per_left_inner_block, 0, sizeof(LeftInnerBlock) * 8);
        auto &right_block = outer_block.add_inode(
            num_tuples_per_right_inner_block, offset_right_inner_block_in_bits, sizeof(RightInnerBlock) * 8
        );
        uint64_t offset_in_bits = 0;
        for (std::size_t idx = 0; idx != 4; ++idx) {
            auto type = table[idx].type;
            if (auto rem = offset_in_bits % type->alignment())
                offset_in_bits += type->alignment() - rem; // padding
            left_block.add_leaf(type, idx, offset_in_bits, type->size());
            offset_in_bits += table[idx].type->size() * num_tuples_per_left_inner_block;
        }
        offset_in_bits = 0;
        for (std::size_t idx = 4; idx != table.num_attrs(); ++idx) {
            auto type = table[idx].type;
            if (auto rem = offset_in_bits % type->alignment())
                offset_in_bits += type->alignment() - rem; // padding
            right_block.add_leaf(type, idx, offset_in_bits, type->size());
            offset_in_bits += table[idx].type->size() * num_tuples_per_right_inner_block;
        }
        if (auto rem = offset_in_bits % 64)
            offset_in_bits += 64 - rem; // padding to next multiple of 8 bytes
        right_block.add_leaf(m::Type::Get_Bitmap(m::Type::TY_Vector, table.num_attrs()), table.num_attrs(), offset_in_bits, table.num_attrs());
        table.layout(std::move(layout));

        /* Create data. */
        data.f       = { 3.14f, -2.71f, -0.42f, 1.23f, 29.09f, -4.41f, 42.17f };
        data.i64     = { -123456789, 4321, 999, -10, 314, 123, -710 };
        data.b1      = { true, false, false, true, true, false, true };
        data.b2      = { false, true, false, false, true, true, true };
        data.c       = { "test", "success", "failed", "mutable", "UdS", "MMCI", "CS" };
        data.i8      = { 42, 17, -29, -1, 7, -3, 9 };
        data.is_null = { 0b110000, 0b000101, 0b001010, 0b001100, 0b100110, 0b000010, 0b000000 };

        /* Store data directly into memory of table through C struct. */
        OuterBlock* const mem_ptr = table.store().memory().as<OuterBlock*>();
        for (std::size_t idx = 0; idx < data.f.size(); ++idx) {
            auto outer_block_ptr = mem_ptr + (idx / num_tuples_per_outer_block);
            auto idx_in_outer_block = idx % num_tuples_per_outer_block;
            auto &left_block_ptr = outer_block_ptr->left_block[idx_in_outer_block / num_tuples_per_left_inner_block];
            auto idx_in_left_block = idx_in_outer_block % num_tuples_per_left_inner_block;
            auto &right_block_ptr = outer_block_ptr->right_block[idx_in_outer_block / num_tuples_per_right_inner_block];
            auto idx_in_right_block = idx_in_outer_block % num_tuples_per_right_inner_block;
            left_block_ptr.f[idx_in_left_block] = data.f.at(idx);
            left_block_ptr.i64[idx_in_left_block] = data.i64.at(idx);
            left_block_ptr.b1 ^= (-uint16_t(data.b1.at(idx)) ^ left_block_ptr.b1) & (uint16_t(1) << idx_in_left_block);
            left_block_ptr.b2 ^= (-uint16_t(data.b2.at(idx)) ^ left_block_ptr.b2) & (uint16_t(1) << idx_in_left_block);
            strncpy(right_block_ptr.c[idx_in_right_block], data.c.at(idx), max_string_length);
            right_block_ptr.i8[idx_in_right_block] = data.i8.at(idx);
            right_block_ptr.is_null = (right_block_ptr.is_null & ~(uint64_t(0b111111) << (idx_in_right_block * 6))) |
                                      (data.is_null.at(idx) << (idx_in_right_block * 6));
            table.store().append();
        }

        REQUIRE_NOTHROW(invoke());
    }

    m::WasmEngine::Dispose_Wasm_Context(Module::ID());
    Module::Dispose();
}
