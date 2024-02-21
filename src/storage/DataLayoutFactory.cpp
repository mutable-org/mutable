#include <mutable/storage/DataLayoutFactory.hpp>

#include <memory>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/Type.hpp>
#include <numeric>


using namespace m;
using namespace m::storage;

M_LCOV_EXCL_START
std::ostream & m::storage::operator<<(std::ostream &out, const DataLayoutFactory &factory)
{
    factory.print(out);
    return out;
}
void DataLayoutFactory::dump(std::ostream &out) const { out << *this << std::endl; }
void DataLayoutFactory::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP

namespace {

namespace options {

/** Whether to reorder attributes when creating data layouts. */
bool attribute_reordering = true;

/** Whether to remove the NULL bitmap in all created data layouts. */
bool remove_null_bitmap = false;

/** Whether to pack one tuple less than theoretically possible in a created PAX data layout. */
bool pax_pack_one_tuple_less = false;

}

__attribute__((constructor(201)))
static void add_storage_args()
{
    Catalog &C = Catalog::Get();

    /*----- Command-line arguments -----*/
    C.arg_parser().add<bool>(
        /* group=       */ "Storage",
        /* short=       */ nullptr,
        /* long=        */ "--no-attribute-reordering",
        /* description= */ "do not reorder attributes when creating data layouts, e.g. to minimize padding",
        /* callback=    */ [](bool){ options::attribute_reordering = false; }
    );
    C.arg_parser().add<bool>(
        /* group=       */ "Storage",
        /* short=       */ nullptr,
        /* long=        */ "--remove-null-bitmap",
        /* description= */ "remove the NULL bitmap in all created data layouts",
        /* callback=    */ [](bool){ options::remove_null_bitmap = true; }
    );
    C.arg_parser().add<bool>(
        /* group=       */ "Storage",
        /* short=       */ nullptr,
        /* long=        */ "--pax-pack-one-tuple-less",
        /* description= */ "pack one tuple less than possible into PAX blocks; only used for benchmarking purposes",
        /* callback=    */ [](bool){ options::pax_pack_one_tuple_less = true; }
    );
}

}


/** Computes the order for attributes of types \p types and returns this permutation as array of indices.  Attributes
 * are reordered by their alignment requirement to minimize padding except the CLI option `--no-attribute-reordering`
 * is set. */
std::unique_ptr<std::size_t[]>
compute_attribute_order(const std::vector<const Type*> &types)
{
    /*----- Collect all indices. -----*/
    auto indices = std::make_unique<std::size_t[]>(types.size());
    std::iota(indices.get(), indices.get() + types.size(), 0);

    if (options::attribute_reordering) {
        /*----- Sort indices by alignment. -----*/
        std::stable_sort(indices.get(), indices.get() + types.size(), [&](std::size_t left, std::size_t right) {
            return types[left]->alignment() > types[right]->alignment();
        });
    }

    return indices;
}

DataLayout RowLayoutFactory::make(std::vector<const Type*> types, std::size_t num_tuples) const
{
    M_insist(not types.empty(), "cannot make layout for zero types");

    auto indices = compute_attribute_order(types);
    uint64_t offsets[types.size()]; // in bits

    /*----- Compute offsets. -----*/
    uint64_t offset_in_bits = 0;
    uint64_t alignment_in_bits = 8;

    for (std::size_t idx = 0; idx != types.size(); ++idx) {
        const auto mapped_idx = indices[idx];
        offsets[mapped_idx] = offset_in_bits;
        offset_in_bits += types[mapped_idx]->size();
        alignment_in_bits = std::max(alignment_in_bits, types[mapped_idx]->alignment());
    }

    const uint64_t null_bitmap_offset = offset_in_bits;

    /*----- Compute row size with padding. -----*/
    if (not options::remove_null_bitmap)
        offset_in_bits += types.size(); // space for NULL bitmap
    if (uint64_t rem = offset_in_bits % alignment_in_bits; rem)
        offset_in_bits += alignment_in_bits - rem;
    const uint64_t row_size_in_bits = offset_in_bits;

    /*----- Construct DataLayout. -----*/
    DataLayout layout(num_tuples);
    auto &row = layout.add_inode(1, row_size_in_bits);
    for (std::size_t idx = 0; idx != types.size(); ++idx)
        row.add_leaf(types[idx], idx, offsets[idx], 0); // add attribute
    if (not options::remove_null_bitmap) {
        row.add_leaf( // add NULL bitmap
            /* type=           */ Type::Get_Bitmap(Type::TY_Vector, types.size()),
            /* idx=            */ types.size(),
            /* offset_in_bits= */ null_bitmap_offset,
            /* stride_in_bits= */ 0
        );
    }

    return layout;
}

DataLayout PAXLayoutFactory::make(std::vector<const Type*> types, std::size_t num_tuples) const
{
    M_insist(not types.empty(), "cannot make layout for zero types");

    auto indices = compute_attribute_order(types);
    uint64_t offsets[types.size() + 1]; // in bits

    /*----- Compute attribute offsets in a virtual row. -----*/
    uint64_t offset_in_bits = 0;
    uint64_t min_size_in_bytes = std::numeric_limits<uint64_t>::max();
    uint64_t alignment_in_bits = 8;
    std::size_t num_not_byte_aligned = 0;

    for (std::size_t idx = 0; idx != types.size(); ++idx) {
        const auto mapped_idx = indices[idx];
        offsets[mapped_idx] = offset_in_bits;
        offset_in_bits += types[mapped_idx]->size();
        min_size_in_bytes = std::min(min_size_in_bytes, (types[mapped_idx]->size() + 7) / 8);
        alignment_in_bits = std::max(alignment_in_bits, types[mapped_idx]->alignment());
        if (types[mapped_idx]->size() % 8)
            ++num_not_byte_aligned;
    }

    /*----- Compute NULL bitmap offset in a virtual row. -----*/
    const uint64_t null_bitmap_size_in_bits =
        options::remove_null_bitmap ? 0 : std::max(ceil_to_pow_2(types.size()), 8UL); // add padding to support SIMDfication
    offsets[types.size()] = offset_in_bits;
    if (null_bitmap_size_in_bits % 8)
        ++num_not_byte_aligned;

    /*----- Compute number of rows per block and number of blocks per row. -----*/
    const auto num_simd_lanes = std::max<std::size_t>(1, 16 / min_size_in_bytes); // possible number of SIMD lanes
    std::size_t num_rows_per_block, num_blocks_per_row;
    if (NTuples == option_) {
        num_rows_per_block = num_tuples_;
        if (num_rows_per_block > num_simd_lanes)
            num_rows_per_block =
                (num_tuples_ / num_simd_lanes) * num_simd_lanes; // floor to multiple of possible number of SIMD lanes
        num_blocks_per_row = 1;
    } else {
        const uint64_t row_size_in_bits = offsets[types.size()] + null_bitmap_size_in_bits; // space for NULL bitmap
        /* Compute number of rows within a PAX block. Consider worst case padding of 7 bits (because each column within
         * a PAX block must be byte aligned) for every possibly not byte-aligned attribute column. Null bitmap column is
         * ignored since it is the last column. */
        num_rows_per_block = std::max<std::size_t>(1, (num_bytes_ * 8 - num_not_byte_aligned * 7) / row_size_in_bits);
        if (num_rows_per_block > num_simd_lanes)
            num_rows_per_block =
                (num_rows_per_block / num_simd_lanes) * num_simd_lanes; // floor to multiple of possible number of SIMD lanes
        if (options::pax_pack_one_tuple_less and num_rows_per_block > 1)
            --num_rows_per_block;
        num_blocks_per_row = (row_size_in_bits + num_bytes_ * 8 - 1UL) / (num_bytes_ * 8);
    }

    /*----- Compute column offsets. -----*/
    uint64_t running_padding = 0;
    for (std::size_t idx = 0; idx != types.size(); ++idx) {
        const auto mapped_idx = indices[idx];
        offsets[mapped_idx] = offsets[mapped_idx] * num_rows_per_block + running_padding;
        M_insist(offsets[mapped_idx] % 8 == 0, "attribute column must be byte aligned");
        if (uint64_t bit_offset = (types[mapped_idx]->size() * num_rows_per_block) % 8; bit_offset)
            running_padding += 8UL - bit_offset;
    }
    offsets[types.size()] = offsets[types.size()] * num_rows_per_block + running_padding;

    /*----- Compute block size. -----*/
    uint64_t block_size_in_bits;
    if (NTuples == option_) {
        block_size_in_bits = offsets[types.size()] + null_bitmap_size_in_bits * num_rows_per_block;
        if (uint64_t alignment_offset = block_size_in_bits % alignment_in_bits)
            block_size_in_bits += alignment_in_bits - alignment_offset;
    } else {
        block_size_in_bits = num_bytes_ * 8;
    }

    M_insist(offsets[types.size()] % 8 == 0, "NULL bitmap column must be byte aligned");
    M_insist(offsets[types.size()] + null_bitmap_size_in_bits * num_rows_per_block <=
             block_size_in_bits * num_blocks_per_row,
             "computed block layout must not exceed block size");

    /*----- Construct DataLayout. -----*/
    DataLayout layout(num_tuples);
    auto &pax_block = layout.add_inode(num_rows_per_block, num_blocks_per_row * block_size_in_bits);
    for (std::size_t idx = 0; idx != types.size(); ++idx)
        pax_block.add_leaf(types[idx], idx, offsets[idx], types[idx]->size());
    if (not options::remove_null_bitmap) {
        pax_block.add_leaf( // add NULL bitmap
            /* type=           */ Type::Get_Bitmap(Type::TY_Vector, types.size()),
            /* idx=            */ types.size(),
            /* offset_in_bits= */ offsets[types.size()],
            /* stride_in_bits= */ null_bitmap_size_in_bits
        );
    }

    return layout;
}

__attribute__((constructor(202)))
static void register_data_layouts()
{
    Catalog &C = Catalog::Get();
#define REGISTER_PAX_BYTES(NAME, BLOCK_SIZE, DESCRIPTION) \
    C.register_data_layout(C.pool(#NAME), std::make_unique<PAXLayoutFactory>(PAXLayoutFactory::NBytes, BLOCK_SIZE), DESCRIPTION)
#define REGISTER_PAX_TUPLES(NAME, BLOCK_SIZE, DESCRIPTION) \
    C.register_data_layout(C.pool(#NAME), std::make_unique<PAXLayoutFactory>(PAXLayoutFactory::NTuples, BLOCK_SIZE), DESCRIPTION)
    REGISTER_PAX_BYTES(PAX4M, 1UL << 22, "stores attributes using PAX layout with 4MiB blocks"); // default
    REGISTER_PAX_BYTES(PAX4K, 1UL << 12, "stores attributes using PAX layout with 4KiB blocks");
    REGISTER_PAX_BYTES(PAX64K, 1UL << 16, "stores attributes using PAX layout with 64KiB blocks");
    REGISTER_PAX_BYTES(PAX512K, 1UL << 19, "stores attributes using PAX layout with 512KiB blocks");
    REGISTER_PAX_BYTES(PAX64M, 1UL << 26, "stores attributes using PAX layout with 64MiB blocks");
    REGISTER_PAX_TUPLES(PAX16Tup, 16, "stores attributes using PAX layout with blocks for 16 tuples");
    REGISTER_PAX_TUPLES(PAX128Tup, 128, "stores attributes using PAX layout with blocks for 128 tuples");
    REGISTER_PAX_TUPLES(PAX1024Tup, 1024, "stores attributes using PAX layout with blocks for 1024 tuples");
    C.register_data_layout(C.pool("Row"), std::make_unique<RowLayoutFactory>(), "stores attributes in row-major order");
#undef REGISTER_PAX
}
