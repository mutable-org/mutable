#include "backend/Interpreter.hpp"

#include "util/container/RefCountingHashMap.hpp"
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <iterator>
#include <mutable/Options.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/fn.hpp>
#include <numeric>
#include <type_traits>


using namespace m;
using namespace m::storage;


/*======================================================================================================================
 * Helper function
 *====================================================================================================================*/

/** Compile a `StackMachine` to load or store a tuple of `Schema` `tuple_schema` using a given memory address and a
 * given `DataLayout`.
 *
 * @param tuple_schema  the `Schema` of the tuple to load/store, specifying the `Schema::Identifier`s to load/store
 * @param address       the memory address of the `Store` we are loading from / storing to
 * @param layout        the `DataLayout` of the `Table` we are loading from / storing to
 * @param layout_schema the `Schema` of `layout`, specifying the `Schema::Identifier`s present in `layout`
 * @param row_id        the ID of the *first* row to load/store
 * @param tuple_id      the ID of the tuple used for loading/storing
 */
template<bool IsStore>
static StackMachine compile_data_layout(const Schema &tuple_schema, void *address, const DataLayout &layout,
                                        const Schema &layout_schema, std::size_t row_id, std::size_t tuple_id)
{
    StackMachine SM; // the `StackMachine` to compile

    struct stride_info_t
    {
        std::size_t counter_id;
        uint64_t num_tuples;
        uint64_t stride_in_bits;
    };
    std::vector<stride_info_t> stride_info_stack; // used to track all strides from root to leaf

    struct {
        std::size_t id = -1UL; ///< context id
        std::size_t offset_id = -1UL; ///< id to keep track of current adjustable bit offset in case of a bit stride
        uintptr_t bit_offset; ///< fixed offset, in bits
        uint64_t bit_stride; ///< stride in bits
        uint64_t num_tuples; ///< number of tuples of the linearization in which the null bitmap is stored
        std::size_t row_id; ///< the row id within the linearization in which the null bitmap is stored

        operator bool() { return id != -1UL; }
        bool adjustable_offset() { return offset_id != -1UL; }
    } null_bitmap_info;

    std::unordered_map<std::size_t, std::size_t> leaf2id;
    std::unordered_map<std::size_t, std::size_t> leaf2mask;

    /*----- Check whether any of the entries in `tuple_schema` can be NULL, so that we need the NULL bitmap. -----*/
    const bool needs_null_bitmap = [&]() {
        for (auto &tuple_entry : tuple_schema) {
            if (layout_schema[tuple_entry.id].second.nullable())
                return true; // found an entry in `tuple_schema` that can be NULL according to `layout_schema`
        }
        return false; // no attribute in `tuple_schema` can be NULL according to `layout_schema`
    }();

    /* Compute location of NULL bitmap. */
    const auto null_bitmap_idx = layout_schema.num_entries();
    auto find_null_bitmap = [&](const DataLayout &layout, std::size_t row_id) -> void {
        auto find_null_bitmap_impl = [&](const DataLayout::INode &node, uintptr_t offset, std::size_t row_id,
                                         auto &find_null_bitmap_ref) -> void
        {
            for (auto &child : node) {
                if (auto child_leaf = cast<const DataLayout::Leaf>(child.ptr.get())) {
                    if (child_leaf->index() == null_bitmap_idx) {
                        M_insist(not null_bitmap_info, "there must be at most one null bitmap in the linearization");
                        const uint64_t additional_offset_in_bits = child.offset_in_bits + row_id * child.stride_in_bits;
                        /* add NULL bitmap address to context */
                        null_bitmap_info.id = SM.add(reinterpret_cast<void*>(offset + additional_offset_in_bits / 8));
                        null_bitmap_info.bit_offset = (additional_offset_in_bits) % 8;
                        null_bitmap_info.bit_stride = child.stride_in_bits;
                        null_bitmap_info.num_tuples = node.num_tuples();
                        null_bitmap_info.row_id = row_id;
                    }
                } else {
                    auto child_inode = as<const DataLayout::INode>(child.ptr.get());
                    const std::size_t lin_id = row_id / child_inode->num_tuples();
                    const std::size_t inner_row_id = row_id % child_inode->num_tuples();
                    const uint64_t additional_offset = child.offset_in_bits / 8 + lin_id * child.stride_in_bits / 8;
                    find_null_bitmap_ref(*child_inode, offset + additional_offset, inner_row_id, find_null_bitmap_ref);
                }
            }
        };
        find_null_bitmap_impl(static_cast<const DataLayout::INode&>(layout), uintptr_t(address), row_id,
                              find_null_bitmap_impl);
    };
    if (needs_null_bitmap)
        find_null_bitmap(layout, row_id);
    if (null_bitmap_info and null_bitmap_info.bit_stride) {
        null_bitmap_info.offset_id = SM.add(null_bitmap_info.bit_offset); // add initial NULL bitmap offset to context
    }

    /* Emit code for attribute access and pointer increment. */
    auto compile_accesses = [&](const DataLayout &layout, std::size_t row_id) -> void {
        auto compile_accesses_impl = [&](const DataLayout::INode &node, uintptr_t offset, std::size_t row_id,
                                         auto &compile_accesses_ref) -> void
        {
            for (auto &child : node) {
                if (auto child_leaf = cast<const DataLayout::Leaf>(child.ptr.get())) {
                    if (child_leaf->index() != null_bitmap_idx) {
                        const bool attr_can_be_null = null_bitmap_info and layout_schema[child_leaf->index()].nullable();
                        auto &id = layout_schema[child_leaf->index()].id;

                        /* Locate the attribute in the operator schema. */
                        if (auto it = tuple_schema.find(id); it != tuple_schema.end()) {
                            uint64_t idx = std::distance(tuple_schema.begin(), it); // get attribute index in schema
                            const uint64_t additional_offset_in_bits = child.offset_in_bits + row_id * child.stride_in_bits;
                            const std::size_t byte_offset = additional_offset_in_bits / 8;
                            const std::size_t bit_offset = additional_offset_in_bits % 8;
                            M_insist(not bit_offset or child_leaf->type()->is_boolean() or child_leaf->type()->is_bitmap(),
                                     "only booleans and bitmaps may not be byte aligned");

                            const std::size_t byte_stride = child.stride_in_bits / 8;
                            const std::size_t bit_stride  = child.stride_in_bits % 8;
                            M_insist(not bit_stride or child_leaf->type()->is_boolean() or child_leaf->type()->is_bitmap(),
                                     "only booleans and bitmaps may not be byte aligned");
                            M_insist(bit_stride == 0 or byte_stride == 0,
                                     "the stride must be a whole multiple of a byte or less than a byte");

                            /* Access NULL bit. */
                            if (attr_can_be_null) {
                                if (not null_bitmap_info.bit_stride) {
                                    /* No bit stride means the NULL bitmap only advances with parent sequence. */
                                    const std::size_t bit_offset = null_bitmap_info.bit_offset + child_leaf->index();
                                    if (bit_offset < 8) {
                                        SM.emit_Ld_Ctx(null_bitmap_info.id);
                                        if constexpr (IsStore) {
                                            SM.emit_Ld_Tup(tuple_id, idx);
                                            SM.emit_Is_Null();
                                            SM.emit_St_b(bit_offset);
                                        } else {
                                            SM.emit_Ld_b(0x1UL << bit_offset);
                                        }
                                    } else {
                                        /* Advance to respective byte. */
                                        SM.add_and_emit_load(uint64_t(bit_offset / 8));
                                        SM.emit_Ld_Ctx(null_bitmap_info.id);
                                        SM.emit_Add_p();
                                        if constexpr (IsStore) {
                                            SM.emit_Ld_Tup(tuple_id, idx);
                                            SM.emit_Is_Null();
                                            SM.emit_St_b(bit_offset % 8);
                                        } else {
                                            SM.emit_Ld_b(0x1UL << (bit_offset % 8));
                                        }
                                    }
                                } else {
                                    /* With bit stride. Use adjustable offset instead of fixed offset. */
                                    M_insist(null_bitmap_info.adjustable_offset());

                                    /* Create variables for address and mask in context. Only used for storing.*/
                                    std::size_t address_id, mask_id;
                                    if constexpr (IsStore) {
                                        address_id = SM.add(reinterpret_cast<void*>(0));
                                        mask_id = SM.add(uint64_t(0));
                                    }

                                    /* Compute address of entire byte containing the NULL bit. */
                                    SM.emit_Ld_Ctx(null_bitmap_info.offset_id);
                                    SM.add_and_emit_load(child_leaf->index());
                                    SM.emit_Add_i();
                                    SM.emit_SARi_i(3); // (adj_offset + attr.id) / 8
                                    SM.emit_Ld_Ctx(null_bitmap_info.id);
                                    SM.emit_Add_p();
                                    if constexpr (IsStore)
                                        SM.emit_Upd_Ctx(address_id); // store address in context
                                    else
                                        SM.emit_Ld_i8(); // load byte from address

                                    if constexpr (IsStore) {
                                        /* Test whether value equals NULL. */
                                        SM.emit_Ld_Tup(tuple_id, idx);
                                        SM.emit_Is_Null();
                                    }

                                    /* Initialize mask. */
                                    SM.add_and_emit_load(uint64_t(0x1UL));

                                    /* Compute offset of NULL bit. */
                                    SM.emit_Ld_Ctx(null_bitmap_info.offset_id);
                                    SM.add_and_emit_load(child_leaf->index());
                                    SM.emit_Add_i();
                                    SM.add_and_emit_load(uint64_t(0b111));
                                    SM.emit_And_i(); // (adj_offset + attr.id) % 8

                                    /* Shift mask by offset. */
                                    SM.emit_ShL_i();
                                    if constexpr (IsStore) {
                                        SM.emit_Upd_Ctx(mask_id); // store mask in context

                                        /* Load byte and set NULL bit to 1. */
                                        SM.emit_Ld_Ctx(address_id);
                                        SM.emit_Ld_i8();
                                        SM.emit_Or_i(); // in case of NULL

                                        /* Load byte and set NULL bit to 0. */
                                        SM.emit_Ld_Ctx(mask_id);
                                        SM.emit_Neg_i();
                                        SM.emit_Ld_Ctx(address_id);
                                        SM.emit_Ld_i8();
                                        SM.emit_And_i(); // in case of not NULL

                                        SM.emit_Sel(); // select the respective modified byte

                                        /* Write entire byte back to the store. */
                                        SM.emit_St_i8();
                                    } else {
                                        /* Apply mask and cast to boolean. */
                                        SM.emit_And_i();
                                        SM.emit_NEZ_i();
                                    }
                                }
                                if constexpr (not IsStore)
                                    SM.emit_Push_Null(); // to select it later if NULL
                            }

                            /* Introduce leaf pointer. */
                            const std::size_t offset_id = SM.add_and_emit_load(reinterpret_cast<void*>(offset + byte_offset));
                            leaf2id[child_leaf->index()] = offset_id;

                            if (bit_stride) {
                                M_insist(child_leaf->type()->is_boolean(), "only booleans are supported yet");

                                if constexpr (IsStore) {
                                    /* Load value to stack. */
                                    SM.emit_Ld_Tup(tuple_id, idx); // boolean
                                } else {
                                    /* Load byte with the respective value. */
                                    SM.emit_Ld_i8();
                                }

                                /* Introduce mask. */
                                const std::size_t mask_id = SM.add(uint64_t(0x1UL << bit_offset));
                                leaf2mask[child_leaf->index()] = mask_id;

                                if constexpr (IsStore) {
                                    /* Load byte and set bit to 1. */
                                    SM.emit_Ld_Ctx(offset_id);
                                    SM.emit_Ld_i8();
                                    SM.emit_Ld_Ctx(mask_id);
                                    SM.emit_Or_i(); // in case of TRUE

                                    /* Load byte and set bit to 0. */
                                    SM.emit_Ld_Ctx(offset_id);
                                    SM.emit_Ld_i8();
                                    SM.emit_Ld_Ctx(mask_id);
                                    SM.emit_Neg_i(); // negate mask
                                    SM.emit_And_i(); // in case of FALSE

                                    SM.emit_Sel(); // select the respective modified byte

                                    /* Write entire byte back to the store. */
                                    SM.emit_St_i8();
                                } else {
                                    /* Load and apply mask and convert to bool. */
                                    SM.emit_Ld_Ctx(mask_id);
                                    SM.emit_And_i();
                                    SM.emit_NEZ_i();

                                    if (attr_can_be_null)
                                        SM.emit_Sel();

                                    /* Store value in output tuple. */
                                    SM.emit_St_Tup(tuple_id, idx, child_leaf->type());
                                    SM.emit_Pop();
                                }

                                /* Update the mask. */
                                SM.emit_Ld_Ctx(mask_id);
                                SM.emit_ShLi_i(1);
                                SM.emit_Upd_Ctx(mask_id);

                                /* Check whether we are in the 8th iteration and reset mask. */
                                SM.add_and_emit_load(uint64_t(0x1UL) << 8);
                                SM.emit_Eq_i();
                                SM.emit_Dup(); // duplicate outcome for later use
                                SM.add_and_emit_load(uint64_t(0x1UL));
                                SM.emit_Ld_Ctx(mask_id);
                                SM.emit_Sel();
                                SM.emit_Upd_Ctx(mask_id); // mask <- mask == 256 ? 1 : mask
                                SM.emit_Pop();

                                /* If the mask was reset, advance to the next byte. */
                                SM.emit_Cast_i_b(); // convert outcome of previous check to int
                                SM.emit_Ld_Ctx(offset_id);
                                SM.emit_Add_p();
                                SM.emit_Upd_Ctx(offset_id);
                                SM.emit_Pop();
                            } else {
                                if constexpr (IsStore) {
                                    /* Load value to stack. */
                                    SM.emit_Ld_Tup(tuple_id, idx);

                                    /* Store value. */
                                    if (child_leaf->type()->is_boolean())
                                        SM.emit_St_b(bit_offset);
                                    else
                                        SM.emit_St(child_leaf->type());
                                } else {
                                    /* Load value. */
                                    if (child_leaf->type()->is_boolean())
                                        SM.emit_Ld_b(0x1UL << bit_offset); // convert the fixed bit offset to a fixed mask
                                    else
                                        SM.emit_Ld(child_leaf->type());

                                    if (attr_can_be_null)
                                        SM.emit_Sel();

                                    /* Store value in output tuple. */
                                    SM.emit_St_Tup(tuple_id, idx, child_leaf->type());
                                    SM.emit_Pop();
                                }

                                /* If the attribute has a stride, advance the pointer accordingly. */
                                M_insist(not bit_stride);
                                if (byte_stride) {
                                    /* Advance the attribute pointer by the attribute's stride. */
                                    SM.add_and_emit_load(int64_t(byte_stride));
                                    SM.emit_Ld_Ctx(offset_id);
                                    SM.emit_Add_p();
                                    SM.emit_Upd_Ctx(offset_id);
                                    SM.emit_Pop();
                                }
                            }

                        }
                    }
                } else {
                    auto child_inode = as<const DataLayout::INode>(child.ptr.get());
                    const std::size_t lin_id = row_id / child_inode->num_tuples();
                    const std::size_t inner_row_id = row_id % child_inode->num_tuples();
                    const uint64_t additional_offset = child.offset_in_bits / 8 + lin_id * child.stride_in_bits / 8;
                    compile_accesses_ref(*child_inode, offset + additional_offset, inner_row_id, compile_accesses_ref);
                }
            }
        };
        compile_accesses_impl(static_cast<const DataLayout::INode&>(layout), uintptr_t(address), row_id,
                              compile_accesses_impl);
    };
    compile_accesses(layout, row_id);

    /* If the NULL bitmap has a stride, advance the adjustable offset accordingly. */
    if (null_bitmap_info and null_bitmap_info.bit_stride) {
        M_insist(null_bitmap_info.adjustable_offset());
        M_insist(null_bitmap_info.num_tuples > 1);

        /* Update adjustable offset. */
        SM.emit_Ld_Ctx(null_bitmap_info.offset_id);
        SM.add_and_emit_load(null_bitmap_info.bit_stride);
        SM.emit_Add_i();
        SM.emit_Upd_Ctx(null_bitmap_info.offset_id);
        SM.emit_Pop();

        /* Check whether we are in the last iteration and advance to correct byte. */
        const auto counter_id = SM.add_and_emit_load(uint64_t(null_bitmap_info.row_id));
        SM.emit_Inc();
        SM.emit_Upd_Ctx(counter_id);
        SM.add_and_emit_load(null_bitmap_info.num_tuples);
        SM.emit_NE_i();
        SM.emit_Dup(); SM.emit_Dup(); // triple outcome for later use
        SM.emit_Not_b(); // negate outcome of check
        SM.emit_Cast_i_b(); // convert to int
        SM.emit_Ld_Ctx(null_bitmap_info.offset_id);
        SM.emit_SARi_i(3); // corresponds div 8
        SM.emit_Mul_i();
        SM.emit_Ld_Ctx(null_bitmap_info.id);
        SM.emit_Add_p();
        SM.emit_Upd_Ctx(null_bitmap_info.id); // id <- counter != num_tuples ? id : id + adj_offset / 8
        SM.emit_Pop();

        /* If we were in the last iteration, reset adjustable offset. */
        SM.emit_Cast_i_b(); // convert outcome of previous check to int
        SM.emit_Ld_Ctx(null_bitmap_info.offset_id);
        SM.emit_Mul_i();
        SM.emit_Upd_Ctx(null_bitmap_info.offset_id);
        SM.emit_Pop();

        /* If we were in the last iteration, reset the counter. */
        SM.emit_Cast_i_b(); // convert outcome of previous check to int
        SM.emit_Ld_Ctx(counter_id);
        SM.emit_Mul_i();
        SM.emit_Upd_Ctx(counter_id);
        SM.emit_Pop();
    }

    /* Emit code to gap strides. */
    auto compile_strides = [&](const DataLayout &layout, std::size_t row_id) -> void {
        auto compile_strides_impl = [&](const DataLayout::INode &node, std::size_t row_id,
                                        auto &compile_strides_ref) -> void {
            for (auto &child : node) {
                if (auto child_leaf = cast<const DataLayout::Leaf>(child.ptr.get())) {
                    std::size_t offset_id;
                    std::size_t mask_id = -1UL;
                    if (null_bitmap_info and child_leaf->index() == null_bitmap_idx) {
                        offset_id = null_bitmap_info.id;
                        mask_id = null_bitmap_info.offset_id;
                    } else if (auto it = leaf2id.find(child_leaf->index()); it != leaf2id.end()) {
                        offset_id = it->second;
                        if (auto it = leaf2mask.find(child_leaf->index()); it != leaf2mask.end())
                            mask_id = it->second;
                    } else {
                        continue; // nothing to be done
                    }

                    /* Emit code for stride jumps. */
                    std::size_t prev_num_tuples = 1;
                    std::size_t prev_stride_in_bits = child.stride_in_bits;
                    for (auto it = stride_info_stack.rbegin(), end = stride_info_stack.rend(); it != end; ++it) {
                        auto &info = *it;

                        /* Compute the remaining stride in bits. */
                        const std::size_t stride_remaining_in_bits =
                            info.stride_in_bits - (info.num_tuples / prev_num_tuples) * prev_stride_in_bits;

                        /* Perform stride jump, if necessary. */
                        if (stride_remaining_in_bits) {
                            std::size_t byte_stride = stride_remaining_in_bits / 8;
                            const std::size_t bit_stride = stride_remaining_in_bits % 8;

                            if (bit_stride) {
                                M_insist(child_leaf->index() == null_bitmap_idx or child_leaf->type()->is_boolean(),
                                       "only the null bitmap or booleans may cause not byte aligned stride jumps, "
                                       "bitmaps are not supported yet");
                                M_insist(child_leaf->index() != null_bitmap_idx or null_bitmap_info.adjustable_offset(),
                                       "only null bitmaps with adjustable offset may cause not byte aligned stride jumps");
                                M_insist(mask_id != -1UL);

                                /* Reset mask. */
                                if (child_leaf->index() == null_bitmap_idx) {
                                    /* Reset adjustable bit offset to 0. */
                                    if (info.num_tuples != 1) {
                                        /* Check whether counter equals num_tuples. */
                                        SM.emit_Ld_Ctx(info.counter_id);
                                        SM.add_and_emit_load(int64_t(info.num_tuples));
                                        SM.emit_NE_i();
                                        SM.emit_Cast_i_b();
                                    } else {
                                        SM.add_and_emit_load(uint64_t(0));
                                    }
                                    SM.emit_Ld_Ctx(mask_id);
                                    SM.emit_Mul_i();
                                    SM.emit_Upd_Ctx(mask_id);
                                    SM.emit_Pop();
                                } else {
                                    /* Reset mask to 0x1UL to access first bit again. */
                                    if (info.num_tuples != 1) {
                                        /* Check whether counter equals num_tuples. */
                                        SM.emit_Ld_Ctx(info.counter_id);
                                        SM.add_and_emit_load(int64_t(info.num_tuples));
                                        SM.emit_Eq_i();
                                        SM.add_and_emit_load(uint64_t(0x1UL));
                                        SM.emit_Ld_Ctx(mask_id);
                                        SM.emit_Sel();
                                    } else {
                                        SM.add_and_emit_load(uint64_t(0x1UL));
                                    }
                                    SM.emit_Upd_Ctx(mask_id);
                                    SM.emit_Pop();
                                }

                                /* Ceil to next entire byte. */
                                ++byte_stride;
                            }

                            /* Advance pointer. */
                            if (info.num_tuples != 1) {
                                /* Check whether counter equals num_tuples. */
                                SM.emit_Ld_Ctx(info.counter_id);
                                SM.add_and_emit_load(int64_t(info.num_tuples));
                                SM.emit_Eq_i();
                                SM.emit_Cast_i_b();

                                SM.add_and_emit_load(byte_stride);
                                SM.emit_Mul_i();
                            } else {
                                SM.add_and_emit_load(byte_stride);
                            }
                            SM.emit_Ld_Ctx(offset_id);
                            SM.emit_Add_p();
                            SM.emit_Upd_Ctx(offset_id);
                            SM.emit_Pop();
                        }

                        /* Update variables for next iteration. */
                        prev_num_tuples = info.num_tuples;
                        prev_stride_in_bits = info.stride_in_bits;
                    }
                } else {
                    auto child_inode = as<const DataLayout::INode>(child.ptr.get());

                    /* Initialize counter and emit increment. */
                    const std::size_t inner_row_id = row_id % child_inode->num_tuples();
                    const auto counter_id = SM.add_and_emit_load(inner_row_id); // introduce counter to track iteration count
                    SM.emit_Inc();
                    SM.emit_Upd_Ctx(counter_id);
                    SM.emit_Pop(); // XXX: not needed if recursion cleans up stack properly

                    /* Put context on stack and perform recursive descend. */
                    stride_info_stack.push_back(stride_info_t{
                        .counter_id = counter_id,
                        .num_tuples = child_inode->num_tuples(),
                        .stride_in_bits = child.stride_in_bits
                    });
                    compile_strides_ref(*child_inode, inner_row_id, compile_strides_ref);
                    stride_info_stack.pop_back();

                    /* Reset counter if iteration is whole multiple of num_tuples. */
                    if (child_inode->num_tuples() != 1) {
                        SM.emit_Ld_Ctx(counter_id); // XXX: not needed if recursion cleans up stack properly
                        SM.add_and_emit_load(child_inode->num_tuples());
                        SM.emit_NE_i();
                        SM.emit_Cast_i_b();
                        SM.emit_Ld_Ctx(counter_id);
                        SM.emit_Mul_i();
                    } else {
                        SM.add_and_emit_load(int64_t(0));
                    }
                    SM.emit_Upd_Ctx(counter_id);
                }
            }
        };
        compile_strides_impl(static_cast<const DataLayout::INode&>(layout), row_id, compile_strides_impl);
    };
    compile_strides(layout, row_id);

    return SM;
}

StackMachine Interpreter::compile_load(const Schema &tuple_schema, void *address, const storage::DataLayout &layout,
                                       const Schema &layout_schema, std::size_t row_id, std::size_t tuple_id)
{
    return compile_data_layout<false>(tuple_schema, address, layout, layout_schema, row_id, tuple_id);
}

StackMachine Interpreter::compile_store(const Schema &tuple_schema, void *address, const storage::DataLayout &layout,
                                        const Schema &layout_schema, std::size_t row_id, std::size_t tuple_id)
{
    return compile_data_layout<true>(tuple_schema, address, layout, layout_schema, row_id, tuple_id);
}

/*======================================================================================================================
 * Declaration of operator data.
 *====================================================================================================================*/

namespace {

struct PrintData : OperatorData
{
    uint32_t num_rows = 0;
    StackMachine printer;
    PrintData(const PrintOperator &op)
        : printer(op.schema())
    {
        auto &S = op.schema();
        auto ostream_index = printer.add(&op.out);
        for (std::size_t i = 0; i != S.num_entries(); ++i) {
            if (i != 0)
                printer.emit_Putc(ostream_index, ',');
            printer.emit_Ld_Tup(0, i);
            printer.emit_Print(ostream_index, S[i].type);
        }
    }
};

struct NoOpData : OperatorData
{
    uint32_t num_rows = 0;
};

struct ProjectionData : OperatorData
{
    Pipeline pipeline;
    std::optional<StackMachine> projections;
    Tuple res;

    ProjectionData(const ProjectionOperator &op)
        : pipeline(op.schema())
        , res(op.schema())
    { }

    void emit_projections(const Schema &pipeline_schema, const ProjectionOperator &op) {
        projections.emplace(pipeline_schema);
        std::size_t out_idx = 0;
        for (auto &p : op.projections()) {
            projections->emit(p.first.get(), 1);
            projections->emit_St_Tup(0, out_idx++, p.first.get().type());
        }
    }
};

struct JoinData : OperatorData
{
    Pipeline pipeline;
    std::vector<StackMachine> load_attrs;

    JoinData(const JoinOperator &op) : pipeline(op.schema()) { }

    void emit_load_attrs(const Schema &in_schema) {
        auto &SM = load_attrs.emplace_back();
        for (std::size_t schema_idx = 0; schema_idx != in_schema.num_entries(); ++schema_idx) {
            auto &e = in_schema[schema_idx];
            auto it = pipeline.schema().find(e.id);
            if (it != pipeline.schema().end()) { // attribute is needed
                SM.emit_Ld_Tup(1, schema_idx);
                SM.emit_St_Tup(0, std::distance(pipeline.schema().begin(), it), e.type);
            }
        }
    }
};

struct NestedLoopsJoinData : JoinData
{
    using buffer_type = std::vector<Tuple>;

    StackMachine predicate; ///< evaluated the predicate to a bool
    std::vector<Schema> buffer_schemas; ///< schema of each buffer
    buffer_type *buffers; ///< tuple buffer per child
    std::size_t active_child;
    Tuple res;

    NestedLoopsJoinData(const JoinOperator &op)
        : JoinData(op)
        , buffers(new buffer_type[op.children().size() - 1])
        , res({ Type::Get_Boolean(Type::TY_Vector) })
    { }

    ~NestedLoopsJoinData() { delete[] buffers; }
};

struct SimpleHashJoinData : JoinData
{
    bool is_probe_phase = false; ///< determines whether tuples are used to *build* or *probe* the hash table
    std::vector<std::pair<const ast::Expr*, const ast::Expr*>> exprs;
    StackMachine build_key; ///< extracts the key of the build input
    StackMachine probe_key; ///< extracts the key of the probe input
    RefCountingHashMap<Tuple, Tuple> ht; ///< hash table on build input

    Schema key_schema; ///< the `Schema` of the `key`
    Tuple key; ///< `Tuple` to hold the key

    SimpleHashJoinData(const JoinOperator &op)
        : JoinData(op)
        , ht(1024)
    {
        auto &schema_lhs = op.child(0)->schema();
#ifndef NDEBUG
        auto &schema_rhs = op.child(1)->schema();
#endif

        /* Decompose each join predicate of the form `A.x = B.y` into parts `A.x` and `B.y` and build the schema of the
         * join key. */
        auto &pred = op.predicate();
        for (auto &clause : pred) {
            M_insist(clause.size() == 1, "invalid predicate for simple hash join");
            auto &literal = clause[0];
            M_insist(not literal.negative(), "invalid predicate for simple hash join");
            auto &expr = literal.expr();
            auto binary = as<const ast::BinaryExpr>(&expr);
            M_insist(binary->tok == TK_EQUAL);
            auto first = binary->lhs.get();
            auto second = binary->rhs.get();
            M_insist(is_comparable(first->type(), second->type()), "the two sides of a comparison should be comparable");
            M_insist(first->type() == second->type(), "operand types must be equal");

            /* Add type to general key schema. */
            key_schema.add("key", first->type());

            /*----- Decide which side of the join the predicate belongs to. -----*/
            auto required_by_first = first->get_required();
#ifndef NDEBUG
            auto required_by_second = second->get_required();
#endif
            if ((required_by_first & schema_lhs).num_entries() != 0) {
#ifndef NDEBUG
                M_insist((required_by_second & schema_rhs).num_entries() != 0, "second must belong to RHS");
#endif
                exprs.emplace_back(first, second);
            } else {
#ifndef NDEBUG
                M_insist((required_by_first & schema_rhs).num_entries() != 0, "first must belong to RHS");
                M_insist((required_by_second & schema_lhs).num_entries() != 0, "second must belong to LHS");
#endif
                exprs.emplace_back(second, first);
            }
        }

        /* Create the tuple holding a key. */
        key = Tuple(key_schema);
    }

    void load_build_key(const Schema &pipeline_schema) {
        for (std::size_t i = 0; i != exprs.size(); ++i) {
            const ast::Expr *expr = exprs[i].first;
            build_key.emit(*expr, pipeline_schema, 1); // compile expr
            build_key.emit_St_Tup(0, i, expr->type()); // write result to index i
        }
    }

    void load_probe_key(const Schema &pipeline_schema) {
        for (std::size_t i = 0; i != exprs.size(); ++i) {
            const ast::Expr *expr = exprs[i].second;
            probe_key.emit(*expr, pipeline_schema, 1); // compile expr
            probe_key.emit_St_Tup(0, i, expr->type()); // write result to index i
        }
    }
};

struct LimitData : OperatorData
{
    std::size_t num_tuples = 0;
};

struct GroupingData : OperatorData
{
    Pipeline pipeline;
    StackMachine compute_key; ///< computes the key for a tuple
    std::vector<StackMachine> compute_aggregate_arguments; ///< StackMachines to compute the argumetns of aggregations
    std::vector<Tuple> args; ///< tuple used to hold the computed arguments

    GroupingData(const GroupingOperator &op)
        : pipeline(op.schema())
        , compute_key(op.child(0)->schema())
    {
        std::ostringstream oss;

        /* Compile the stack machine to compute the key and compute the key schema. */
        {
            std::size_t key_idx = 0;
            for (auto [grp, alias] : op.group_by()) {
                compute_key.emit(grp.get(), 1);
                compute_key.emit_St_Tup(0, key_idx++, grp.get().type());
            }
        }

        /* Compile a StackMachine to compute the arguments of each aggregation function.  For example, for the
         * aggregation `AVG(price * tax)`, the compiled StackMachine computes `price * tax`. */
        for (auto agg : op.aggregates()) {
            auto &fe = as<const ast::FnApplicationExpr>(agg.get());
            std::size_t arg_idx = 0;
            StackMachine sm(op.child(0)->schema());
            std::vector<const Type*> arg_types;
            for (auto &arg : fe.args) {
                sm.emit(*arg, 1);
                sm.emit_Cast(agg.get().type(), arg->type()); // cast argument type to aggregate type, e.g. f32 to f64 for SUM
                sm.emit_St_Tup(0, arg_idx++, arg->type());
                arg_types.push_back(arg->type());
            }
            args.emplace_back(Tuple(arg_types));
            compute_aggregate_arguments.emplace_back(std::move(sm));
        }
    }
};

struct AggregationData : OperatorData
{
    Pipeline pipeline;
    Tuple aggregates;
    std::vector<StackMachine> compute_aggregate_arguments; ///< StackMachines to compute the argumetns of aggregations
    std::vector<Tuple> args; ///< tuple used to hold the computed arguments

    AggregationData(const AggregationOperator &op)
        : pipeline(op.schema())
    {
        std::vector<const Type*> types;
        for (auto &e : op.schema())
            types.push_back(e.type);
        types.push_back(Type::Get_Integer(Type::TY_Scalar, 8)); // add nth_tuple counter
        aggregates = Tuple(std::move(types));
        aggregates.set(op.schema().num_entries(), 0L); // initialize running count

        for (auto agg : op.aggregates()) {
            auto &fe = as<const ast::FnApplicationExpr>(agg.get());
            std::size_t arg_idx = 0;
            StackMachine sm(op.child(0)->schema());
            std::vector<const Type*> arg_types;
            for (auto &arg : fe.args) {
                sm.emit(*arg, 1);
                sm.emit_Cast(agg.get().type(), arg->type()); // cast argument type to aggregate type, e.g. f32 to f64 for SUM
                sm.emit_St_Tup(0, arg_idx++, agg.get().type()); // store casted argument of aggregate type to tuple
                arg_types.push_back(agg.get().type());
            }
            args.emplace_back(Tuple(arg_types));
            compute_aggregate_arguments.emplace_back(std::move(sm));
        }
    }
};

struct HashBasedGroupingData : GroupingData
{
    /** Callable to compute the hash of the keys of a tuple. */
    struct hasher
    {
        std::size_t key_size;

        hasher(std::size_t key_size) : key_size(key_size) { }

        uint64_t operator()(const Tuple &tup) const {
            std::hash<Value> h;
            uint64_t hash = 0xcbf29ce484222325;
            for (std::size_t i = 0; i != key_size; ++i) {
                hash ^= tup.is_null(i) ? 0 : h(tup[i]);
                hash *= 1099511628211;
            }
            return hash;
        }
    };

    /** Callable to compare two tuples by their keys. */
    struct equals
    {
        std::size_t key_size;

        equals(std::size_t key_size) : key_size(key_size) { }

        uint64_t operator()(const Tuple &first, const Tuple &second) const {
            for (std::size_t i = 0; i != key_size; ++i) {
                if (first.is_null(i) != second.is_null(i)) return false;
                if (not first.is_null(i))
                    if (first.get(i) != second.get(i)) return false;
            }
            return true;
        }
    };

    /** A map of `Tuple`s, where the key part is used for hashing and comparison.  The mapped to value holds the count
     * of tuples that belong to this group. */
    std::unordered_map<Tuple, unsigned, hasher, equals> groups;

    HashBasedGroupingData(const GroupingOperator &op)
        : GroupingData(op)
        , groups(1024, hasher(op.group_by().size()), equals(op.group_by().size()))
    { }
};

struct SortingData : OperatorData
{
    Pipeline pipeline;
    std::vector<Tuple> buffer;

    SortingData(Schema buffer_schema) : pipeline(std::move(buffer_schema)) { }
};

struct FilterData : OperatorData
{
    StackMachine filter;
    Tuple res;

    FilterData(const FilterOperator &op, const Schema &pipeline_schema)
        : filter(pipeline_schema)
        , res({ Type::Get_Boolean(Type::TY_Vector) })
    {
        filter.emit(op.filter(), 1);
        filter.emit_St_Tup_b(0, 0);
    }
};

struct DisjunctiveFilterData : OperatorData
{
    std::vector<StackMachine> predicates;
    Tuple res;

    DisjunctiveFilterData(const DisjunctiveFilterOperator &op, const Schema &pipeline_schema)
        : res({ Type::Get_Boolean(Type::TY_Vector) })
    {
        auto clause = op.filter()[0];
        for (cnf::Predicate &pred : clause) {
            cnf::Clause clause({ pred });
            cnf::CNF cnf({ clause });
            StackMachine &SM = predicates.emplace_back(pipeline_schema);
            SM.emit(cnf, 1); // compile single predicate
            SM.emit_St_Tup_b(0, 0);
        }
    }
};

}


/*======================================================================================================================
 * Pipeline
 *====================================================================================================================*/

void Pipeline::operator()(const ScanOperator &op)
{
    auto &store = op.store();
    auto &table = store.table();
    const auto num_rows = store.num_rows();

    /* Compile StackMachine to load tuples from store. */
    auto loader = Interpreter::compile_load(op.schema(), store.memory().addr(), table.layout(), table.schema());

    const auto remainder = num_rows % block_.capacity();
    std::size_t i = 0;
    /* Fill entire vector. */
    for (auto end = num_rows - remainder; i != end; i += block_.capacity()) {
        block_.clear();
        block_.fill();
        for (std::size_t j = 0; j != block_.capacity(); ++j) {
            Tuple *args[] = { &block_[j] };
            loader(args);
        }
        op.parent()->accept(*this);
    }
    if (i != num_rows) {
        /* Fill last vector with remaining tuples. */
        block_.clear();
        block_.mask((1UL << remainder) - 1);
        for (std::size_t j = 0; i != op.store().num_rows(); ++i, ++j) {
            M_insist(j < block_.capacity());
            Tuple *args[] = { &block_[j] };
            loader(args);
        }
        op.parent()->accept(*this);
    }
}

void Pipeline::operator()(const CallbackOperator &op)
{
    for (auto &t : block_)
        op.callback()(op.schema(), t);
}

void Pipeline::operator()(const PrintOperator &op)
{
    auto data = as<PrintData>(op.data());
    data->num_rows += block_.size();
    for (auto &t : block_) {
        Tuple *args[] = { &t };
        data->printer(args);
        op.out << '\n';
    }
}

void Pipeline::operator()(const NoOpOperator &op)
{
    as<NoOpData>(op.data())->num_rows += block_.size();
}

void Pipeline::operator()(const FilterOperator &op)
{
    if (not op.data())
        op.data(new FilterData(op, this->schema()));

    auto data = as<FilterData>(op.data());
    for (auto it = block_.begin(); it != block_.end(); ++it) {
        Tuple *args[] = { &data->res, &*it };
        data->filter(args);
        if (data->res.is_null(0) or not data->res[0].as_b()) block_.erase(it);
    }
    if (not block_.empty())
        op.parent()->accept(*this);
}

void Pipeline::operator()(const DisjunctiveFilterOperator &op)
{
    if (not op.data())
        op.data(new DisjunctiveFilterData(op, this->schema()));

    auto data = as<DisjunctiveFilterData>(op.data());
    for (auto it = block_.begin(); it != block_.end(); ++it) {
        data->res.set(0, false); // reset
        Tuple *args[] = { &data->res, &*it };

        for (auto &pred : data->predicates) {
            pred(args);
            if (not data->res.is_null(0) and data->res[0].as_b())
                goto satisfied; // one predicate is satisfied ⇒ entire clause is satisfied
        }
        block_.erase(it); // no predicate was satisfied ⇒ drop tuple
satisfied:;
    }
    if (not block_.empty())
        op.parent()->accept(*this);
}

void Pipeline::operator()(const JoinOperator &op)
{
    if (is<SimpleHashJoinData>(op.data())) {
        /* Perform simple hash join. */
        auto data = as<SimpleHashJoinData>(op.data());
        Tuple *args[2] = { &data->key, nullptr };
        if (data->is_probe_phase) {
            if (data->load_attrs.size() != 2) {
                data->load_probe_key(this->schema());
                data->emit_load_attrs(this->schema());
            }
            auto &pipeline = data->pipeline;
            std::size_t i = 0;
            for (auto &t : block_) {
                args[1] = &t;
                data->probe_key(args);
                pipeline.block_.fill();
                data->ht.for_all(*args[0], [&](std::pair<const Tuple, Tuple> &v) {
                    if (i == pipeline.block_.capacity()) {
                        pipeline.push(*op.parent());
                        i = 0;
                    }

                    {
                        Tuple *load_args[2] = { &pipeline.block_[i], &v.second };
                        data->load_attrs[0](load_args); // load build attrs
                    }
                    {
                        Tuple *load_args[2] = { &pipeline.block_[i], &t };
                        data->load_attrs[1](load_args); // load probe attrs
                    }
                    ++i;
                });
            }

            if (i != 0) {
                M_insist(i <= pipeline.block_.capacity());
                pipeline.block_.mask(i == pipeline.block_.capacity() ? -1UL : (1UL << i) - 1);
                pipeline.push(*op.parent());
            }
        } else {
            if (data->load_attrs.size() != 1) {
                data->load_build_key(this->schema());
                data->emit_load_attrs(this->schema());
            }
            const auto &tuple_schema = op.child(0)->schema();
            for (auto &t : block_) {
                args[1] = &t;
                data->build_key(args);
                data->ht.insert_with_duplicates(args[0]->clone(data->key_schema), t.clone(tuple_schema));
            }
        }
    } else {
        /* Perform nested-loops join. */
        auto data = as<NestedLoopsJoinData>(op.data());
        auto size = op.children().size();
        std::vector<Tuple*> predicate_args(size + 1, nullptr);
        predicate_args[0] = &data->res;

        if (data->active_child == size - 1) {
            /* This is the right-most child.  Combine its produced tuple with all combinations of the buffered
             * tuples. */
            std::vector<std::size_t> positions(size - 1, std::size_t(-1L)); // positions within each buffer
            std::size_t child_id = 0; // cursor to the child that provides the next part of the joined tuple
            auto &pipeline = data->pipeline;

            /* Compile loading data from current child. */
            if (data->buffer_schemas.size() != size) {
                M_insist(data->buffer_schemas.size() == size - 1);
                M_insist(data->load_attrs.size() == size - 1);
                data->emit_load_attrs(this->schema());
                data->buffer_schemas.emplace_back(this->schema()); // save the schema of the current pipeline
                if (op.predicate().size()) {
                    std::vector<std::size_t> tuple_ids(size);
                    std::iota(tuple_ids.begin(), tuple_ids.end(), 1); // start at index 1
                    data->predicate.emit(op.predicate(), data->buffer_schemas, tuple_ids);
                    data->predicate.emit_St_Tup_b(0, 0);
                }
            }

            M_insist(data->buffer_schemas.size() == size);
            M_insist(data->load_attrs.size() == size);

            for (;;) {
                if (child_id == size - 1) { // right-most child, which produced the RHS `block_`
                    /* Combine the tuples.  One tuple from each buffer. */
                    pipeline.clear();
                    pipeline.block_.mask(block_.mask());

                    if (op.predicate().size()) {
                        for (std::size_t cid = 0; cid != child_id; ++cid)
                            predicate_args[cid + 1] = &data->buffers[cid][positions[cid]];
                    }

                    /* Concatenate tuples from the first n-1 children. */
                    for (auto output_it = pipeline.block_.begin(); output_it != pipeline.block_.end(); ++output_it) {
                        auto &rhs = block_[output_it.index()];
                        if (op.predicate().size()) { // do we have a predicate?
                            predicate_args[size] = &rhs;
                            data->predicate(predicate_args.data()); // evaluate predicate
                            if (data->res.is_null(0) or not data->res[0].as_b()) {
                                pipeline.block_.erase(output_it);
                                continue;
                            }
                        }

                        for (std::size_t i = 0; i != child_id; ++i) {
                            auto &buffer = data->buffers[i]; // get buffer of i-th child
                            Tuple *load_args[2] = { &*output_it, &buffer[positions[i]] }; // load child's current tuple
                            data->load_attrs[i](load_args);
                        }

                        {
                            Tuple *load_args[2] = { &*output_it, &rhs }; // load last child's attributes
                            data->load_attrs[child_id](load_args);
                        }
                    }

                    if (not pipeline.block_.empty())
                        pipeline.push(*op.parent());
                    --child_id;
                } else { // child whose tuples have been materialized in a buffer
                    ++positions[child_id];
                    auto &buffer = data->buffers[child_id];
                    if (positions[child_id] == buffer.size()) { // reached the end of this buffer; backtrack
                        if (child_id == 0)
                            break;
                        positions[child_id] = std::size_t(-1L);
                        --child_id;
                    } else {
                        M_insist(positions[child_id] < buffer.size(), "position out of bounds");
                        ++child_id;
                    }
                }
            }
        } else {
            /* This is not the right-most child.  Collect its produced tuples in a buffer. */
            const auto &tuple_schema = op.child(data->active_child)->schema();
            if (data->buffer_schemas.size() <= data->active_child) {
                data->buffer_schemas.emplace_back(this->schema()); // save the schema of the current pipeline
                data->emit_load_attrs(this->schema());
                M_insist(data->buffer_schemas.size() == data->load_attrs.size());
            }
            for (auto &t : block_)
                data->buffers[data->active_child].emplace_back(t.clone(tuple_schema));
        }
    }
}

void Pipeline::operator()(const ProjectionOperator &op)
{
    auto data = as<ProjectionData>(op.data());
    auto &pipeline = data->pipeline;
    if (not data->projections)
        data->emit_projections(this->schema(), op);

    pipeline.clear();
    pipeline.block_.mask(block_.mask());

    for (auto it = block_.begin(); it != block_.end(); ++it) {
        auto &out = pipeline.block_[it.index()];
        Tuple *args[] = { &out, &*it };
        (*data->projections)(args);
    }

    pipeline.push(*op.parent());
}

void Pipeline::operator()(const LimitOperator &op)
{
    auto data = as<LimitData>(op.data());

    for (auto it = block_.begin(); it != block_.end(); ++it) {
        if (data->num_tuples < op.offset() or data->num_tuples >= op.offset() + op.limit())
            block_.erase(it); /* discard this tuple */
        ++data->num_tuples;
    }

    if (not block_.empty())
        op.parent()->accept(*this);

    if (data->num_tuples >= op.offset() + op.limit())
        throw LimitOperator::stack_unwind(); // all tuples produced, now unwind the stack
}

void Pipeline::operator()(const GroupingOperator &op)
{
    auto perform_aggregation = [&](decltype(HashBasedGroupingData::groups)::value_type &entry, Tuple &tuple,
                                   GroupingData &data)
    {
        const std::size_t key_size = op.group_by().size();

        Tuple &group = const_cast<Tuple&>(entry.first);
        const unsigned nth_tuple = ++entry.second;

        /* Add this tuple to its group by computing the aggregates. */
        for (std::size_t i = 0, end = op.aggregates().size(); i != end; ++i) {
            auto &aggregate_arguments = data.args[i];
            Tuple *args[] = { &aggregate_arguments, &tuple };
            data.compute_aggregate_arguments[i](args);

            bool is_null = group.is_null(key_size + i);
            auto &val = group[key_size + i];

            auto &fe = as<const ast::FnApplicationExpr>(op.aggregates()[i].get());
            auto ty = fe.type();
            auto &fn = fe.get_function();

            switch (fn.fnid) {
                default:
                    M_unreachable("function kind not implemented");

                case Function::FN_UDF:
                    M_unreachable("UDFs not yet supported");

                case Function::FN_COUNT:
                    if (is_null)
                        group.set(key_size + i, 0); // initialize
                    if (fe.args.size() == 0) { // COUNT(*)
                        val.as_i() += 1;
                    } else { // COUNT(x) aka. count not NULL
                        val.as_i() += not aggregate_arguments.is_null(0);
                    }
                    break;

                case Function::FN_SUM: {
                    auto n = as<const Numeric>(ty);
                    if (is_null) {
                        if (n->is_floating_point())
                            group.set(key_size + i, 0.); // double precision
                        else
                            group.set(key_size + i, 0); // int
                    }
                    if (aggregate_arguments.is_null(0)) continue; // skip NULL
                    if (n->is_floating_point())
                        val.as_d() += aggregate_arguments[0].as_d();
                    else
                        val.as_i() += aggregate_arguments[0].as_i();
                    break;
                }

                case Function::FN_AVG: {
                    if (is_null) {
                        if (ty->is_floating_point())
                            group.set(key_size + i, 0.); // double precision
                        else
                            group.set(key_size + i, 0); // int
                    }
                    if (aggregate_arguments.is_null(0)) continue; // skip NULL
                    /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming Vol 2,
                     * section 4.2.2. */
                    val.as_d() += (aggregate_arguments[0].as_d() - val.as_d()) / nth_tuple;
                    break;
                }

                case Function::FN_MIN: {
                    using std::min;
                    if (aggregate_arguments.is_null(0)) continue; // skip NULL
                    if (is_null) {
                        group.set(key_size + i, aggregate_arguments[0]);
                        continue;
                    }

                    auto n = as<const Numeric>(ty);
                    if (n->is_float())
                        val.as_f() = min(val.as_f(), aggregate_arguments[0].as_f());
                    else if (n->is_double())
                        val.as_d() = min(val.as_d(), aggregate_arguments[0].as_d());
                    else
                        val.as_i() = min(val.as_i(), aggregate_arguments[0].as_i());
                    break;
                }

                case Function::FN_MAX: {
                    using std::max;
                    if (aggregate_arguments.is_null(0)) continue; // skip NULL
                    if (is_null) {
                        group.set(key_size + i, aggregate_arguments[0]);
                        continue;
                    }

                    auto n = as<const Numeric>(ty);
                    if (n->is_float())
                        val.as_f() = max(val.as_f(), aggregate_arguments[0].as_f());
                    else if (n->is_double())
                        val.as_d() = max(val.as_d(), aggregate_arguments[0].as_d());
                    else
                        val.as_i() = max(val.as_i(), aggregate_arguments[0].as_i());
                    break;
                }
            }
        }
    };

    /* Find the group. */
    auto data = as<HashBasedGroupingData>(op.data());
    auto &groups = data->groups;

    Tuple key(op.schema());
    for (auto &tuple : block_) {
        Tuple *args[] = { &key, &tuple };
        data->compute_key(args);
        auto it = groups.find(key);
        if (it == groups.end()) {
            /* Initialize the group's aggregate to NULL.  This will be overwritten by the neutral element w.r.t.
             * the aggregation function. */
            it = groups.emplace_hint(it, std::move(key), 0);
            key = Tuple(op.schema());
        }
        perform_aggregation(*it, tuple, *data);
    }
}

void Pipeline::operator()(const AggregationOperator &op)
{
    auto data = as<AggregationData>(op.data());
    auto &nth_tuple = data->aggregates[op.schema().num_entries()].as_i();

    for (auto &tuple : block_) {
        nth_tuple += 1UL;
        for (std::size_t i = 0, end = op.aggregates().size(); i != end; ++i) {
            auto &aggregate_arguments = data->args[i];
            Tuple *args[] = { &aggregate_arguments, &tuple };
            data->compute_aggregate_arguments[i](args);

            auto &fe = as<const ast::FnApplicationExpr>(op.aggregates()[i].get());
            auto ty = fe.type();
            auto &fn = fe.get_function();

            bool agg_is_null = data->aggregates.is_null(i);
            auto &val = data->aggregates[i];

            switch (fn.fnid) {
                default:
                    M_unreachable("function kind not implemented");

                case Function::FN_UDF:
                    M_unreachable("UDFs not yet supported");

                case Function::FN_COUNT:
                    if (fe.args.size() == 0) { // COUNT(*)
                        val.as_i() += 1;
                    } else { // COUNT(x) aka. count not NULL
                        val.as_i() += not aggregate_arguments.is_null(0);
                    }
                    break;

                case Function::FN_SUM: {
                    auto n = as<const Numeric>(ty);
                    if (aggregate_arguments.is_null(0)) continue; // skip NULL
                    if (n->is_floating_point())
                        val.as_d() += aggregate_arguments[0].as_d();
                    else
                        val.as_i() += aggregate_arguments[0].as_i();
                    break;
                }

                case Function::FN_AVG: {
                    if (aggregate_arguments.is_null(0)) continue; // skip NULL
                    /* Compute AVG as iterative mean as described in Knuth, The Art of Computer Programming Vol 2,
                     * section 4.2.2. */
                    val.as_d() += (aggregate_arguments[0].as_d() - val.as_d()) / nth_tuple;
                    break;
                }

                case Function::FN_MIN: {
                    using std::min;
                    if (aggregate_arguments.is_null(0)) continue; // skip NULL
                    if (agg_is_null) {
                        data->aggregates.set(i, aggregate_arguments[0]);
                        continue;
                    }

                    auto n = as<const Numeric>(ty);
                    if (n->is_float())
                        val.as_f() = min(val.as_f(), aggregate_arguments[0].as_f());
                    else if (n->is_double())
                        val.as_d() = min(val.as_d(), aggregate_arguments[0].as_d());
                    else
                        val.as_i() = min(val.as_i(), aggregate_arguments[0].as_i());
                    break;
                }

                case Function::FN_MAX: {
                    using std::max;
                    if (aggregate_arguments.is_null(0)) continue; // skip NULL
                    if (agg_is_null) {
                        data->aggregates.set(i, aggregate_arguments[0]);
                        continue;
                    }

                    auto n = as<const Numeric>(ty);
                    if (n->is_float())
                        val.as_f() = max(val.as_f(), aggregate_arguments[0].as_f());
                    else if (n->is_double())
                        val.as_d() = max(val.as_d(), aggregate_arguments[0].as_d());
                    else
                        val.as_i() = max(val.as_i(), aggregate_arguments[0].as_i());
                    break;
                }
            }
        }
    }
}

void Pipeline::operator()(const SortingOperator &op)
{
    if (not op.data())
        op.data(new SortingData(this->schema()));

    /* cache all tuples for sorting */
    auto data = as<SortingData>(op.data());
    for (auto &t : block_)
        data->buffer.emplace_back(t.clone(this->schema()));
}

/*======================================================================================================================
 * Interpreter - Recursive descent
 *====================================================================================================================*/

void Interpreter::operator()(const CallbackOperator &op)
{
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const PrintOperator &op)
{
    op.data(new PrintData(op));
    op.child(0)->accept(*this);
    if (not Options::Get().quiet)
        op.out << as<PrintData>(op.data())->num_rows << " rows\n";
}

void Interpreter::operator()(const NoOpOperator &op)
{
    op.data(new NoOpData());
    op.child(0)->accept(*this);
    op.out << as<NoOpData>(op.data())->num_rows << " rows\n";
}

void Interpreter::operator()(const ScanOperator &op)
{
    Pipeline pipeline(op.schema());
    pipeline.push(op);
}

void Interpreter::operator()(const FilterOperator &op)
{
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const DisjunctiveFilterOperator &op)
{
    op.child(0)->accept(*this);
}

void Interpreter::operator()(const JoinOperator &op)
{
    if (op.predicate().is_equi()) {
        /* Perform simple hash join. */
        auto data = new SimpleHashJoinData(op);
        op.data(data);
        if (op.has_info())
            data->ht.resize(op.info().estimated_cardinality);
        op.child(0)->accept(*this); // build HT on LHS
        if (data->ht.size() == 0) // no tuples produced
            return;
        data->is_probe_phase = true;
        op.child(1)->accept(*this); // probe HT with RHS
    } else {
        /* Perform nested-loops join. */
        auto data = new NestedLoopsJoinData(op);
        op.data(data);
        for (std::size_t i = 0, end = op.children().size(); i != end; ++i) {
            data->active_child = i;
            auto c = op.child(i);
            c->accept(*this);
            if (i != op.children().size() - 1 and data->buffers[i].empty()) // no tuples produced
                return;
        }
    }
}

void Interpreter::operator()(const ProjectionOperator &op)
{
    bool has_child = op.children().size();
    auto data = new ProjectionData(op);
    op.data(data);

    /* Evaluate the projection. */
    if (has_child)
        op.child(0)->accept(*this);
    else {
        Pipeline pipeline;
        pipeline.block_.mask(1); // evaluate the projection EXACTLY ONCE on an empty tuple
        pipeline.push(op);
    }
}

void Interpreter::operator()(const LimitOperator &op)
{
    try {
        op.data(new LimitData());
        op.child(0)->accept(*this);
    } catch (LimitOperator::stack_unwind) {
        /* OK, we produced all tuples and unwinded the stack */
    }
}

void Interpreter::operator()(const GroupingOperator &op)
{
    auto &parent = *op.parent();
    auto data = new HashBasedGroupingData(op);
    op.data(data);

    op.child(0)->accept(*this);

    const auto num_groups = data->groups.size();
    const auto remainder = num_groups % data->pipeline.block_.capacity();
    auto it = data->groups.begin();
    for (std::size_t i = 0; i != num_groups - remainder; i += data->pipeline.block_.capacity()) {
        data->pipeline.block_.clear();
        data->pipeline.block_.fill();
        for (std::size_t j = 0; j != data->pipeline.block_.capacity(); ++j) {
            auto node = data->groups.extract(it++);
            swap(data->pipeline.block_[j], node.key());
        }
        data->pipeline.push(parent);
    }
    data->pipeline.block_.clear();
    data->pipeline.block_.mask((1UL << remainder) - 1UL);
    for (std::size_t i = 0; i != remainder; ++i) {
        auto node = data->groups.extract(it++);
        swap(data->pipeline.block_[i], node.key());
    }
    data->pipeline.push(parent);
}

void Interpreter::operator()(const AggregationOperator &op)
{
    op.data(new AggregationData(op));
    auto data = as<AggregationData>(op.data());

    /* Initialize aggregates. */
    for (std::size_t i = 0, end = op.aggregates().size(); i != end; ++i) {
        auto &fe = as<const ast::FnApplicationExpr>(op.aggregates()[i].get());
        auto ty = fe.type();
        auto &fn = fe.get_function();

        switch (fn.fnid) {
            default:
                M_unreachable("function kind not implemented");

            case Function::FN_UDF:
                M_unreachable("UDFs not yet supported");

            case Function::FN_COUNT:
                data->aggregates.set(i, 0); // initialize
                break;

            case Function::FN_SUM: {
                auto n = as<const Numeric>(ty);
                if (n->is_floating_point())
                    data->aggregates.set(i, 0.); // double precision
                else
                    data->aggregates.set(i, 0L); // int64
                break;
            }

            case Function::FN_AVG: {
                if (ty->is_floating_point())
                    data->aggregates.set(i, 0.); // double precision
                else
                    data->aggregates.set(i, 0L); // int64
                break;
            }

            case Function::FN_MIN:
            case Function::FN_MAX: {
                data->aggregates.null(i); // initialize to NULL
                break;
            }
        }
    }
    op.child(0)->accept(*this);

    using std::swap;
    data->pipeline.block_.clear();
    data->pipeline.block_.mask(1UL);
    swap(data->pipeline.block_[0], data->aggregates);
    data->pipeline.push(*op.parent());
}

void Interpreter::operator()(const SortingOperator &op)
{
    op.child(0)->accept(*this);

    auto data = as<SortingData>(op.data());
    if (not data) // no tuples produced
        return;

    const auto &orderings = op.order_by();

    StackMachine comparator(data->pipeline.schema());
    for (auto o : orderings) {
        comparator.emit(o.first.get(), 1); // LHS
        comparator.emit(o.first.get(), 2); // RHS

        /* Emit comparison. */
        auto ty = o.first.get().type();
        visit(overloaded {
            [&comparator](const Boolean&) { comparator.emit_Cmp_b(); },
            [&comparator](const CharacterSequence&) { comparator.emit_Cmp_s(); },
            [&comparator](const Numeric &n) {
                switch (n.kind) {
                    case Numeric::N_Int:
                    case Numeric::N_Decimal:
                        comparator.emit_Cmp_i();
                        break;

                    case Numeric::N_Float:
                        if (n.size() <= 32)
                            comparator.emit_Cmp_f();
                        else
                            comparator.emit_Cmp_d();
                        break;
                }
            },
            [&comparator](const Date&) { comparator.emit_Cmp_i(); },
            [&comparator](const DateTime&) { comparator.emit_Cmp_i(); },
            [](auto&&) { M_insist("invalid type"); }
        }, *ty);

        if (not o.second)
            comparator.emit_Minus_i(); // sort descending
        comparator.emit_St_Tup_i(0, 0);
        comparator.emit_Stop_NZ();
    }

    Tuple res({ Type::Get_Integer(Type::TY_Vector, 4) });
    std::sort(data->buffer.begin(), data->buffer.end(), [&](Tuple &first, Tuple &second) {
        Tuple *args[] = { &res, &first, &second };
        comparator(args);
        M_insist(not res.is_null(0));
        return res[0].as_i() < 0;
    });

    auto &parent = *op.parent();
    const auto num_tuples = data->buffer.size();
    const auto remainder = num_tuples % data->pipeline.block_.capacity();
    auto it = data->buffer.begin();
    for (std::size_t i = 0; i != num_tuples - remainder; i += data->pipeline.block_.capacity()) {
        data->pipeline.block_.clear();
        data->pipeline.block_.fill();
        for (std::size_t j = 0; j != data->pipeline.block_.capacity(); ++j)
            data->pipeline.block_[j] = std::move(*it++);
        data->pipeline.push(parent);
    }
    data->pipeline.block_.clear();
    data->pipeline.block_.mask((1UL << remainder) - 1UL);
    for (std::size_t i = 0; i != remainder; ++i)
        data->pipeline.block_[i] = std::move(*it++);
    data->pipeline.push(parent);
}

__attribute__((constructor(202)))
static void register_interpreter()
{
    Catalog &C = Catalog::Get();
    C.register_backend<Interpreter>("Interpreter", "tuple-at-a-time Interpreter built with virtual stack machines");
}
