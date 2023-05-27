#include <mutable/storage/DataLayout.hpp>

#include <mutable/catalog/Type.hpp>


using namespace m;
using namespace m::storage;


/*----------------------------------------------------------------------------------------------------------------------
 * DataLayout::Node
 *--------------------------------------------------------------------------------------------------------------------*/

DataLayout::Node::~Node() { }


/*----------------------------------------------------------------------------------------------------------------------
 * DataLayout::Leaf
 *--------------------------------------------------------------------------------------------------------------------*/

void DataLayout::Leaf::accept(ConstDataLayoutVisitor &v) const { v(*this); };


/*----------------------------------------------------------------------------------------------------------------------
 * DataLayout::INode
 *--------------------------------------------------------------------------------------------------------------------*/

DataLayout::Leaf & DataLayout::INode::add_leaf(const m::Type *type, size_type idx,
                                               uint64_t offset_in_bits, uint64_t stride_in_bits)
{
    M_insist(this->num_tuples() != 1 or stride_in_bits == 0, "no stride without repetition");
    M_insist(stride_in_bits % 8 == 0 or type->is_boolean() or type->is_bitmap(),
             "only booleans and bitmaps may not be byte aligned");
    M_insist(offset_in_bits % 8 == 0 or type->is_boolean() or type->is_bitmap(),
             "only booleans and bitmaps may not be byte aligned");

    auto leaf = new Leaf(type, idx);
    children_.emplace_back(child_t{
        .ptr = std::unique_ptr<DataLayout::Node>(as<Node>(leaf)),
        .offset_in_bits = offset_in_bits,
        .stride_in_bits = stride_in_bits,
    });
    return *leaf;
}

DataLayout::INode & DataLayout::INode::add_inode(size_type num_tuples, uint64_t offset_in_bits, uint64_t stride_in_bits)
{
    M_insist(num_tuples != 0, "the new INode must be large enough for at least one tuple");
    M_insist(this->num_tuples() != 1 or stride_in_bits == 0, "no stride without repetition");
    M_insist(this->num_tuples() % num_tuples == 0,
             "the number of tuples in the parent must be a whole multiple of the number of tuples of the newly created "
             "INode");
    M_insist(offset_in_bits % 8 == 0, "the offset of the newly created INode must be byte aligned");
    M_insist(stride_in_bits % 8 == 0, "the stride of the newly created INode must be byte aligned");

    auto inode = new INode(num_tuples);
    children_.emplace_back(child_t{
        .ptr = std::unique_ptr<DataLayout::Node>(as<Node>(inode)),
        .offset_in_bits = offset_in_bits,
        .stride_in_bits = stride_in_bits,
    });
    return *inode;
}

void DataLayout::INode::accept(ConstDataLayoutVisitor &v) const { v(*this); }

void DataLayout::INode::for_sibling_leaves(level_info_stack_t &level_info_stack, uint64_t inode_offset_in_bits,
                                           const callback_leaves_t &callback) const
{
    std::vector<leaf_info_t> leaves;
    leaves.reserve(this->num_children());

    for (auto &child : *this) {
        if (auto child_leaf = cast<const Leaf>(child.ptr.get())) {
            leaves.emplace_back(leaf_info_t{
                .leaf = *child_leaf,
                .offset_in_bits = child.offset_in_bits,
                .stride_in_bits = child.stride_in_bits,
            });
        } else {
            auto child_inode = as<const INode>(child.ptr.get());
            level_info_stack.emplace_back(level_info_t{
                .stride_in_bits = child.stride_in_bits,
                .num_tuples = child_inode->num_tuples(),
            });
            child_inode->for_sibling_leaves(level_info_stack, inode_offset_in_bits + child.offset_in_bits, callback);
            level_info_stack.pop_back();
        }
    }

    if (not leaves.empty())
        callback(leaves, level_info_stack, inode_offset_in_bits);
}

M_LCOV_EXCL_START
namespace {

/** Start a new line with proper indentation. */
std::ostream & indent(std::ostream &out, unsigned indentation)
{
    out << '\n' << std::string(4 * indentation, ' ');
    return out;
}

}

void DataLayout::INode::print(std::ostream &out, unsigned int indentation) const
{
    const std::size_t decimal_places = std::ceil(num_children() / Numeric::DECIMAL_TO_BINARY_DIGITS);

    auto it = cbegin();
    for (std::size_t i = 0; i != num_children(); ++i, ++it) {
        M_insist(it != cend());

        indent(out, indentation) << "[" << std::setw(decimal_places) << i << "]: ";

        auto &child = *it;
        if (auto child_leaf = cast<const Leaf>(child.ptr.get())) {
            out << "Leaf " << child_leaf->index() << " of type " << *child_leaf->type() << " with bit offset "
                << child.offset_in_bits << " and bit stride " << child.stride_in_bits;
        } else {
            auto child_inode = as<const INode>(child.ptr.get());
            out << "INode of " << child_inode->num_tuples() << " tuple(s) with bit offset " << child.offset_in_bits
                << " and bit stride " << child.stride_in_bits;
            child_inode->print(out, indentation + 1);
        }
    }
}
M_LCOV_EXCL_STOP


/*----------------------------------------------------------------------------------------------------------------------
 * DataLayout
 *--------------------------------------------------------------------------------------------------------------------*/

DataLayout::Leaf & DataLayout::add_leaf(const m::Type *type, size_type idx, uint64_t stride_in_bits)
{
    M_insist(inode_.num_children() == 0, "child already set");
    return inode_.add_leaf(type, idx, 0, stride_in_bits);
}

DataLayout::INode & DataLayout::add_inode(size_type num_tuples, uint64_t stride_in_bits)
{
    M_insist(inode_.num_children() == 0, "child already set");
    M_insist(num_tuples != 0, "the new INode must be large enough for at least one tuple");
    M_insist(inode_.num_tuples() != 1 or stride_in_bits == 0, "no stride without repetition");
    M_insist(stride_in_bits % 8 == 0, "the stride of the newly created INode must be byte aligned");

    auto inode = new INode(num_tuples);
    inode_.children_.emplace_back(INode::child_t{
        .ptr = std::unique_ptr<DataLayout::Node>(as<Node>(inode)),
        .offset_in_bits = 0,
        .stride_in_bits = stride_in_bits,
    });
    return *inode;
}

void DataLayout::accept(ConstDataLayoutVisitor &v) const { v(*this); }

void DataLayout::for_sibling_leaves(DataLayout::callback_leaves_t callback) const
{
    level_info_stack_t level_info_stack;
    inode_.for_sibling_leaves(level_info_stack, 0, callback);
}
