#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutable/util/exception.hpp>
#include <mutable/util/macro.hpp>
#include <mutable/util/Visitor.hpp>
#include <vector>


namespace m {

struct Type;

namespace storage {

struct ConstDataLayoutVisitor;

struct DataLayout
{
    /*----- Types ----------------------------------------------------------------------------------------------------*/
    using size_type = std::size_t;

    struct Leaf;
    struct INode;

    ///> combines information of a single leaf for `for_sibling_leaves()`
    struct leaf_info_t
    {
        const Leaf &leaf;
        uint64_t offset_in_bits;
        uint64_t stride_in_bits;
    };

    ///> combines information of a single internal level inside the `DataLayout`, used by `for_sibling_leaves()`
    struct level_info_t
    {
        ///> the stride of instances of this level
        uint64_t stride_in_bits;
        ///> number of tuples that fit into an instance of this level
        size_type num_tuples;
    };

    using level_info_stack_t = std::vector<level_info_t>;

    using callback_leaves_t = std::function<void(const std::vector<leaf_info_t> &leaves,
                                                 const level_info_stack_t &levels,
                                                 uint64_t inode_offset_in_bits)>;

    struct Node
    {
        virtual ~Node() = 0;

        virtual size_type num_tuples() const = 0;

        virtual void accept(ConstDataLayoutVisitor &v) const = 0;
    };

    struct Leaf : Node
    {
        friend struct DataLayout;
        friend struct INode;

        private:
        const m::Type *type_;
        size_type idx_;

        Leaf(const m::Type *type, size_type idx) : type_(type), idx_(idx) { }

        public:
        /** Returns the `Type` of this `Leaf`. */
        const m::Type * type() const { return type_; }
        /** Returns the index assigned to this `Leaf`.  Must be unique within the entire `DataLayout`. */
        size_type index() const { return idx_; }

        size_type num_tuples() const override { return 1; }

        void accept(ConstDataLayoutVisitor &v) const override;
    };

    struct INode : Node
    {
        friend struct DataLayout;

        struct child_t
        {
            std::unique_ptr<Node> ptr;
            uint64_t offset_in_bits;
            uint64_t stride_in_bits;
        };

        struct const_iterator
        {
            private:
            const INode &the_inode_;
            size_type idx_;

            public:
            const_iterator(const INode &the_inode, size_type idx = 0)
                : the_inode_(the_inode)
                , idx_(idx)
            { }

            bool operator==(const_iterator other) const {
                M_insist(&this->the_inode_ == &other.the_inode_, "comparing iterators to different INodes");
                return this->idx_ == other.idx_;
            }
            bool operator!=(const_iterator other) const { return not operator==(other); }

            const_iterator & operator++() {
                M_insist(idx_ < the_inode_.num_children(), "index out of bounds");
                ++idx_;
                return *this;
            }

            const_iterator operator++(int) { const_iterator old = *this; this->operator++(); return old; }

            const child_t & operator*() { return the_inode_[idx_]; }
            const child_t & operator->() { return the_inode_[idx_]; }
        };

        private:
        std::vector<child_t> children_;
        ///> the number of tuples that fit into one instance of this `INode`
        size_type num_tuples_;

        INode(size_type num_tuples) : num_tuples_(num_tuples) { }

        INode(INode&&) = default;

        INode & operator=(INode&&) = default;

        public:
        size_type num_tuples() const override { return num_tuples_; }
        size_type num_children() const { return children_.size(); }

        Leaf & add_leaf(const m::Type *type, size_type idx, uint64_t offset_in_bits, uint64_t stride_in_bits);
        INode & add_inode(size_type num_tuples, uint64_t offset_in_bits, uint64_t stride_in_bits);

        const child_t & operator[](size_type idx) const {
            M_insist(idx < children_.size(), "index out of bounds");
            return children_[idx];
        }
        const child_t & at(size_type idx) const {
            if (idx >= children_.size()) throw m::invalid_argument("index out of bounds");
            return children_[idx];
        }

        const_iterator begin() const { return const_iterator(*this); }
        const_iterator end() const { return const_iterator(*this, this->num_children()); }
        const_iterator cbegin() const { return begin(); }
        const_iterator cend() const { return end(); }

        void accept(ConstDataLayoutVisitor &v) const override;

        private:
        void for_sibling_leaves(level_info_stack_t &level_info_stack, uint64_t inode_offset_in_bits,
                                const callback_leaves_t &callback) const;

        void print(std::ostream &out, unsigned indentation = 0) const;
    };

    /*----- Fields ---------------------------------------------------------------------------------------------------*/
    private:
    ///> use an `INode` to store a single child, allowing us to exploit `INode` abstractions within `DataLayout`
    INode inode_;

    /*----- Methods --------------------------------------------------------------------------------------------------*/
    public:
    DataLayout(size_type num_tuples = 0) : inode_(num_tuples) { }

    bool is_finite() const { return inode_.num_tuples() != 0; }
    size_type num_tuples() const {
        M_insist(is_finite(), "infinite data layouts have no number of tuples");
        return inode_.num_tuples();
    }
    uint64_t stride_in_bits() const { M_insist(bool(*this), "no child set"); return inode_[0].stride_in_bits; }
    const Node & child() const { M_insist(bool(*this), "no child set"); return *inode_[0].ptr; }

    operator bool() const { return inode_.num_children() == 1; }
    explicit operator const INode&() const { return inode_; }

    Leaf & add_leaf(const m::Type *type, size_type idx, uint64_t stride_in_bits);
    INode & add_inode(size_type num_tuples, uint64_t stride_in_bits);

    void accept(ConstDataLayoutVisitor &v) const;
    void for_sibling_leaves(callback_leaves_t callback) const;

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const DataLayout &layout) {
        if (layout.is_finite())
            out << "DataLayout of " << layout.num_tuples() << " tuple(s)";
        else
            out << "DataLayout of infinite tuples";
        layout.inode_.print(out);
        return out;
    };

    void dump() const { dump(std::cerr); }
    void dump(std::ostream &out) const { out << *this << std::endl; }
M_LCOV_EXCL_STOP
};

#define DATA_LAYOUT_CLASSES(X) \
    X(DataLayout::INode) \
    X(DataLayout::Leaf) \
    X(DataLayout)

M_DECLARE_VISITOR(ConstDataLayoutVisitor, const DataLayout::Node, DATA_LAYOUT_CLASSES)

}

}
