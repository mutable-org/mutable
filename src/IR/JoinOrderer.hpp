#pragma once

#include "IR/JoinGraph.hpp"
#include "util/macro.hpp"
#include <cstdint>
#include <unordered_map>
#include <vector>


namespace db {

/** An interface for all join orderers. */
struct JoinOrderer
{
    struct entry_type
    {
        private:
        static constexpr intptr_t MASK = 0x1; ///< marks a pointer to a join
        intptr_t ptr_; ///< pointer to either a data source or a join

        public:
        entry_type(const DataSource *DS) : ptr_(reinterpret_cast<intptr_t>(DS)) { }
        entry_type(const Join *J) : ptr_(reinterpret_cast<intptr_t>(J) | MASK) { }

        bool is_join() const { return bool(ptr_ & MASK); }

        const DataSource * as_datasource() const {
            insist((ptr_ & MASK) == 0, "expected a datasource but got a join");
            return reinterpret_cast<const DataSource*>(ptr_);
        }
        const Join * as_join() const {
            insist((ptr_ & MASK) == 1, "expected a join but got a data source");
            return reinterpret_cast<const Join*>(ptr_ & ~MASK);
        }

        friend std::ostream & operator<<(std::ostream &out, const entry_type &e) {
            if (e.is_join()) {
                out << "⋈";
            } else {
                auto ds = e.as_datasource();
                out << ds->alias();
            }
            return out;
        }
    };

    /** Represents the order in which relations are to be joined in reverse polish notation (RPN).  The join order in
     * RPN resembles a post-order walk of the corresponding operator tree, enabling easy construction of the operator
     * tree. */
    using order_type = std::vector<entry_type>;

    /** Assigns a join order to each join graph instance (including the nested join graphs of subqueries). */
    using mapping_type = std::unordered_map<const JoinGraph*, order_type>;

    virtual ~JoinOrderer() { }

    /** Compute a join order for the given join graph. */
    virtual mapping_type operator()(const JoinGraph &G) const = 0;
};

inline std::ostream & operator<<(std::ostream &out, const JoinOrderer::order_type &order) {
    for (auto begin = order.begin(), end = order.end(), it = begin; it != end; ++it) {
        if (it != begin) out << ' ';
        out << *it;
    }
    return out;
}

/** Computes an arbitrary join order (deterministically). */
struct DummyJoinOrderer : JoinOrderer
{
    mapping_type operator()(const JoinGraph &G) const override;
};

}
