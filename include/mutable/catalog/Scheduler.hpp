#pragma once

#include <mutable/mutable-config.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <compare>
#include <future>


namespace m {

/** The Scheduler handles the execution of all incoming queries. The implementation stored in the catalog determines
 * when and how queries are executed. */
struct M_EXPORT Scheduler
{
    struct Transaction {
        private:
        ///> the Transaction ID
        uint64_t id_;
        ///> the start time of the transaction. Used for multi-versioning and should be set when the transaction executes something.
        int64_t start_time_ = -1;

        ///> Stores the next available Transaction ID, stored atomically to prevent race conditions
        static std::atomic<uint64_t> next_id_;

        public:
        Transaction() : id_(next_id_.fetch_add(1, std::memory_order_relaxed)) { }

        ///> sets the start time of the Transaction. Should only be set once and only to a positive number.
        void start_time(int64_t time) { M_insist(start_time_ == -1 and time >= 0); start_time_ = time; };
        int64_t start_time() const { return start_time_; };

        auto operator==(const Transaction &other) const { return id_ == other.id_; };
        auto operator<=>(const Transaction &other) const { return id_ <=> other.id_; };
    };

    protected:
    using queued_command = std::tuple<Transaction &, std::unique_ptr<ast::Command>, Diagnostic &, std::promise<bool>>;

    public:
    Scheduler() = default;
    virtual ~Scheduler() {}

    /** Schedule a `ast::Command` for execution within the given `Scheduler::Transaction`.
     * Returns a `std:future<bool>` that is set to true if the `ast::Command` was successfully executed, false otherwise. */
    virtual std::future<bool> schedule_command(Transaction &t, std::unique_ptr<ast::Command> command, Diagnostic &diag) = 0;

    /** Returns a new `Scheduler::Transaction` object that is passed along when scheduling commands. */
    virtual std::unique_ptr<Transaction> begin_transaction() = 0;

    /** Closes the given `Scheduler::Transaction` and commits its changes.
     * Returns true if the changes were committed successfully. */
    virtual bool commit(std::unique_ptr<Transaction> t) = 0;

    /** Closes the given `Scheduler::Transaction` and discards its changes.
     * Returns true if the changes were undone successfully. */
    virtual bool abort(std::unique_ptr<Transaction> t) = 0;

    /** Schedule a `ast::Command` for execution and automatically commits its changes.
     * Returns true if the `ast::Command` was executed and its changes were committed successfully. */
    bool autocommit(std::unique_ptr<ast::Command> command, Diagnostic &diag);
};

}
