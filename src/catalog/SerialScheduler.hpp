#pragma once

#include <mutable/catalog/Scheduler.hpp>
#include <condition_variable>
#include <list>
#include <future>
#include <thread>


namespace m {

/** This class implements a Scheduler that executes all incoming `DatabaseCommand`s serially.
 * This means that there is no concurrent execution.
 * Therefore, a consistent, serial execution of multiple queries is guaranteed. */
struct SerialScheduler : Scheduler
{
    private:
    /** A thread-safe query plan queue. */
    struct CommandQueue
    {
        private:
        std::list<queued_command> command_list_;
        Transaction *running_transaction_; ///< the currently running transaction. Only commands by this transaction are returned.
        std::mutex mutex_;
        std::condition_variable has_element_;
        bool closed_ = false;

        public:
        CommandQueue() = default;
        ~CommandQueue() = default;

        ///> returns the next queued `ast::Command`. Returns `std::nullopt` if the queue is closed.
        std::optional<queued_command> pop();
        /** Inserts the command into the queue. Internally the position in the `command_list_` is determined by the
         * start time of `Transaction t`. A smaller start time is inserted before a larger start time and elements
         * with the same start time are ordered in FIFO order by the time of arrival. */
        void push(Transaction &t, std::unique_ptr<ast::Command> command, Diagnostic &diag, std::promise<bool> promise);
        void close();    ///< empties and closes the queue without executing the remaining `ast::Command`s.
        bool is_closed();  ///< signals waiting threads that no more elements will be pushed
        void stop_transaction(Transaction &t); ///< Marks `t` as no longer running.
    };

    static CommandQueue query_queue_; ///< instance of our thread-safe query queue that stores all incoming plans.
    std::thread schedule_thread_; ///< the worker thread that executes all incoming queries.

    static std::atomic<int64_t> next_start_time; ///< stores the next transaction start time

    public:
    SerialScheduler() = default;
    ~SerialScheduler();

    std::future<bool> schedule_command(Transaction &t, std::unique_ptr<ast::Command> command, Diagnostic &diag) override;

    std::unique_ptr<Transaction> begin_transaction() override;

    bool commit(std::unique_ptr<Transaction> t) override;

    bool abort(std::unique_ptr<Transaction> t) override;

    private:
    /** The method run by the worker thread `schedule_thread_`.
     * While stopping, the query that is already being executed will complete its execution
     * but queued queries will not be executed. */
    static void schedule_thread();
};

}
