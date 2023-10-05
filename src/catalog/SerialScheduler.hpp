#pragma once

#include <mutable/catalog/Scheduler.hpp>
#include <condition_variable>
#include <future>
#include <queue>
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
        std::queue<queued_command> command_queue_;
        std::mutex mutex_;
        std::condition_variable not_empty_;
        bool closed_ = false;

        public:
        CommandQueue() = default;
        ~CommandQueue() = default;

        std::optional<queued_command> pop();   ///< returns the next queued `DatabaseCommand`. If the queue is in pop-only mode this returns `std::nullopt`.
        void push(std::unique_ptr<DatabaseCommand> command, Diagnostic &diag, std::promise<bool> promise);
        void close();    ///< empties and closes the queue without executing the remaining `DatabaseCommand`s.
        bool is_closed();  ///< signals waiting threads that no more elements will be pushed
    };

    static CommandQueue query_queue_; ///< instance of our thread-safe query queue that stores all incoming plans.
    std::thread schedule_thread_; ///< the worker thread that executes all incoming queries.

    public:
    SerialScheduler() = default;
    ~SerialScheduler();

    bool schedule_command(std::unique_ptr<DatabaseCommand> command, Diagnostic &diag) override;

    private:
    /** The method run by the worker thread `schedule_thread_`.
     * While stopping, the query that is already being executed will complete its execution
     * but queued queries will not be executed. */
    static void schedule_thread();
};

}