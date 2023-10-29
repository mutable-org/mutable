#include "catalog/SerialScheduler.hpp"
#include "parse/Sema.hpp"
#include <mutable/mutable.hpp>


using namespace m;


std::optional<m::Scheduler::queued_command> SerialScheduler::CommandQueue::pop()
{
    std::unique_lock<std::mutex> lock(mutex_);
    not_empty_.wait(lock, []{
        // wait until either the queue is no longer empty or the queue is in closed
        return not query_queue_.command_queue_.empty() or query_queue_.closed_;
    });
    // if the queue is still empty here the queue should be closed.
    if (command_queue_.empty()) return std::nullopt;

    queued_command res = std::move(command_queue_.front());
    command_queue_.pop();
    return {std::move(res)};
}

void SerialScheduler::CommandQueue::push(std::unique_ptr<ast::Command> command, Diagnostic &diag, std::promise<bool> promise)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) {
        promise.set_value(false);
        return;
    }
    command_queue_.emplace(std::move(command), diag, std::move(promise));
    lock.unlock();
    not_empty_.notify_one();
}

void SerialScheduler::CommandQueue::close()
{
    std::unique_lock<std::mutex> lock(mutex_);
    closed_ = true;
    while (not command_queue_.empty()) {
        std::get<2>(command_queue_.front()).set_value(false);
        command_queue_.pop();
    }
    lock.unlock();
    not_empty_.notify_all();
}

bool SerialScheduler::CommandQueue::is_closed()
{
    std::lock_guard<std::mutex> lock(mutex_);
    return query_queue_.closed_;
}

SerialScheduler::CommandQueue SerialScheduler::query_queue_;

SerialScheduler::~SerialScheduler()
{
    if (schedule_thread_.joinable()) {
        query_queue_.close();
        schedule_thread_.join();
    }
}

bool SerialScheduler::schedule_command(std::unique_ptr<ast::Command> command, Diagnostic &diag)
{
    std::promise<bool> execution_completed;
    auto execution_completed_future = execution_completed.get_future();
    query_queue_.push(std::move(command), diag, std::move(execution_completed));

    if (not schedule_thread_.joinable())
        // Creating the worker thread not here but in the constructor of `SerialScheduler` causes deadlocks.
        schedule_thread_ = std::thread(schedule_thread);

    // We wait here right away since we want a serial execution of the commands.
    execution_completed_future.wait();
    return execution_completed_future.get();
}

void SerialScheduler::schedule_thread()
{
    Catalog &C = Catalog::Get();
    while (not query_queue_.is_closed()) {
        auto ret = query_queue_.pop();
        // pop() should only return no value if the queue is closed
        if (not ret.has_value()) continue;

        auto [ast, diag, promise] = std::move(ret.value());
        ast::Sema sema(diag);
        bool err = diag.num_errors() > 0; // parser errors

        diag.clear();
        auto cmd = sema.analyze(std::move(ast));
        err |= diag.num_errors() > 0; // sema errors

        M_insist(not err == bool(cmd), "when there are no errors, Sema must have returned a command");
        if (not err and cmd) {
            cmd->execute(diag);
            promise.set_value(true);
            continue;
        }
        promise.set_value(false);
    }
}

__attribute__((constructor(202)))
static void register_scheduler()
{
    Catalog &C = Catalog::Get();
    C.register_scheduler(
        "SerialScheduler",
        std::make_unique<SerialScheduler>(),
        "executes all incoming queries serially"
    );
}
