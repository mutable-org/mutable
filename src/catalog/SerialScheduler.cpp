#include "catalog/SerialScheduler.hpp"
#include "parse/Sema.hpp"
#include <mutable/mutable.hpp>


using namespace m;


std::optional<m::Scheduler::queued_command> SerialScheduler::CommandQueue::pop()
{
    std::unique_lock<std::mutex> lock(mutex_);
    has_element_.wait(lock, []{
        // always wake up if the queue is closed
        if (query_queue_.closed_) [[unlikely]]
            return true;
        // keep waiting if the queue is empty but not closed
        else if (query_queue_.command_list_.empty())
            return false;
        // wake up if there is no running_transaction_ or if the next command is from the running_transaction_
        else if (not query_queue_.running_transaction_ or std::get<0>(query_queue_.command_list_.front()) == *query_queue_.running_transaction_)
            return true;
        // otherwise keep waiting
        else
            return false;
    });
    // if the queue is still empty here the queue should be closed.
    if (command_list_.empty()) [[unlikely]] return std::nullopt;

    queued_command res = std::move(command_list_.front());
    command_list_.pop_front();

    // if there is currently no running transaction, set the next transaction in the queue as the running transaction
    if (not running_transaction_)
        running_transaction_ = &std::get<0>(res);

    return {std::move(res)};
}

void SerialScheduler::CommandQueue::push(Transaction &t, std::unique_ptr<ast::Command> command, Diagnostic &diag, std::promise<bool> promise)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_) {
        /* Since the command queue is closed, no more command will be executed
         * => set the promise of this newly pushed command to false right away */
        promise.set_value(false);
        return;
    }

    /* Check if transaction `t` already has elements in the queue, if not then check if the transaction is the currently
     * running transaction. If yes, then emplace the command in the front, otherwise emplace at the end. */
    auto it = std::find_if(command_list_.rend(), command_list_.rbegin(),
            [&t](queued_command &x){ return (t == std::get<0>(x)); }
    );
    if (running_transaction_ and *running_transaction_ == t and it == command_list_.rend())
        command_list_.emplace_front(t, std::move(command), diag, std::move(promise));
    else if (it == command_list_.rbegin())
        command_list_.emplace_back(t, std::move(command), diag, std::move(promise));
    else
        command_list_.emplace(++it.base(), t, std::move(command), diag, std::move(promise));

    lock.unlock();
    has_element_.notify_one();
}

void SerialScheduler::CommandQueue::close()
{
    std::unique_lock<std::mutex> lock(mutex_);
    closed_ = true;
    while (not command_list_.empty()) {
        std::get<3>(command_list_.front()).set_value(false);
        command_list_.pop_front();
    }
    lock.unlock();
    has_element_.notify_all();
}

bool SerialScheduler::CommandQueue::is_closed()
{
    std::lock_guard<std::mutex> lock(mutex_);
    return query_queue_.closed_;
}

void SerialScheduler::CommandQueue::stop_transaction(Transaction &t) {
    std::unique_lock<std::mutex> lock(mutex_);
    M_insist(&t == running_transaction_);
    running_transaction_ = nullptr;
    lock.unlock();
    has_element_.notify_one();
}

SerialScheduler::CommandQueue SerialScheduler::query_queue_;
std::atomic<int64_t> SerialScheduler::next_start_time = 1; // skip 0 as we interpret 0 as the UNDEFINED value

SerialScheduler::~SerialScheduler()
{
    if (schedule_thread_.joinable()) {
        query_queue_.close();
        schedule_thread_.join();
    }
}

std::future<bool> SerialScheduler::schedule_command(Transaction &t, std::unique_ptr<ast::Command> command, Diagnostic &diag)
{
    std::promise<bool> execution_completed;
    auto execution_completed_future = execution_completed.get_future();
    query_queue_.push(t, std::move(command), diag, std::move(execution_completed));

    if (not schedule_thread_.joinable()) [[unlikely]]
        // Creating the worker thread not here but in the constructor of `SerialScheduler` causes deadlocks.
        schedule_thread_ = std::thread(schedule_thread);
    return execution_completed_future;
}

std::unique_ptr<SerialScheduler::Transaction> SerialScheduler::begin_transaction() {
    return std::make_unique<SerialScheduler::Transaction>();
}

bool SerialScheduler::commit(std::unique_ptr<SerialScheduler::Transaction> t) {
    /* TODO: When autocommit is not used as the default anymore, the transaction must check for conflicts with
     * other transactions that were introduced in the time between when this transaction executed statements and now. */
    query_queue_.stop_transaction(*t);
    return true;
}

bool SerialScheduler::abort(std::unique_ptr<SerialScheduler::Transaction> t) {
    /* TODO: Undo changes of transaction */
    query_queue_.stop_transaction(*t);
    return true;
}

void SerialScheduler::schedule_thread()
{
    Catalog &C = Catalog::Get();
    while (not query_queue_.is_closed()) {
        // TODO: save currently executing transaction and only execute it's statements until it commits
        auto ret = query_queue_.pop();
        // pop() should only return no value if the queue is closed
        if (not ret.has_value()) continue;

        auto [t, ast, diag, promise] = std::move(ret.value());

        // check if transaction has a start_time, set one if not
        if (t.start_time() == 0) t.start_time(next_start_time++);
        // overflow detection
        if (next_start_time <= 0) [[unlikely]] next_start_time = 1;

        ast::Sema sema(diag);
        bool err = diag.num_errors() > 0; // parser errors

        diag.clear();
        auto cmd = sema.analyze(std::move(ast));
        err |= diag.num_errors() > 0; // sema errors

        M_insist(not err == bool(cmd), "when there are no errors, Sema must have returned a command");
        if (not err and cmd) {
            cmd->transaction(&t);
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
