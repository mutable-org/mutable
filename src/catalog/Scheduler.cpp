#include <mutable/catalog/Scheduler.hpp>


using namespace m;


std::atomic<uint64_t> Scheduler::Transaction::next_id_;

bool Scheduler::autocommit (std::unique_ptr<ast::Command> command, Diagnostic &diag) {
    auto t = begin_transaction();
    auto res_future = schedule_command(*t, std::move(command), diag);
    res_future.wait();
    if (res_future.get()) {
        bool res = commit(std::move(t));
        M_insist(res);
        return res;
    } else {
        bool aborted = abort(std::move(t));
        M_insist(aborted);
        return false;
    }
}
