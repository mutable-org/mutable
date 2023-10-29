#pragma once

#include <mutable/mutable-config.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <future>


namespace m {

/** The Scheduler handles the execution of all incoming queries. The implementation stored in the catalog determines
 * when and how queries are executed. */
struct M_EXPORT Scheduler
{
    protected:
    using queued_command = std::tuple<std::unique_ptr<ast::Command>, Diagnostic &, std::promise<bool>>;

    public:
    Scheduler() = default;
    virtual ~Scheduler() {}

    /** Schedule a `ast::Command` for execution.
     * Blocks execution until the `ast::Command` finished its execution or until the Scheduler aborts the execution.
     * Returns true if the `ast::Command` was successfully executed, false otherwise. */
    virtual bool schedule_command(std::unique_ptr<ast::Command> command, Diagnostic &diag) = 0;
};

}
