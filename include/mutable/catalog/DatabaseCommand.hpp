#pragma once

#include <mutable/util/Diagnostic.hpp>
#include <vector>


namespace m {

/** The command pattern for operations in the DBMS. */
struct DatabaseCommand
{
    virtual ~DatabaseCommand() = default;

    /** Executes the command. */
    virtual void execute(Diagnostic &diag) const = 0;
};

/** A `DatabaseInstruction` represents an invokation of an *instruction*, i.e. an input starting with `\`, an
 * instruction name, and the arguments for that instruction. A new instruction can be implemented by making a new
 * subclass of `DatabaseInstruction`. The newly implemented instruction must then be registered in the `Catalog` using
 * `Catalog::register_instruction()`. */
struct DatabaseInstruction : DatabaseCommand
{
    private:
    std::vector<std::string> args_; ///< the arguments of this instruction

    public:
    DatabaseInstruction(std::vector<std::string> args) : args_(std::move(args)) { }

    /** Returns the arguments of this instruction. */
    const std::vector<std::string> & args() const { return args_; }
};

/** Learn an SPN on every table in the database that is currently in use. */
struct learn_spns : DatabaseInstruction
{
    learn_spns(std::vector<std::string> args) : DatabaseInstruction(std::move(args)) { }

    void execute(Diagnostic &diag) const override;
};

}
