#pragma once

#include "mutable/util/Diagnostic.hpp"
#include <vector>


namespace m {

/** The command pattern for `Stmt`s and `Instruction`s. */
struct DatabaseCommand
{
    virtual void execute(Diagnostic &diag) const = 0;
    virtual ~DatabaseCommand() = default;
};

/** Implementation of an `Instruction`. A `DatabaseInstruction` can be used to implement custom commands (not to
 * be confused with the superclass `DatabaseCommand` which is called so because of the Command pattern). A new
 * instruction can be implemented by making a new subclass of `DatabaseInstruction` and overriding the method
 * `execute_instruction`. The newly implemented instruction must then be registered in the `Catalog`. */
struct DatabaseInstruction : DatabaseCommand
{
    void execute(Diagnostic &diag) const override { execute_instruction(std::vector<std::string>(), diag); };
    virtual void execute_instruction(const std::vector<std::string> &args, Diagnostic &diag) const = 0;
};

/** Learn an SPN on every table in the database that is currently in use. */
struct learn_spns : DatabaseInstruction
{
    void execute_instruction(const std::vector<std::string>&, Diagnostic &diag) const override;
};

}
