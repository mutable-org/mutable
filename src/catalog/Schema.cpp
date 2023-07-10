#include <mutable/catalog/Schema.hpp>

#include <algorithm>
#include <cmath>
#include <iostream>
#include <iterator>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/catalog/CostFunction.hpp>
#include <mutable/catalog/CostFunctionCout.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/enum_ops.hpp>
#include <mutable/util/fn.hpp>
#include <stdexcept>


using namespace m;


/*======================================================================================================================
 * Schema
 *====================================================================================================================*/

Schema::Identifier Schema::Identifier::CONST_ID_ = Schema::Identifier(Catalog::Get().pool("$const"));

Schema::Identifier::Identifier(const ast::Expr &expr)
{
    if (auto d = cast<const ast::Designator>(&expr)) {
        prefix = d->table_name.text;
        name = d->attr_name.text;
    } else {
        std::ostringstream oss;
        oss << expr;
        prefix = nullptr;
        name = Catalog::Get().pool(oss.str().c_str());
    }
}

M_LCOV_EXCL_START
void Schema::dump(std::ostream &out) const { out << *this << std::endl; }
void Schema::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * Attribute
 *====================================================================================================================*/

bool Attribute::is_unique() const {
    auto primary_key = table.primary_key();
    auto pred = [this](const auto &ref){ return *this == ref.get(); };
    return unique or (primary_key.size() == 1 and
                      std::find_if(primary_key.cbegin(), primary_key.cend(), pred) != primary_key.cend());
}

M_LCOV_EXCL_START
void Attribute::dump(std::ostream &out) const
{
    out << "Attribute `" << table.name << "`.`" << name << "`, "
        << "id " << id << ", "
        << "type " << *type
        << std::endl;
}

void Attribute::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * Table
 *====================================================================================================================*/

Schema Table::schema(const char *alias) const
{
    Schema S;
    for (auto &attr : *this) {
        Schema::entry_type::constraints_t constraints{0};
        if (attr.not_nullable)
            constraints |= Schema::entry_type::NOT_NULLABLE;
        if (attr.is_unique())
            constraints |= Schema::entry_type::UNIQUE;
        if (attr.reference and attr.reference->is_unique())
            constraints |= Schema::entry_type::REFERENCES_UNIQUE;
        S.add({alias ? alias : this->name, attr.name}, attr.type, constraints);
    }
    return S;
}

void Table::layout(const storage::DataLayoutFactory &factory) {
    view v(cbegin(), cend(), [](auto it) -> auto & { return it->type; });
    layout_ = factory.make(v.begin(), v.end());
}

M_LCOV_EXCL_START
void Table::dump(std::ostream &out) const
{
    out << "Table `" << name << '`';
    for (const auto &attr : attrs_)
        out << "\n` " << attr.id << ": `" << attr.name << "` " << *attr.type;
    out << std::endl;
}

void Table::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * Function
 *====================================================================================================================*/

constexpr const char * Function::FNID_TO_STR_[];
constexpr const char * Function::KIND_TO_STR_[];

M_LCOV_EXCL_START
void Function::dump(std::ostream &out) const
{
    out << "Function{ name = \"" << name << "\", fnid = " << FNID_TO_STR_[fnid] << ", kind = " << KIND_TO_STR_[kind]
        << "}" << std::endl;
}
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * Database
 *====================================================================================================================*/

Database::Database(const char *name)
    : name(name)
{
    cardinality_estimator_ = Catalog::Get().create_cardinality_estimator(name);
}

Database::~Database()
{
    for (auto &r : tables_)
        delete r.second;
    for (auto &f : functions_)
        delete f.second;
}

const Function * Database::get_function(const char *name) const
{
    try {
        return functions_.at(name);
    } catch (std::out_of_range) {
        /* not defined within the database; search the global catalog */
        return Catalog::Get().get_function(name);
    }
}
