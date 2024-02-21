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
#include <mutable/lex/Token.hpp>
#include <mutable/Options.hpp>
#include <mutable/parse/AST.hpp>
#include <mutable/util/enum_ops.hpp>
#include <mutable/util/fn.hpp>
#include <stdexcept>


using namespace m;


/*======================================================================================================================
 * Schema
 *====================================================================================================================*/

Schema::Identifier Schema::Identifier::GetConstant()
{
    return Identifier(Catalog::Get().pool("$const"));
}

Schema::Identifier::Identifier(const ast::Expr &expr)
    : name(Catalog::Get().pool(""))
{
    if (auto d = cast<const ast::Designator>(&expr)) {
        prefix = d->table_name.text;
        name = d->attr_name.text.assert_not_none();
    } else {
        std::ostringstream oss;
        oss << expr;
        prefix = {};
        name = Catalog::Get().pool(oss.str().c_str());
    }
}

Schema::entry_type::entry_type() : id(Catalog::Get().pool("")) { }

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
    out << "Attribute `" << table.name() << "`.`" << name << "`, "
        << "id " << id << ", "
        << "type " << *type
        << std::endl;
}

void Attribute::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * ConcreteTable
 *====================================================================================================================*/

Schema ConcreteTable::schema(const ThreadSafePooledOptionalString &alias) const
{
    Schema S;
    for (auto attr = this->begin_all(); attr != this->end_all(); ++attr) {
        Schema::entry_type::constraints_t constraints{0};
        if (attr->not_nullable)
            constraints |= Schema::entry_type::NOT_NULLABLE;
        if (attr->is_unique())
            constraints |= Schema::entry_type::UNIQUE;
        if (attr->reference and attr->reference->is_unique())
            constraints |= Schema::entry_type::REFERENCES_UNIQUE;
        if (attr->is_hidden)
            constraints |= Schema::entry_type::IS_HIDDEN;
        S.add({alias.has_value() ? alias : this->name(), attr->name}, attr->type, constraints);
    }
    return S;
}

void ConcreteTable::layout(const storage::DataLayoutFactory &factory) {
    view v(cbegin_all(), cend_all(), [](auto it) -> auto & { return it->type; });
    layout_ = factory.make(v.begin(), v.end());
}

M_LCOV_EXCL_START
void ConcreteTable::dump(std::ostream &out) const
{
    out << "Table `" << name_ << '`';
    for (const auto &attr : attrs_)
        out << "\n` " << attr.id << ": `" << attr.name << "` " << *attr.type;
    out << std::endl;
}

void ConcreteTable::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * MultiVersioningTable
 *====================================================================================================================*/

MultiVersioningTable::MultiVersioningTable(std::unique_ptr<Table> table)
    : TableDecorator(std::move(table))
{
    auto &C = Catalog::Get();

    /*----- Add hidden timestamp attributes. -----*/
    auto i8 = Type::Get_Integer(Type::TY_Vector, 8);
    auto ts_begin = C.pool("$ts_begin");
    auto ts_end = C.pool("$ts_end");
    table_->push_back(ts_begin, i8);
    table_->push_back(ts_end, i8);
    (*table_)[ts_begin].is_hidden = true;
    (*table_)[ts_begin].not_nullable = true;
    (*table_)[ts_end].is_hidden = true;
    (*table_)[ts_end].not_nullable = true;
}

M_LCOV_EXCL_START
void MultiVersioningTable::dump(std::ostream &out) const
{
    out << "MultiVersioningTable Decorator" << std::endl;
    table_->dump(out);
}

void MultiVersioningTable::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


namespace {


void apply_timestamp_filter(QueryGraph &G)
{
    Catalog &C = Catalog::Get();

    auto pos = Position(nullptr);
    ast::Token ts_begin(pos, C.pool("$ts_begin"), TK_IDENTIFIER);
    ast::Token ts_end(pos, C.pool("$ts_end"), TK_IDENTIFIER);

    for (auto &ds : G.sources()) {
        if (auto bt = cast<const BaseTable>(ds.get())) {
            /* Set timestamp filter */
            auto it = std::find_if(bt->table().cbegin_hidden(),
                                   bt->table().end_hidden(),
                                   [&](const Attribute & attr) {
                                       return attr.name == C.pool("$ts_begin");
                                   });

            if (it != bt->table().end_hidden()) {
                ast::Token table_name(pos, bt->table().name(), TK_EOF);
                /*----- Build AST -----*/
                // $ts_begin
                std::unique_ptr<ast::Expr> ts_begin_designator = std::make_unique<ast::Designator>(
                        ts_begin,
                        table_name,
                        ts_begin,
                        Type::Get_Integer(Type::TY_Vector, 8),
                        &*it
                );

                // TST := transaction start time constant
                std::unique_ptr<ast::Expr> ts_begin_transaction_constant = std::make_unique<ast::Constant>(ast::Token(
                        pos,
                        C.pool(std::to_string(G.transaction()->start_time()).c_str()),
                        m::TK_DEC_INT
                ));
                ts_begin_transaction_constant->type(Type::Get_Integer(Type::TY_Vector, 8));

                // $ts_begin <= TST
                std::unique_ptr<ast::Expr> ts_begin_filter_clause = std::make_unique<ast::BinaryExpr>(
                        ast::Token(pos, C.pool("<="), TK_LESS_EQUAL),
                        std::move(ts_begin_designator),
                        std::move(ts_begin_transaction_constant)
                );
                ts_begin_filter_clause->type(Type::Get_Boolean(Type::TY_Vector));

                // $ts_end
                std::unique_ptr<ast::Expr> ts_end_designator = std::make_unique<ast::Designator>(
                        ts_end,
                        table_name,
                        ts_end,
                        Type::Get_Integer(Type::TY_Vector, 8),
                        &bt->table()[C.pool("$ts_end")]
                );

                // TST := transaction start time constant
                std::unique_ptr<ast::Expr> ts_end_transaction_constant = std::make_unique<ast::Constant>(ast::Token(
                        pos,
                        C.pool(std::to_string(G.transaction()->start_time()).c_str()),
                        m::TK_DEC_INT
                ));
                ts_end_transaction_constant->type(Type::Get_Integer(Type::TY_Vector, 8));

                // $ts_end > TST
                std::unique_ptr<ast::Expr> ts_end_greater_transaction_expr = std::make_unique<ast::BinaryExpr>(
                        ast::Token(pos, C.pool(">"), TK_GREATER),
                        std::move(ts_end_designator),
                        std::move(ts_end_transaction_constant)
                );
                ts_end_greater_transaction_expr->type(Type::Get_Boolean(Type::TY_Vector));

                // $ts_end
                std::unique_ptr<ast::Expr> ts_end_designator_2 = std::make_unique<ast::Designator>(
                        ts_end,
                        table_name,
                        ts_end,
                        Type::Get_Integer(Type::TY_Vector, 8),
                        &bt->table()[C.pool("$ts_end")]
                );

                // -1
                std::unique_ptr<ast::Expr> neg_one_constant = std::make_unique<ast::Constant>(ast::Token(
                        pos,
                        C.pool("-1"),
                        m::TK_DEC_INT
                ));
                neg_one_constant->type(Type::Get_Integer(Type::TY_Vector, 8));

                // $ts_end = -1
                std::unique_ptr<ast::Expr> ts_end_eq_zero = std::make_unique<ast::BinaryExpr>(
                        ast::Token(pos, C.pool("="), TK_EQUAL),
                        std::move(ts_end_designator_2),
                        std::move(neg_one_constant)
                );
                ts_end_eq_zero->type(Type::Get_Boolean(Type::TY_Vector));

                // $ts_end > TST OR $ts_end = -1
                std::unique_ptr<ast::Expr> ts_end_filter_clause = std::make_unique<ast::BinaryExpr>(
                        ast::Token(pos, C.pool("OR"), TK_Or),
                        std::move(ts_end_greater_transaction_expr),
                        std::move(ts_end_eq_zero)
                );
                ts_end_filter_clause->type(Type::Get_Boolean(Type::TY_Vector));

                // $ts_begin <= TST AND ($ts_end > TST OR $ts_end = 0)
                std::unique_ptr<ast::Expr> filter_expr = std::make_unique<ast::BinaryExpr>(
                        ast::Token(pos, C.pool("AND"), TK_And),
                        std::move(ts_begin_filter_clause),
                        std::move(ts_end_filter_clause)
                );
                filter_expr->type(Type::Get_Boolean(Type::TY_Vector));

                G.add_custom_filter(std::move(filter_expr), *ds);
            }
        }
    }
}

__attribute__((constructor(202)))
void register_pre_optimization()
{
    Catalog &C = Catalog::Get();
    C.register_pre_optimization(C.pool("multi-versioning"),
                                apply_timestamp_filter,
                                "adds timestamp filters to the QueryGraph");
}


}


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

Database::Database(ThreadSafePooledString name)
    : name(name)
{
    cardinality_estimator_ = Catalog::Get().create_cardinality_estimator(std::move(name));
}

Database::~Database()
{
    for (auto &f : functions_)
        delete f.second;
}

Table & Database::add_table(ThreadSafePooledString name) {
    auto it = tables_.find(name);
    if (it != tables_.end()) throw std::invalid_argument("table with that name already exists");
    it = tables_.emplace_hint(it, std::move(name), Catalog::Get().table_factory().make(name));
    return *it->second;
}

const Function * Database::get_function(const ThreadSafePooledString &name) const
{
    try {
        return functions_.at(name);
    } catch (std::out_of_range) {
        /* not defined within the database; search the global catalog */
        return Catalog::Get().get_function(name);
    }
}
