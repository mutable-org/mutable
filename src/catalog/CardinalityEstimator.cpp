#include <mutable/catalog/CardinalityEstimator.hpp>

#include "backend/Interpreter.hpp"
#include "catalog/SpnWrapper.hpp"
#include "util/Spn.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <mutable/catalog/Catalog.hpp>
#include <mutable/IR/CNF.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/Options.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/Pool.hpp>
#include <nlohmann/json.hpp>


using namespace m;


namespace {

namespace options {

std::filesystem::path injected_cardinalities_file;

}

}

/*======================================================================================================================
 * CardinalityEstimator
 *====================================================================================================================*/

DataModel::~DataModel() { }

CardinalityEstimator::~CardinalityEstimator() { }

double CardinalityEstimator::predict_number_distinct_values(const DataModel&) const
{
    throw data_model_exception("predicting the number of distinct values is not supported by this data model.");
};

M_LCOV_EXCL_START
void CardinalityEstimator::dump(std::ostream &out) const
{
    print(out);
    out << std::endl;
}

void CardinalityEstimator::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * CartesianProductEstimator
 *====================================================================================================================*/

std::unique_ptr<DataModel> CartesianProductEstimator::empty_model() const
{
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = 0;
    return model;
}

std::unique_ptr<DataModel> CartesianProductEstimator::copy(const DataModel &_data) const
{
    auto &data = as<const CartesianProductDataModel>(_data);
    return std::make_unique<CartesianProductDataModel>(data); // copy
}

std::unique_ptr<DataModel> CartesianProductEstimator::estimate_scan(const QueryGraph &G, Subproblem P) const
{
    M_insist(P.size() == 1, "Subproblem must identify exactly one DataSource");
    auto idx = *P.begin();
    auto &BT = as<const BaseTable>(*G.sources()[idx]);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = BT.table().store().num_rows();
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_filter(const QueryGraph&, const DataModel &_data, const cnf::CNF&) const
{
    /* This model cannot estimate the effects of applying a filter. */
    auto &data = as<const CartesianProductDataModel>(_data);
    return std::make_unique<CartesianProductDataModel>(data); // copy
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_limit(const QueryGraph&, const DataModel &_data, std::size_t limit,
                                          std::size_t offset) const
{
    auto data = as<const CartesianProductDataModel>(_data);
    const std::size_t remaining = offset > data.size ? 0UL : data.size - offset;
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = std::min(remaining, limit);
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_grouping(const QueryGraph&, const DataModel &_data,
                                             const std::vector<group_type>&) const
{
    auto &data = as<const CartesianProductDataModel>(_data);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = data.size; // this model cannot estimate the effects of grouping
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_join(const QueryGraph&, const DataModel &_left, const DataModel &_right,
                                         const cnf::CNF&) const
{
    auto left = as<const CartesianProductDataModel>(_left);
    auto right = as<const CartesianProductDataModel>(_right);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = left.size * right.size; // this model cannot estimate the effects of a join condition
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_semi_join(const QueryGraph &G, const DataModel &_left, const DataModel &_right,
                   const cnf::CNF &condition) const
{
    auto left = as<const CartesianProductDataModel>(_left);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = left.size; // The left is not reduced as a cartesian product is assumed.
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_full_reduction(const m::QueryGraph &G, const m::DataModel &_model, Subproblem except) const
{
    auto model = as<const CartesianProductDataModel>(_model);
    auto reduced_model = std::make_unique<CartesianProductDataModel>();
    reduced_model->size = model.size; // The left is not reduced as a cartesian product is assumed.
    return reduced_model;
}

template<typename PlanTable>
std::unique_ptr<DataModel>
CartesianProductEstimator::operator()(estimate_join_all_tag, PlanTable &&PT, const QueryGraph&, Subproblem to_join,
                                      const cnf::CNF&) const
{
    M_insist(not to_join.empty());
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = 1UL;
    for (auto it = to_join.begin(); it != to_join.end(); ++it)
        model->size *= as<const CartesianProductDataModel>(*PT[it.as_set()].model).size;
    return model;
}

template
std::unique_ptr<DataModel>
CartesianProductEstimator::operator()(estimate_join_all_tag, const PlanTableSmallOrDense&, const QueryGraph&,
                                      Subproblem, const cnf::CNF&) const;
template
std::unique_ptr<DataModel>
CartesianProductEstimator::operator()(estimate_join_all_tag, const PlanTableLargeAndSparse&, const QueryGraph&,
                                      Subproblem, const cnf::CNF&) const;

std::size_t CartesianProductEstimator::predict_cardinality(const DataModel &data) const
{
    return as<const CartesianProductDataModel>(data).size;
}

M_LCOV_EXCL_START
void CartesianProductEstimator::print(std::ostream &out) const
{
    out << "CartesianProductEstimator - returns size of the Cartesian product of the given subproblems";
}
M_LCOV_EXCL_STOP


/*======================================================================================================================
 * InjectionCardinalityEstimator
 *====================================================================================================================*/

/*----- Constructors -------------------------------------------------------------------------------------------------*/

InjectionCardinalityEstimator::InjectionCardinalityEstimator(ThreadSafePooledString name_of_database)
    : fallback_(name_of_database)
{
    Diagnostic diag(Options::Get().has_color, std::cout, std::cerr);
    Position pos("InjectionCardinalityEstimator");

    if (options::injected_cardinalities_file.empty()) {
        std::cout << "No injection file was passed.\n";
    } else {
        std::ifstream in(options::injected_cardinalities_file);
        if (in) {
            read_json(diag, in, name_of_database);
        } else {
            diag.w(pos) << "Could not open file " << options::injected_cardinalities_file << ".\n"
                        << "A dummy estimator will be used to do estimations.\n";
        }
    }
}

InjectionCardinalityEstimator::InjectionCardinalityEstimator(Diagnostic &diag, ThreadSafePooledString name_of_database,
                                                             std::istream &in)
    : fallback_(name_of_database)
{
    read_json(diag, in, name_of_database);
}

void InjectionCardinalityEstimator::read_json(Diagnostic &diag, std::istream &in,
                                              const ThreadSafePooledString &name_of_database)
{
    Catalog &C = Catalog::Get();
    Position pos("InjectionCardinalityEstimator");
    std::string prev_relation;

    using json = nlohmann::json;
    json cardinalities;
    try {
        in >> cardinalities;
    } catch (json::parse_error parse_error) {
        diag.w(pos) << "The file could not be parsed as json. Parser error output:\n"
                    << parse_error.what() << "\n"
                    << "A dummy estimator will be used to do estimations.\n";
        return;
    }
    json *database_entry;
    try {
        database_entry = &cardinalities.at(*name_of_database); //throws if key does not exist
    } catch (json::out_of_range &out_of_range) {
        diag.w(pos) << "No entry for the db " << name_of_database << " in the file.\n"
                    << "A dummy estimator will be used to do estimations.\n";
        return;
    }

    cardinality_table_.reserve(database_entry->size());
    std::vector<std::string> names;
    auto write_identifier_to_buffer = [&names, this](auto &relations_array) {
        names.clear();
        for (auto it = relations_array->begin(); it != relations_array->end(); ++it)
            names.emplace_back(it->template get<std::string>());
        std::sort(names.begin(), names.end());

        buf_.clear();
        for (auto it = names.begin(); it != names.end(); ++it) {
            if (it != names.begin())
                buf_.emplace_back('$');
            buf_append(*it);
        }
        buf_.emplace_back(0);
    };
    for (auto &subproblem_entry : *database_entry) {
        json* relations_array;
        json* size;

        /* Regular Join */
        try {
            relations_array = &subproblem_entry.at("relations");
            size = &subproblem_entry.at("size");
        } catch (json::exception &exception) {
            diag.w(pos) << "The entry " << subproblem_entry << " for the db \"" << name_of_database << "\""
                        << " does not have the required form of {\"relations\": ..., \"size\": ... [, \"reductions\": ...] } "
                        << "and will thus be ignored.\n";
            continue;
        }

        write_identifier_to_buffer(relations_array);
        ThreadSafePooledString str = C.pool(buf_view());

        auto res = cardinality_table_.emplace(std::move(str), CardinalityEntry(*size, std::unordered_map<ThreadSafePooledString, std::size_t>(0)));
        M_insist(res.second, "insertion must not fail as we do not allow for duplicates in the input file");

        /* Semi joins */
        json* reductions_array;
        try {
            reductions_array = &subproblem_entry.at("reductions");
        } catch (json::exception &exception) {
            /* No warning here */
            continue;
        }

        res.first->second.semi_join_table.reserve(reductions_array->size());
        for (auto &reduction_entry : *reductions_array) {
            json* right_relations;
            try {
                right_relations = &reduction_entry.at("right_relations");
                size = &reduction_entry.at("size");
            } catch (json::exception &exception) {
                diag.w(pos) << "The entry " << reduction_entry << " for the db \"" << name_of_database << "\""
                            << " does not have the required form of {\"right_relations\": ..., \"size\": ...} "
                            << "and will thus be ignored.\n";
                continue;
            }
            write_identifier_to_buffer(right_relations);
            ThreadSafePooledString reduced_by_str = C.pool(buf_view());
            auto [reduce_it, success] = res.first->second.semi_join_table.emplace(std::move(reduced_by_str), *size);
            M_insist(success, "insertion must not fail as we do not allow for duplicate reduction entries for the same left relation in the input file");
        }

    }
}


/*----- Model calculation --------------------------------------------------------------------------------------------*/

std::unique_ptr<DataModel> InjectionCardinalityEstimator::empty_model() const
{
    return std::make_unique<InjectionCardinalityDataModel>(Subproblem(), 0);
}

std::unique_ptr<DataModel> InjectionCardinalityEstimator::copy(const DataModel &_data) const
{
    auto &data = as<const InjectionCardinalityDataModel>(_data);
    return std::make_unique<InjectionCardinalityDataModel>(data); // copy
}

std::unique_ptr<DataModel> InjectionCardinalityEstimator::estimate_scan(const QueryGraph &G, Subproblem P) const
{
    M_insist(P.size() == 1);
    auto id = make_identifier(G, P);

    if (auto it = cardinality_table_.find(id); it != cardinality_table_.end()) {
        return std::make_unique<InjectionCardinalityDataModel>(P, it->second.size);
    } else {
        /* no match, fall back */
        auto fallback_model = fallback_.estimate_scan(G, P);
        return std::make_unique<InjectionCardinalityDataModel>(P, fallback_.predict_cardinality(*fallback_model));
    }
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_filter(const QueryGraph&, const DataModel &_data, const cnf::CNF&) const
{
    /* This model cannot estimate the effects of applying a filter. */
    return copy(_data);
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_limit(const QueryGraph&, const DataModel &_data, std::size_t limit,
                                              std::size_t offset) const
{
    auto &data = as<const InjectionCardinalityDataModel>(_data);
    const std::size_t remaining = offset > data.size_ ? 0UL : data.size_ - offset;
    return std::make_unique<InjectionCardinalityDataModel>(data.subproblem_, std::min(remaining, limit));
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_grouping(const QueryGraph&, const DataModel &_data,
                                                 const std::vector<group_type> &exprs) const
{
    auto &data = as<const InjectionCardinalityDataModel>(_data);

    if (exprs.empty())
        return std::make_unique<InjectionCardinalityDataModel>(data.subproblem_, 1); // single group

    /* Combine grouping keys into an identifier. */
    oss_.str("");
    oss_ << "g";
    for (auto [grp, alias] : exprs) {
        oss_ << '#';
        if (alias.has_value())
            oss_ << alias;
        else
            oss_ << grp.get();
    }
    ThreadSafePooledString id = Catalog::Get().pool(oss_.str().c_str());

    if (auto it = cardinality_table_.find(id); it != cardinality_table_.end()) {
        /* Clamp injected cardinality to at most the cardinality of the grouping's child since it cannot produce more
         * tuples than it receives. */
        return std::make_unique<InjectionCardinalityDataModel>(data.subproblem_, std::min(it->second.size, data.size_));
    } else {
        /* This model cannot estimate the effects of grouping. */
        return std::make_unique<InjectionCardinalityDataModel>(data); // copy
    }
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_join(const QueryGraph &G, const DataModel &_left, const DataModel &_right,
                                             const cnf::CNF &condition) const
{
    auto &left  = as<const InjectionCardinalityDataModel>(_left);
    auto &right = as<const InjectionCardinalityDataModel>(_right);

    const Subproblem subproblem = left.subproblem_ | right.subproblem_;
    ThreadSafePooledString id = make_identifier(G, subproblem);

    /* Lookup cardinality in table. */
    if (auto it = cardinality_table_.find(id); it != cardinality_table_.end()) {
        /* Clamp injected cardinality to at most the cardinality of the cartesian product of the join's children
         * since it cannot produce more tuples than that. */
        const std::size_t max_cardinality = left.size_ * right.size_;
        return std::make_unique<InjectionCardinalityDataModel>(subproblem, std::min(it->second.size, max_cardinality));
    } else {
        /* Fallback to CartesianProductEstimator. */
        if (not Options::Get().quiet)
            std::cerr << "warning: failed to estimate the join of " << left.subproblem_ << " and " << right.subproblem_
                      << '\n';
        auto left_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
        left_fallback->size = left.size_;
        auto right_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
        right_fallback->size = right.size_;
        auto fallback_model = fallback_.estimate_join(G, *left_fallback, *right_fallback, condition);
        return std::make_unique<InjectionCardinalityDataModel>(subproblem,
                                                               fallback_.predict_cardinality(*fallback_model));
    }
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_semi_join(const QueryGraph &G, const DataModel &_left, const DataModel &_right,
                                             const cnf::CNF &condition) const
{
    auto &left  = as<const InjectionCardinalityDataModel>(_left);
    auto &right = as<const InjectionCardinalityDataModel>(_right);

    Subproblem new_reduced_by = left.reduced_by_ | right.subproblem_ | right.reduced_by_;
    ThreadSafePooledString left_id = make_identifier(G, left.subproblem_);
    ThreadSafePooledString reduced_id = make_identifier(G, new_reduced_by);


    if (auto left_it = cardinality_table_.find(left_id); left_it != cardinality_table_.end()) {
            if (auto right_it = left_it->second.semi_join_table.find(reduced_id); right_it != left_it->second.semi_join_table.end()) {
                /* Clamp injected cardinality to at most the size of the left input,
                 * since the relation can not become larger. */
                return std::make_unique<InjectionCardinalityDataModel>(left.subproblem_,
                                                                       new_reduced_by, std::min(right_it->second, left.size_));
            }
        }
    /* Fallback to CartesianProductEstimator.
    if (not Options::Get().quiet)
        std::cerr << "warning: failed to estimate the semi-join of " << left_id << " and " << reduced_id
                  << '\n';
    */
    auto left_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
    left_fallback->size = left.size_;
    auto right_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
    right_fallback->size = right.size_;
    auto fallback_model = fallback_.estimate_semi_join(G, *left_fallback, *right_fallback, condition);
    return std::make_unique<InjectionCardinalityDataModel>(left.subproblem_, new_reduced_by,
                                                           fallback_.predict_cardinality(*fallback_model));
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_full_reduction(const m::QueryGraph &G, const m::DataModel &_model, Subproblem except) const
{
    auto &model = as<const InjectionCardinalityDataModel>(_model);

    auto reduced_by_all = Subproblem::All(G.num_sources()) - model.subproblem_;
    ThreadSafePooledString left_id = make_identifier(G, model.subproblem_);
    ThreadSafePooledString neighbor_id = make_identifier(G, reduced_by_all - except);

    if (auto left_it = cardinality_table_.find(left_id); left_it != cardinality_table_.end()) {
        if (auto right_it = left_it->second.semi_join_table.find(neighbor_id); right_it != left_it->second.semi_join_table.end()) {
            /* Clamp injected cardinality to at most the size of the left input,
             * since the relation can not become larger. */
            return std::make_unique<InjectionCardinalityDataModel>(model.subproblem_, reduced_by_all,
                                                                   std::min(right_it->second, model.size_));
        }
    }

    /* Fallback to CartesianProductEstimator.
    if (not Options::Get().quiet)
        std::cerr << "warning: failed to estimate the full reduction of " << left_id
                  << '\n';
    */
    auto model_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
    model_fallback->size = model.size_;
    auto fallback_model = fallback_.estimate_full_reduction(G, *model_fallback, except);
    return std::make_unique<InjectionCardinalityDataModel>(model.subproblem_, reduced_by_all,
                                                           fallback_.predict_cardinality(*fallback_model));
}

template<typename PlanTable>
std::unique_ptr<DataModel>
InjectionCardinalityEstimator::operator()(estimate_join_all_tag, PlanTable &&PT, const QueryGraph &G,
                                          Subproblem to_join, const cnf::CNF&) const
{
    ThreadSafePooledString id = make_identifier(G, to_join);
    if (auto it = cardinality_table_.find(id); it != cardinality_table_.end()) {
        /* Clamp injected cardinality to at most the cardinality of the cartesian product of the join's children
         * since it cannot produce more tuples than that. */
        std::size_t max_cardinality = 1;
        for (auto it = to_join.begin(); it != to_join.end(); ++it)
            max_cardinality *= as<const InjectionCardinalityDataModel>(*PT[it.as_set()].model).size_;
        return std::make_unique<InjectionCardinalityDataModel>(to_join, std::min(it->second.size, max_cardinality));
    } else {
        /* Fallback to cartesian product. */
        if (not Options::Get().quiet)
            std::cerr << "warning: failed to estimate the join of all data sources in " << to_join << '\n';
        auto ds_it = to_join.begin();
        std::size_t size = as<const InjectionCardinalityDataModel>(*PT[ds_it.as_set()].model).size_;
        for (; ds_it != to_join.end(); ++ds_it)
            size *= as<const InjectionCardinalityDataModel>(*PT[ds_it.as_set()].model).size_;
        return std::make_unique<InjectionCardinalityDataModel>(to_join, size);
    }
}

template
std::unique_ptr<DataModel>
InjectionCardinalityEstimator::operator()(estimate_join_all_tag, const PlanTableSmallOrDense&, const QueryGraph&,
                                          Subproblem, const cnf::CNF&) const;

template
std::unique_ptr<DataModel>
InjectionCardinalityEstimator::operator()(estimate_join_all_tag, const PlanTableLargeAndSparse&, const QueryGraph&,
                                          Subproblem, const cnf::CNF&) const;

std::size_t InjectionCardinalityEstimator::predict_cardinality(const DataModel &data) const
{
    return as<const InjectionCardinalityDataModel>(data).size_;
}

M_LCOV_EXCL_START
void InjectionCardinalityEstimator::print(std::ostream &out) const
{
    constexpr uint32_t max_rows_printed = 100;     /// Number of rows of the cardinality_table printed
    constexpr uint32_t max_semi_joins_printed = 500; /// Number of rows of the semi_join_cardinality_table printed
    std::size_t sub_len = 13;                         /// Length of Subproblem column
    for (auto &entry : cardinality_table_)
        sub_len = std::max(sub_len, strlen(*entry.first));

    out << std::left << "InjectionCardinalityEstimator - Joins\n"
        << std::setw(sub_len) << "Subproblem" << "Size" << "\n" << std::right;

    /* ------- Print maximum max_rows_printed rows of the cardinality_table_ */
    uint32_t counter = 0;
    for (auto &entry : cardinality_table_) {
        if (counter >= max_rows_printed) break;
        out << std::left << std::setw(sub_len) << entry.first << entry.second.size << "\n";
        counter++;
    }

    out << std::left << "InjectionCardinalityEstimator - Semi-Joins\n"
        <<  "Left Subproblem" << "Reduced by" << "Right Subproblem" << "Size" << "\n" << std::right;

    counter = 0;
    for (auto left_it = cardinality_table_.begin(); left_it != cardinality_table_.end(); left_it++) {{
            for (auto right_it = left_it->second.semi_join_table.begin(); right_it != left_it->second.semi_join_table.end(); right_it++) {
                if (counter >= max_rows_printed) break;
                if (left_it == cardinality_table_.begin()) {
                    out << std::left << left_it->first << right_it->first << right_it->second << "\n";
                } else {
                    out << std::left << std::setw(sub_len) << right_it->first << right_it->second << "\n";
                }
                counter++;
            }
        }
    }
}
M_LCOV_EXCL_STOP

ThreadSafePooledString InjectionCardinalityEstimator::make_identifier(const QueryGraph &G, const Subproblem S) const
{
    auto &C = Catalog::Get();
    static thread_local std::vector<ThreadSafePooledString> names;
    names.clear();
    for (auto id : S) {
        auto &ds = G.sources()[id];
        if (auto Q = cast<Query>(ds.get())) {
            Q->query_graph().get_base_table_identifiers(names);
        } else names.emplace_back(ds->name());
    }
    std::sort(names.begin(), names.end(), [](auto lhs, auto rhs){ return strcmp(*lhs, *rhs) < 0; });

    buf_.clear();
    for (auto it = names.begin(); it != names.end(); ++it) {
        if (it != names.begin())
            buf_.emplace_back('$');
        buf_append(**it);
    }

    buf_.emplace_back(0);
    return C.pool(buf_view());
}

/*======================================================================================================================
 * SpnEstimator
 *====================================================================================================================*/

namespace {

/** Visitor to translate a CNF to an Spn filter. Only consider sargable expressions. */
struct FilterTranslator : ast::ConstASTExprVisitor {
    Catalog &C = Catalog::Get();
    ThreadSafePooledString attribute;
    float value;
    Spn::SpnOperator op;

    FilterTranslator() : attribute(C.pool("")), value(0), op(Spn::EQUAL) { }

    using ConstASTExprVisitor::operator();

    void operator()(const ast::Designator &designator) { attribute = designator.attr_name.text.assert_not_none(); }

    void operator()(const ast::Constant &constant) {
        auto val = Interpreter::eval(constant);

        visit(overloaded {
                [&val, this](const Numeric &numeric) {
                    switch (numeric.kind) {
                        case Numeric::N_Int:
                            value = float(val.as_i());
                            break;
                        case Numeric::N_Float:
                            value = val.as_f();
                            break;
                        case Numeric::N_Decimal:
                            value = float(val.as_d());
                            break;
                    }
                },
                [this](const NoneType&) { op = Spn::IS_NULL; },
                [](auto&&) { M_unreachable("Unsupported type."); },
        }, *constant.type());
    }

    void operator()(const ast::BinaryExpr &binary_expr) {
        switch (binary_expr.op().type) {
            case TK_EQUAL:
                op = Spn::EQUAL;
                break;
            case TK_LESS:
                op = Spn::LESS;
                break;
            case TK_LESS_EQUAL:
                op = Spn::LESS_EQUAL;
                break;
            case TK_GREATER:
                op = Spn::GREATER;
                break;
            case TK_GREATER_EQUAL:
                op = Spn::GREATER_EQUAL;
                break;
            default:
                M_unreachable("Operator can't be handled");
        }

        (*this)(*binary_expr.lhs);
        (*this)(*binary_expr.rhs);
    }

    void operator()(const ast::ErrorExpr&) { /* nothing to be done */ }
    void operator()(const ast::FnApplicationExpr &) { /* nothing to be done */ }
    void operator()(const ast::UnaryExpr &) { /* nothing to be done */ }
    void operator()(const ast::QueryExpr &) { /* nothing to be done */ }
};

/** Visitor to translate a CNF join condition (get the two identifiers of an equi Join). */
struct JoinTranslator : ast::ConstASTExprVisitor {

    std::vector<std::pair<ThreadSafePooledString, ThreadSafePooledString>> join_designator;

    using ConstASTExprVisitor::operator();

    void operator()(const ast::Designator &designator) {
        join_designator.emplace_back(designator.table_name.text, designator.attr_name.text);
    }

    void operator()(const ast::BinaryExpr &binary_expr) {

        if (binary_expr.op().type != m::TK_EQUAL) { M_unreachable("Operator can't be handled"); }

        (*this)(*binary_expr.lhs);
        (*this)(*binary_expr.rhs);
    }

    void operator()(const ast::Constant &) { /* nothing to be done */ }
    void operator()(const ast::ErrorExpr&) { /* nothing to be done */ }
    void operator()(const ast::FnApplicationExpr &) { /* nothing to be done */ }
    void operator()(const ast::UnaryExpr &) { /* nothing to be done */ }
    void operator()(const ast::QueryExpr &) { /* nothing to be done */ }
};

}

SpnEstimator::~SpnEstimator()
{
    for (auto &e : table_to_spn_)
        delete e.second;
}

void SpnEstimator::learn_spns() { table_to_spn_ = SpnWrapper::learn_spn_database(name_of_database_); }

void SpnEstimator::learn_new_spn(const ThreadSafePooledString &name_of_table)
{
    table_to_spn_.emplace(
        name_of_table,
        new SpnWrapper(SpnWrapper::learn_spn_table(name_of_database_, name_of_table))
    );
}

std::pair<unsigned, bool> SpnEstimator::find_spn_id(const SpnDataModel &data, SpnJoin &join)
{
    /* we only have a single spn */
    ThreadSafePooledString table_name = data.spns_.begin()->first;
    auto &attr_to_id = data.spns_.begin()->second.get().get_attribute_to_id();

    unsigned spn_id = 0;
    bool is_primary_key = false;

    /* check which identifier of the join corresponds to the table of the spn, get the corresponding attribute id */
    auto find_iter = table_name == join.first.first ?
            attr_to_id.find(join.first.second) : attr_to_id.find(join.second.second);

    if (find_iter != attr_to_id.end()) { spn_id = find_iter->second; }
    else { is_primary_key = true; }

    return {spn_id, is_primary_key};
}

std::size_t SpnEstimator::max_frequency(const SpnDataModel &data, SpnJoin &join)
{
    auto [spn_id, is_primary_key] = find_spn_id(data, join);

    /* maximum frequency is only computed on data models which only have one Spn */
    const SpnWrapper &spn = data.spns_.begin()->second.get();

    return is_primary_key ? 1 : data.num_rows_ / spn.estimate_number_distinct_values(spn_id);
}

std::size_t SpnEstimator::max_frequency(const SpnDataModel &data, const ThreadSafePooledString &attribute)
{
    /* maximum frequency is only computed on data models which only have one Spn */
    const SpnWrapper &spn = data.spns_.begin()->second.get();
    auto &attr_to_id = spn.get_attribute_to_id();
    auto find_iter = attr_to_id.find(attribute);

    return find_iter == attr_to_id.end() ? 1 : data.num_rows_ / spn.estimate_number_distinct_values(find_iter->second);
}

/*----- Model calculation --------------------------------------------------------------------------------------------*/

std::unique_ptr<DataModel> SpnEstimator::empty_model() const
{
    return std::make_unique<SpnDataModel>(table_spn_map(), 0);
}

std::unique_ptr<DataModel> SpnEstimator::copy(const DataModel &_data) const
{
    auto &data = as<const SpnDataModel>(_data);
    return std::make_unique<SpnDataModel>(data); // copy
}

std::unique_ptr<DataModel> SpnEstimator::estimate_scan(const QueryGraph &G, Subproblem P) const
{
    M_insist(P.size() == 1);
    const auto idx = *P.begin();
    auto &BT = as<const BaseTable>(*G.sources()[idx]);
    /* get the Spn corresponding for the table to scan */
    if (auto it = table_to_spn_.find(BT.name().assert_not_none()); it != table_to_spn_.end()) {
        table_spn_map spns;
        const SpnWrapper &spn = *it->second;
        spns.emplace(BT.name(), spn);
        return std::make_unique<SpnDataModel>(std::move(spns), spn.num_rows());
    } else {
        throw data_model_exception("Table does not exist.");
    }
}

std::unique_ptr<DataModel>
SpnEstimator::estimate_filter(const QueryGraph&, const DataModel &_data, const cnf::CNF &filter) const
{
    auto &data = as<const SpnDataModel>(_data);
    M_insist(data.spns_.size() == 1);
    auto new_data = std::make_unique<SpnDataModel>(data);
    auto &spn = new_data->spns_.begin()->second.get();
    auto &attribute_to_id = spn.get_attribute_to_id();

    Spn::Filter translated_filter;
    FilterTranslator ft;

    /* only consider clauses with one element, since Spns cannot estimate disjunctions */
    for (auto &clause : filter) {
        M_insist(clause.size() == 1);
        ft(*clause[0]);
        unsigned spn_id;

        if (auto it = attribute_to_id.find(ft.attribute); it != attribute_to_id.end()) {
            spn_id = it->second;
        } else {
            throw data_model_exception("Attribute does not exist.");
        }

        translated_filter.emplace(spn_id, std::make_pair(ft.op, ft.value));
    }

    /* Save number of rows in the newly constructed data model with the filter applied */
    new_data->num_rows_ = float(new_data->num_rows_) * spn.likelihood(translated_filter);
    return new_data;
}

std::unique_ptr<DataModel>
SpnEstimator::estimate_limit(const QueryGraph&, const DataModel &data, std::size_t limit, std::size_t) const
{
    auto model = std::make_unique<SpnDataModel>(as<const SpnDataModel>(data));
    model->num_rows_ = std::min(model->num_rows_, limit);
    return model;
}

std::unique_ptr<DataModel> SpnEstimator::estimate_grouping(const QueryGraph&, const DataModel &data,
                                                           const std::vector<group_type> &groups) const
{
    auto model = std::make_unique<SpnDataModel>(as<const SpnDataModel>(data));
    std::size_t num_rows = 1;
    for (auto [grp, alias] : groups) {
        auto designator = cast<const ast::Designator>(&grp.get());
        if (not designator)
            throw data_model_exception("SpnEstimator only supports Designators and no composed expressions");
        auto spn_it = model->spns_.find(designator->table_name.text.assert_not_none());
        if (spn_it == model->spns_.end())
            throw data_model_exception("Could not find table for grouping.");

        auto &spn = spn_it->second.get();
        auto &attr_to_id = spn.get_attribute_to_id();
        if (auto attr_it = attr_to_id.find(designator->attr_name.text.assert_not_none()); attr_it != attr_to_id.end()) {
            num_rows *= spn.estimate_number_distinct_values(attr_it->second);
        } else {
            num_rows *= spn.num_rows(); // if attribute is primary key, distinct values = num rows
        }
    }
    model->num_rows_ = num_rows;
    return model;
}

std::unique_ptr<DataModel>
SpnEstimator::estimate_join(const QueryGraph&, const DataModel &_left, const DataModel &_right,
                            const cnf::CNF &condition) const
{
    auto &left = as<const SpnDataModel>(_left);
    auto &right = as<const SpnDataModel>(_right);

    auto new_left = std::make_unique<SpnDataModel>(left);
    auto new_right = std::make_unique<SpnDataModel>(right);

    JoinTranslator jt;

    if (not condition.empty()) {
        /* only consider single equi join */
        jt(*condition[0][0]);
        auto first_identifier = std::make_pair(jt.join_designator[0].first, jt.join_designator[0].second);
        auto second_identifier = std::make_pair(jt.join_designator[1].first, jt.join_designator[1].second);
        SpnJoin join = std::make_pair(first_identifier, second_identifier);

        /* if a data model is only responsible for one table (one spn) add the maximum frequency of the value
        * of the joined attribute */
        if (left.spns_.size() == 1) { new_left->max_frequencies_.emplace_back(max_frequency(left, join)); }
        if (right.spns_.size() == 1) { new_right->max_frequencies_.emplace_back(max_frequency(right, join)); }

        /* compute the estimated cardinality of the join via distinct count estimates.
         * See http://www.cidrdb.org/cidr2021/papers/cidr2021_paper01.pdf */
        std::size_t mf_left = std::accumulate(
                new_left->max_frequencies_.begin(), new_left->max_frequencies_.end(), 1, std::multiplies<>());

        std::size_t mf_right = std::accumulate(
                new_right->max_frequencies_.begin(), new_right->max_frequencies_.end(), 1, std::multiplies<>());

        std::size_t left_clause = new_left->num_rows_ / mf_left;
        std::size_t right_clause = new_right->num_rows_ / mf_right;

        std::size_t num_rows_join = std::min<std::size_t>(left_clause, right_clause) * mf_left * mf_right;

        new_left->num_rows_ = num_rows_join;
    } else {
        /* compute cartesian product since there is no join condition */
        if (left.spns_.size() == 1) { new_left->max_frequencies_.emplace_back(left.num_rows_); }
        if (right.spns_.size() == 1) { new_right->max_frequencies_.emplace_back(right.num_rows_); }

        new_left->num_rows_ = left.num_rows_ * right.num_rows_;
    }

    /* copy data from new_right to new_left to collect all information in one DataModel */
    new_left->spns_.insert(new_right->spns_.begin(), new_right->spns_.end());
    new_left->max_frequencies_.insert(
            new_left->max_frequencies_.end(), new_right->max_frequencies_.begin(), new_right->max_frequencies_.end());

    return new_left;
}

std::unique_ptr<DataModel>
SpnEstimator::estimate_semi_join(const QueryGraph&, const DataModel &_left, const DataModel &_right,
                            const cnf::CNF &condition) const
{
    auto &left = as<const SpnDataModel>(_left);
    auto &right = as<const SpnDataModel>(_right);

    auto new_left = std::make_unique<SpnDataModel>(left);
    auto new_right = std::make_unique<SpnDataModel>(right);

    JoinTranslator jt;

    if (not condition.empty()) {
        /* only consider single equi join */
        jt(*condition[0][0]);
        auto first_identifier = std::make_pair(jt.join_designator[0].first, jt.join_designator[0].second);
        auto second_identifier = std::make_pair(jt.join_designator[1].first, jt.join_designator[1].second);
        SpnJoin join = std::make_pair(first_identifier, second_identifier);

        /* if a data model is only responsible for one table (one spn) add the maximum frequency of the value
        * of the joined attribute */
        if (left.spns_.size() == 1) { new_left->max_frequencies_.emplace_back(max_frequency(left, join)); }
        if (right.spns_.size() == 1) { new_right->max_frequencies_.emplace_back(max_frequency(right, join)); }

        /* compute the estimated cardinality of the join via distinct count estimates.
         * See http://www.cidrdb.org/cidr2021/papers/cidr2021_paper01.pdf */
        std::size_t mf_left = std::accumulate(
                new_left->max_frequencies_.begin(), new_left->max_frequencies_.end(), 1, std::multiplies<>());

        std::size_t mf_right = std::accumulate(
                new_right->max_frequencies_.begin(), new_right->max_frequencies_.end(), 1, std::multiplies<>());

        std::size_t left_clause = new_left->num_rows_ / mf_left;
        std::size_t right_clause = new_right->num_rows_ / mf_right;

        std::size_t num_rows_join = std::min<std::size_t>(left_clause, right_clause) * mf_left;

        new_left->num_rows_ = num_rows_join;
    } else {
        /* compute cartesian product since there is no join condition */
        if (left.spns_.size() == 1) { new_left->max_frequencies_.emplace_back(left.num_rows_); }
        if (right.spns_.size() == 1) { new_right->max_frequencies_.emplace_back(right.num_rows_); }

        new_left->num_rows_ = left.num_rows_ * right.num_rows_;
    }

    /* copy data from new_right to new_left to collect all information in one DataModel */
    new_left->spns_.insert(new_right->spns_.begin(), new_right->spns_.end());
    new_left->max_frequencies_.insert(
            new_left->max_frequencies_.end(), new_right->max_frequencies_.begin(), new_right->max_frequencies_.end());

    return new_left;
}

template<typename PlanTable>
std::unique_ptr<DataModel>
SpnEstimator::operator()(estimate_join_all_tag, PlanTable &&PT, const QueryGraph&, Subproblem to_join,
                         const cnf::CNF &condition) const
{
    M_insist(not to_join.empty());
    /* compute cartesian product */
    if (condition.empty()) {
        auto model = std::make_unique<SpnDataModel>();
        model->num_rows_ = 1UL;
        for (auto it = to_join.begin(); it != to_join.end(); ++it)
            model->num_rows_ *= as<const SpnDataModel>(*PT[it.as_set()].model).num_rows_;
        return model;
    }

    /* get all attributes to join on */
    JoinTranslator jt;
    std::unordered_map<ThreadSafePooledString, ThreadSafePooledString> table_to_attribute;
    for (auto clause : condition) {
        jt(*clause[0]);
        table_to_attribute.emplace(jt.join_designator[0].first, jt.join_designator[0].second);
        table_to_attribute.emplace(jt.join_designator[1].first, jt.join_designator[1].second);
    }

    /* get first model to join */
    SpnDataModel final_model = as<const SpnDataModel>(*PT[to_join.begin().as_set()].model);
    ThreadSafePooledString first_table_name = final_model.spns_.begin()->first;

    /* if there is a join condition on this model, get the maximum frequency of the attribute */
    if (auto find_iter = table_to_attribute.find(first_table_name); find_iter != table_to_attribute.end()) {
        final_model.max_frequencies_.emplace_back(max_frequency(final_model, find_iter->second));
    }
    /* else, maximum frequency is set as the number of rows (to compute the cartesian product) */
    else {
        final_model.max_frequencies_.emplace_back(final_model.spns_.begin()->second.get().num_rows());
    }

    /* iteratively add the next model to join via distinct count estimates */
    for (auto it = ++to_join.begin(); it != to_join.end(); it++) {
        SpnDataModel model = as<const SpnDataModel>(*PT[it.as_set()].model);
        ThreadSafePooledString table_name = model.spns_.begin()->first;

        if (auto find_iter = table_to_attribute.find(table_name); find_iter != table_to_attribute.end()) {
            model.max_frequencies_.emplace_back(max_frequency(model, find_iter->second));
        } else {
            model.max_frequencies_.emplace_back(model.spns_.begin()->second.get().num_rows());
        }

        std::size_t mf_left = std::accumulate(
                final_model.max_frequencies_.begin(), final_model.max_frequencies_.end(), 1, std::multiplies<>());
        std::size_t mf_right = model.max_frequencies_[0];

        std::size_t left_clause = final_model.num_rows_ / mf_left;
        std::size_t right_clause = model.num_rows_ / mf_right;

        std::size_t num_rows_join = std::min<std::size_t>(left_clause, right_clause) * mf_left * mf_right;

        final_model.num_rows_ = num_rows_join;

        /* copy data from model to final_model to collect all information in one DataModel */
        final_model.spns_.emplace(*model.spns_.begin());
        final_model.max_frequencies_.emplace_back(model.max_frequencies_[0]);
    }

    return std::make_unique<SpnDataModel>(final_model);
}

std::unique_ptr<DataModel>
SpnEstimator::estimate_full_reduction(const QueryGraph &G, const DataModel &_model, Subproblem except) const
{
    //TODO: Not supported yet, currently just Cartesian Product is assumed
    auto &model = as<const SpnDataModel>(_model);
    auto new_model = std::make_unique<SpnDataModel>(model);

    if (model.spns_.size() == 1) { new_model->max_frequencies_.emplace_back(model.num_rows_); }

    new_model->num_rows_ = model.num_rows_;

    return new_model;

}

std::size_t SpnEstimator::predict_cardinality(const DataModel &_data) const
{
    auto &data = as<const SpnDataModel>(_data);
    return data.num_rows_;
}

void SpnEstimator::print(std::ostream&) const { }


#define LIST_CE(X) \
    X(CartesianProductEstimator, "CartesianProduct", "estimates cardinalities as Cartesian product") \
    X(InjectionCardinalityEstimator, "Injected", "estimates cardinalities based on a JSON file") \
    X(SpnEstimator, "Spn", "estimates cardinalities based on Sum-Product Networks")

#define INSTANTIATE(TYPE, _1, _2) \
    template std::unique_ptr<DataModel> TYPE::operator()(estimate_join_all_tag, PlanTableSmallOrDense &&PT, \
                                                         const QueryGraph &G, Subproblem to_join, \
                                                         const cnf::CNF &condition) const; \
    template std::unique_ptr<DataModel> TYPE::operator()(estimate_join_all_tag, PlanTableLargeAndSparse &&PT, \
                                                         const QueryGraph &G, Subproblem to_join, \
                                                         const cnf::CNF &condition) const;
LIST_CE(INSTANTIATE)
#undef INSTANTIATE

__attribute__((constructor(202)))
static void register_cardinality_estimators()
{
    Catalog &C = Catalog::Get();

#define REGISTER(TYPE, NAME, DESCRIPTION) \
    C.register_cardinality_estimator<TYPE>(C.pool(NAME), DESCRIPTION);
LIST_CE(REGISTER)
#undef REGISTER

    C.arg_parser().add<bool>(
        /* group=       */ "Cardinality estimation",
        /* short=       */ nullptr,
        /* long=        */ "--show-cardinality-file-example",
        /* description= */ "show an example of an input JSON file for cardinality injection",
        [] (bool) {
            std::cout << "\
Example for injected cardinalities file:\n\
{\n\
    database1: [\n\
            {\n\
                \"relations\": [\"A\", \"B\", ...],\n\
                \"size\": 150\n\
            },\n\
            {\n\
                \"relations\": [\"C\", \"A\", ...],\n\
                \"size\": 100\n\
            },\n\
    },\n\
    database2: [\n\
            {\n\
                \"relations\": [\"customers\"],\n\
                \"size\": 1000\n\
            },\n\
            {\n\
                \"relations\": [\"customers\", \"orders\", ...],\n\
                \"size\": 50\n\
            },\n\
    },\n\
}\n";
            exit(EXIT_SUCCESS);
        }
    );
    C.arg_parser().add<const char*>(
        /* group=       */ "Cardinality estimation",
        /* short=       */ nullptr,
        /* long=        */ "--use-cardinality-file",
        /* description= */ "inject cardinalities from the given JSON file",
        [] (const char *path) {
            options::injected_cardinalities_file = path;
        }
    );
}
