#include <mutable/catalog/CardinalityEstimator.hpp>

#include "globals.hpp"
#include <algorithm>
#include <cstring>
#include <mutable/IR/CNF.hpp>
#include <mutable/IR/Operator.hpp>
#include <mutable/IR/QueryGraph.hpp>
#include <mutable/util/Diagnostic.hpp>
#include <nlohmann/json.hpp>


using namespace m;
using DataModel = CardinalityEstimator::DataModel;


/*======================================================================================================================
 * CardinalityEstimator
 *====================================================================================================================*/

CardinalityEstimator::DataModel::~DataModel() { }

CardinalityEstimator::~CardinalityEstimator() { }

double CardinalityEstimator::predict_number_distinct_values(const DataModel&) const
{
    throw data_model_exception("predicting the number of distinct values is not supported by this data model.");
};

void CardinalityEstimator::dump(std::ostream &out) const
{
    print(out);
    out << std::endl;
}

void CardinalityEstimator::dump() const { dump(std::cerr); }

const std::unordered_map<std::string, CardinalityEstimator::kind_t> CardinalityEstimator::STR_TO_KIND = {
#define DB_CARDINALITY_ESTIMATOR(NAME, _) { #NAME,  CardinalityEstimator::CE_ ## NAME },
#include "mutable/tables/CardinalityEstimator.tbl"
#undef DB_CARDINALITY_ESTIMATOR
};

std::unique_ptr<CardinalityEstimator>
CardinalityEstimator::Create(CardinalityEstimator::kind_t kind, const char *name_of_database) {
    switch(kind) {
#define DB_CARDINALITY_ESTIMATOR(NAME, _) case CE_ ## NAME: return Create ## NAME(name_of_database);
#include "mutable/tables/CardinalityEstimator.tbl"
#undef DB_CARDINALITY_ESTIMATOR
    }
}


/*======================================================================================================================
 * CartesianProductEstimator
 *====================================================================================================================*/

std::unique_ptr<DataModel> CartesianProductEstimator::empty_model() const
{
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = 0;
    return model;
}

std::unique_ptr<DataModel> CartesianProductEstimator::estimate_scan(const QueryGraph &G, Subproblem P) const
{
    insist(P.size() == 1, "Subproblem must identify exactly one DataSource");
    auto idx = *P.begin();
    auto DS = G.sources()[idx];

    if (auto BT = cast<const BaseTable>(DS)) {
        auto model = std::make_unique<CartesianProductDataModel>();
        model->size = BT->table().store().num_rows();
        return model;
    } else {
        unreachable("nested queries should be estimated when they are planned and their model must be passed to the "
                    "planning process of the outer query");
    }
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_filter(const QueryGraph &G, const DataModel &_data, const cnf::CNF&) const
{
    auto &data = as<const CartesianProductDataModel>(_data);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = data.size; // this model cannot estimate the effects of applying a filter
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_limit(const QueryGraph &G, const DataModel &_data, std::size_t limit,
                                          std::size_t offset) const
{
    auto data = as<const CartesianProductDataModel>(_data);
    const std::size_t remaining = offset > data.size ? 0UL : data.size - offset;
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = std::min(remaining, limit);
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_grouping(const QueryGraph &G, const DataModel &_data,
                                             const std::vector<const Expr*>&) const
{
    auto &data = as<const CartesianProductDataModel>(_data);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = data.size; // this model cannot estimate the effects of grouping
    return model;
}

std::unique_ptr<DataModel>
CartesianProductEstimator::estimate_join(const QueryGraph &G, const DataModel &_left, const DataModel &_right,
                                         const cnf::CNF&) const
{
    auto left = as<const CartesianProductDataModel>(_left);
    auto right = as<const CartesianProductDataModel>(_right);
    auto model = std::make_unique<CartesianProductDataModel>();
    model->size = left.size * right.size; // this model cannot estimate the effects of a join condition
    return model;
}

std::size_t CartesianProductEstimator::predict_cardinality(const DataModel &data) const
{
    return as<const CartesianProductDataModel>(data).size;
}

void CartesianProductEstimator::print(std::ostream &out) const
{
    out << "CartesianProductEstimator - returns size of the Cartesian product of the given subproblems";
}

std::unique_ptr<CardinalityEstimator>
CardinalityEstimator::CreateCartesianProductEstimator(const char*)
{
    return std::make_unique<CartesianProductEstimator>();
}


/*======================================================================================================================
 * InjectionCardinalityEstimator
 *====================================================================================================================*/

/*----- Constructors -------------------------------------------------------------------------------------------------*/

InjectionCardinalityEstimator::InjectionCardinalityEstimator() : InjectionCardinalityEstimator("default") {}

InjectionCardinalityEstimator::InjectionCardinalityEstimator(const char *name_of_database)
    : name_of_database_(name_of_database)
{
    Diagnostic diag(Options::Get().has_color, std::cout, std::cerr);
    Position pos("InjectionCardinalityEstimator");

    if (Options::Get().injected_cardinalities_file) {
        std::ifstream in(Options::Get().injected_cardinalities_file);
        if (in) {
            read_json(diag, in);
        } else {
            diag.w(pos) << "Could not open file " << Options::Get().injected_cardinalities_file << ".\n"
                        << "A dummy estimator will be used to do estimations.\n";
        }
    } else {
        std::cout << "No injection file was passed.\n";
    }
}

InjectionCardinalityEstimator::InjectionCardinalityEstimator(Diagnostic &diag, const char *name_of_database,
                                                             std::istream &in)
    : name_of_database_(name_of_database)
{
    read_json(diag, in);
}

InjectionCardinalityEstimator::~InjectionCardinalityEstimator()
{
    for (auto entry : cardinality_table_)
        free((void*) entry.first);
}

void InjectionCardinalityEstimator::read_json(Diagnostic &diag, std::istream &in)
{
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
        database_entry = &cardinalities.at(name_of_database_); //throws if key does not exist
    } catch (json::out_of_range &out_of_range) {
        diag.w(pos) << "No entry for the db " << name_of_database_ << " in the file.\n"
                    << "A dummy estimator will be used to do estimations.\n";
        return;
    }
    cardinality_table_.reserve(database_entry->size());
    for (auto &subproblem_entry : *database_entry) {
        json* relations_array;
        json* size;
        try {
            relations_array = &subproblem_entry.at("relations");
            size = &subproblem_entry.at("size");
        } catch (json::exception &exception) {
            diag.w(pos) << "The entry " << subproblem_entry << " for the db \"" << name_of_database_ << "\""
                        << " does not have the required form of {\"relations\": ..., \"size\": ... } "
                        << "and will thus be ignored.\n";
            continue;
        }

        buf_.clear();
        prev_relation.clear();
        try {
            for (auto it = relations_array->begin(); it != relations_array->end(); ++it) {
                std::string current(it->get<std::string>());
                if (current <= prev_relation) [[unlikely]]
                    throw std::invalid_argument("relations must be sorted lexicographically and must not contain duplicates");
                if (it != relations_array->begin())
                    buf_.emplace_back('$');
                buf_append(current);
                prev_relation = std::move(current);
            }
        } catch (std::invalid_argument) {
            diag.w(pos) << "Invalid identifier '" << std::string(buf_.cbegin(), buf_.cend())
                        << "', must contain relations in lexicographical order and must not contain duplicates.\n";
            continue;
        }
        buf_.emplace_back(0);
        auto str = strdup(buf_view());
        auto res = cardinality_table_.emplace(str, *size);
        insist(res.second, "insertion must not fail as we do not allow for duplicates in the input file");
    }
}

/*----- Model calculation --------------------------------------------------------------------------------------------*/

std::unique_ptr<DataModel> InjectionCardinalityEstimator::empty_model() const
{
    auto model = std::make_unique<InjectionCardinalityDataModel>();
    model->size_ = 0;
    return model;
}

std::unique_ptr<DataModel> InjectionCardinalityEstimator::estimate_scan(const QueryGraph &G, Subproblem P) const
{
    insist(P.size() == 1);
    auto model = std::make_unique<InjectionCardinalityDataModel>();
    const auto idx = *P.begin();
    auto DS = G.sources()[idx];

    if (DS->alias()) {
        model->relations_.push_back(DS->alias());
    } else {
        auto BT = as<const BaseTable>(DS);
        model->relations_.push_back(BT->table().name);
    }

    // Note: we bypass `make_identifier()` as we know that the model consists of a single relation
    if (auto it = cardinality_table_.find(model->relations_[0]); it != cardinality_table_.end()) {
        model->size_ = it->second;
    } else {
        /* no match, fall back */
        auto fallback_model = fallback_.estimate_scan(G, P);
        model->size_ = fallback_.predict_cardinality(*fallback_model);
    }

    return model;
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_filter(const QueryGraph &G, const DataModel &_data, const cnf::CNF&) const
{
    auto data = as<const InjectionCardinalityDataModel>(_data);
    auto model = std::make_unique<InjectionCardinalityDataModel>();
    model->relations_ = data.relations_;
    model->size_ = data.size_; // this model cannot estimate the effects of applying a filter
    return model;
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_limit(const QueryGraph &G, const DataModel &_data, std::size_t limit,
                                              std::size_t offset) const
{
    auto data = as<const InjectionCardinalityDataModel>(_data);
    const std::size_t remaining = offset > data.size_ ? 0UL : data.size_ - offset;
    auto model = std::make_unique<InjectionCardinalityDataModel>();
    model->size_ = std::min(remaining, limit);
    return model;
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_grouping(const QueryGraph &G, const DataModel &_data,
                                                 const std::vector<const Expr*> &exprs) const
{
    auto data = as<const InjectionCardinalityDataModel>(_data);

    auto model = std::make_unique<InjectionCardinalityDataModel>();
    model->relations_ = data.relations_;

    if (exprs.empty()) {
        model->size_ = 1;
        return model;
    }

    /* Combine grouping keys into an identifier. */
    oss_.str("");
    oss_ << "g";
    for (auto e : exprs)
        oss_ << '#' << *e;

    if (auto it = cardinality_table_.find(oss_.str().c_str()); it != cardinality_table_.end()) {
        model->size_ = it->second;
    } else {
        model->size_ = data.size_; // this model cannot estimate the effects of grouping
    }

    return model;
}

std::unique_ptr<DataModel>
InjectionCardinalityEstimator::estimate_join(const QueryGraph &G, const DataModel &_left, const DataModel &_right,
                                             const cnf::CNF &condition) const
{
    auto &left = as<const InjectionCardinalityDataModel>(_left);
    auto &right = as<const InjectionCardinalityDataModel>(_right);
    auto model = std::make_unique<InjectionCardinalityDataModel>();

    /* Merge the relations of `left` and `right` -- both sorted lexicographically -- into `model` and maintain
     * sortedness. */
    model->relations_.reserve(left.relations_.size() + right.relations_.size());
    std::merge(left.relations_.begin(), left.relations_.end(),
               right.relations_.begin(), right.relations_.end(),
               std::back_inserter(model->relations_),
               [](const char *left, const char *right) { return strcmp(left, right) < 0; });
    insist(std::is_sorted(model->relations_.begin(), model->relations_.end(),
                          [](const char *left, const char *right) { return strcmp(left, right) < 0; }),
           "relations must be sorted lexicographically");

    const char *id = make_identifier(*model);
    if (auto it = cardinality_table_.find(id); it != cardinality_table_.end()) {
        /* Lookup cardinality in table. */
        model->size_ = it->second;
    } else {
        /* Fallback to CartesianProductEstimator. */
        auto left_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
        left_fallback->size = left.size_;
        auto right_fallback = std::make_unique<CartesianProductEstimator::CartesianProductDataModel>();
        right_fallback->size = right.size_;
        auto fallback_model = fallback_.estimate_join(G, *left_fallback, *right_fallback, condition);
        model->size_ = fallback_.predict_cardinality(*fallback_model);
    }
    return model;
}

std::size_t InjectionCardinalityEstimator::predict_cardinality(const DataModel &data) const
{
    return as<const InjectionCardinalityDataModel>(data).size_;
}

void InjectionCardinalityEstimator::print(std::ostream &out) const
{
    constexpr uint32_t max_rows_printed = 100;     /// Number of rows of the cardinality_table printed
    std::size_t sub_len = 13;                         /// Length of Subproblem column
    for (auto &entry : cardinality_table_)
        sub_len = std::max(sub_len, strlen(entry.first));

    out << std::left << "InjectionCardinalityEstimator for the Database: " << name_of_database_ << "\n"
        << std::setw(sub_len) << "Subproblem" << "Size" << "\n" << std::right;

    /* ------- Print maximum max_rows_printed rows of the cardinality_table_ */
    uint32_t counter = 0;
    for (auto &entry : cardinality_table_) {
        if (counter >= max_rows_printed) break;
        out << std::left << std::setw(sub_len) << entry.first << entry.second << "\n";
        counter++;
    }
}

const char * InjectionCardinalityEstimator::make_identifier(const InjectionCardinalityDataModel &model) const
{
    buf_.clear();
    for (auto it = model.relations_.begin(); it != model.relations_.end(); ++it) {
        if (it != model.relations_.begin())
            buf_.emplace_back('$');
        buf_append(*it);
    }
    buf_.emplace_back(0);
    return buf_view();
}

std::unique_ptr<CardinalityEstimator>
CardinalityEstimator::CreateInjectionCardinalityEstimator(const char *name_of_database)
{
    return std::make_unique<InjectionCardinalityEstimator>(name_of_database);
}
