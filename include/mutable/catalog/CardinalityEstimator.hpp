#pragma once

#include <fstream>
#include <iostream>
#include <mutable/util/ADT.hpp>
#include <mutable/util/crtp.hpp>
#include <nlohmann/json.hpp>
#include <sstream>
#include <unordered_map>
#include <vector>


namespace m {

/* Forward declarations */
namespace cnf { struct CNF; }
struct Diagnostic;
struct Expr;
struct GroupingOperator;
struct LimitOperator;
struct Operator;
struct PlanTableLargeAndSparse;
struct PlanTableSmallOrDense;
struct QueryGraph;

using Subproblem = SmallBitset;


/** A `DataModel` describes a data set.
 *
 * A data set is usually the result of evaluating a subplan.  The `DataModel` describes this result.  The way how
 * the data is described depends on the actual kind of model.  A very simplistic model may only describe the upper
 * limit of the tuples in the data set.  More sophisticated models may express statistical information, such as
 * correlation of attributes and frequency of individual values.
 */
struct DataModel
{
    virtual ~DataModel() = 0;
};


struct estimate_join_all_tag : const_virtual_crtp_helper<estimate_join_all_tag>::
    returns<std::unique_ptr<DataModel>>::
    crtp_args<const PlanTableSmallOrDense&, const PlanTableLargeAndSparse&>::
    args<const QueryGraph&, Subproblem, const cnf::CNF&> { };

struct CardinalityEstimator : estimate_join_all_tag::base_type
{
    using estimate_join_all_tag::base_type::operator();

    /** `data_model_exception` is thrown if a `DataModel` implementation does not contain the requested information. */
    struct data_model_exception : m::exception
    {
        explicit data_model_exception(std::string message) : m::exception(std::move(message)) { }
    };

    enum kind_t {
#define M_CARDINALITY_ESTIMATOR(NAME, _) CE_ ## NAME,
#include <mutable/tables/CardinalityEstimator.tbl>
#undef M_CARDINALITY_ESTIMATOR
    };

    static const std::unordered_map<std::string, kind_t> STR_TO_KIND;

    /** Create a `CardinalityEstimator` instance given the kind of cardinality estimator. */
    static std::unique_ptr<CardinalityEstimator> Create(kind_t kind, const char *name_of_database);

    /** Create a `CardinalityEstimator` instance given the name of a cardinality estimator. */
    static std::unique_ptr<CardinalityEstimator> Create(const char *kind, const char *name_of_database) {
        return Create(STR_TO_KIND.at(kind), name_of_database);
    }

#define M_CARDINALITY_ESTIMATOR(NAME, _) \
    static std::unique_ptr<CardinalityEstimator> Create ## NAME(const char *name_of_database);
#include <mutable/tables/CardinalityEstimator.tbl>
#undef M_CARDINALITY_ESTIMATOR

    virtual ~CardinalityEstimator() = 0;


    /*==================================================================================================================
     * Model calculation
     *================================================================================================================*/

    /** Returns a `DataModel` representing the empty set. */
    virtual std::unique_ptr<DataModel> empty_model() const = 0;

    /** Creates a `DataModel` for a single `DataSource`.
     *
     * @param G         the `QueryGraph` of the query
     * @param P         the `Subproblem` to be scanned, must identify a single `DataSource`
     * @return          a `DataModel` of the scan result
     */
    virtual std::unique_ptr<DataModel> estimate_scan(const QueryGraph &G, Subproblem P) const = 0;

    /** Applies a filter to a `DataModel`.
     *
     * @param data      the `DataModel` describing the incoming data
     * @param filter    the condition of the filter as `cnf::CNF`
     * @return          the `DataModel` describing the outcoming, i.e. filtered, data
     */
    virtual std::unique_ptr<DataModel> estimate_filter(const QueryGraph &G, const DataModel &data,
                                                       const cnf::CNF &filter) const = 0;

    /** Extracts a subset from a `DataModel`.
     *
     * @param data      the `DataModel` describing the incoming data
     * @param limit     the number of result tuples to extract
     * @param offset    the offset of the first result tuple to extract
     * @return          the estimated size of the limit result when applying `op` on `P`
     */
    virtual std::unique_ptr<DataModel>
    estimate_limit(const QueryGraph &G, const DataModel &data, const std::size_t limit,
                   const std::size_t offset) const = 0;

    /** Groups data in the `DataModel`.
     *
     * @param data      the `DataModel` describing the incoming data
     * @param groups    a collection of `Expr`s to group by
     * @return          the `DataModel` describing the grouped data
     */
    virtual std::unique_ptr<DataModel>
    estimate_grouping(const QueryGraph &G, const DataModel &data, const std::vector<const Expr*> &groups) const = 0;

    /** Form a new `DataModel` by joining two `DataModel`s.
     *
     * @param left      the `DataModel` describing the data coming from the left input
     * @param right     the `DataModel` describing the data coming from the right input
     * @param condition the join condition as `cnf::CNF`
     * @return          the `DataModel` describing the join result
     */
    virtual std::unique_ptr<DataModel>
    estimate_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const = 0;

    /** Compute a `DataModel` for the result of joining *all* `DataSource`s in `to_join` by `condition`. */
    template<typename PlanTable>
    std::unique_ptr<DataModel>
    estimate_join_all(const QueryGraph &G, const PlanTable &PT, Subproblem to_join, const cnf::CNF &condition) const
    {
        return operator()(estimate_join_all_tag{}, PT, G, to_join, condition);
    }


    /*==================================================================================================================
     * Prediction via model use
     *================================================================================================================*/

    virtual std::size_t predict_cardinality(const DataModel &data) const = 0;

    virtual double predict_number_distinct_values(const DataModel &data) const;

    /*==================================================================================================================
     * other methods
     *================================================================================================================*/

    friend std::ostream & operator<<(std::ostream &out, const CardinalityEstimator &CE) {
        CE.print(out);
        return out;
    }

    void dump(std::ostream &out) const;

    void dump() const;

    protected:
    virtual void print(std::ostream &out) const = 0;
};

namespace {

template<typename Actual>
struct CardinalityEstimatorCRTP : CardinalityEstimator
                                , estimate_join_all_tag::derived_type<Actual>
{ };

}

/**
 * DummyEstimator that always returns the size of the cartesian product of the given subproblems
 */
struct CartesianProductEstimator : CardinalityEstimatorCRTP<CartesianProductEstimator>
{
    struct CartesianProductDataModel : DataModel
    {
        std::size_t size;

        CartesianProductDataModel() = default;
        CartesianProductDataModel(std::size_t size) : size(size) { }
    };

    CartesianProductEstimator() { };


    /*==================================================================================================================
     * Model calculation
     *================================================================================================================*/

    std::unique_ptr<DataModel> empty_model() const override;
    std::unique_ptr<DataModel> estimate_scan(const QueryGraph &G, Subproblem P) const override;
    std::unique_ptr<DataModel>
    estimate_filter(const QueryGraph &G, const DataModel &data, const cnf::CNF &filter) const override;
    std::unique_ptr<DataModel>
    estimate_limit(const QueryGraph &G, const DataModel &data, std::size_t limit, std::size_t offset) const override;
    std::unique_ptr<DataModel>
    estimate_grouping(const QueryGraph &G, const DataModel &data,
                      const std::vector<const Expr*> &groups) const override;
    std::unique_ptr<DataModel>
    estimate_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const override;

    template<typename PlanTable>
    std::unique_ptr<DataModel>
    operator()(estimate_join_all_tag, PlanTable &&PT, const QueryGraph &G, Subproblem to_join,
               const cnf::CNF &condition) const;


    /*==================================================================================================================
     * Prediction via model use
     *================================================================================================================*/

    std::size_t predict_cardinality(const DataModel &data) const override;

    private:
    void print(std::ostream &out) const override;
};

/**
 * InjectionCardinalityEstimator that estimates cardinalities based on a table that contains sizes for the given
 * subproblems
 * Table is initialized in the constructor by using an external json-file
 * If no entry is found, uses the DummyEstimator to return an estimate
 */
struct InjectionCardinalityEstimator : CardinalityEstimatorCRTP<InjectionCardinalityEstimator>
{
    using Subproblem = SmallBitset;

    struct InjectionCardinalityDataModel : DataModel
    {
        friend struct InjectionCardinalityEstimator;

        private:
        Subproblem subproblem_;
        std::size_t size_;

        public:
        InjectionCardinalityDataModel(Subproblem S, std::size_t size) : subproblem_(S), size_(size) { }
        InjectionCardinalityDataModel(const InjectionCardinalityDataModel&) = default;
        InjectionCardinalityDataModel(InjectionCardinalityDataModel&&) = default;
        InjectionCardinalityDataModel & operator=(InjectionCardinalityDataModel &&other) = default;
        InjectionCardinalityDataModel & operator=(const InjectionCardinalityDataModel &other) = default;
    };

    private:
    ///> buffer used to construct identifiers
    mutable std::vector<char> buf_;
    ///> buffer used to construct identifiers
    mutable std::ostringstream oss_;

    std::unordered_map<const char*, std::size_t, StrHash, StrEqual> cardinality_table_;
    CartesianProductEstimator fallback_;

    public:
    /** Create an `InjectionCardinalityEstimator` for the database "default" */
    InjectionCardinalityEstimator();

    /** Create an `InjectionCardinalityEstimator` for the database `name_of_database` from file that was passed by the
     * user via commandline, saved in `Options::Get().injected_cardinalities_file`
     *
     * @param name_of_database the name of the database to create the `InjectionCardinalityEstimator` for
     */
    InjectionCardinalityEstimator(const char *name_of_database);

    /** Create an `InjectionCardinalityEstimator` for the database `name_of_database` from the inputstream `in`
     *
     * @param name_of_database the name of the database to create the `InjectionCardinalityEstimator` for
     * @param in inputstream containing the injected cardinalities in JSON format
     */
    InjectionCardinalityEstimator(Diagnostic &diag, const char *name_of_database, std::istream &in);

    ~InjectionCardinalityEstimator();

    InjectionCardinalityEstimator(const InjectionCardinalityEstimator&) = delete;
    InjectionCardinalityEstimator(InjectionCardinalityEstimator&&) = default;

    InjectionCardinalityEstimator & operator=(InjectionCardinalityEstimator&&) = default;


    /*==================================================================================================================
     * Model calculation
     *================================================================================================================*/

    std::unique_ptr<DataModel> empty_model() const override;
    std::unique_ptr<DataModel> estimate_scan(const QueryGraph &G, Subproblem P) const override;
    std::unique_ptr<DataModel>
    estimate_filter(const QueryGraph &G, const DataModel &data, const cnf::CNF &filter) const override;
    std::unique_ptr<DataModel>
    estimate_limit(const QueryGraph &G, const DataModel &data, std::size_t limit, std::size_t offset) const override;
    std::unique_ptr<DataModel>
    estimate_grouping(const QueryGraph &G, const DataModel &data,
                      const std::vector<const Expr*> &groups) const override;
    std::unique_ptr<DataModel>
    estimate_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const override;

    template<typename PlanTable>
    std::unique_ptr<DataModel>
    operator()(estimate_join_all_tag, PlanTable &&PT, const QueryGraph &G, Subproblem to_join,
               const cnf::CNF &condition) const;

    /*==================================================================================================================
     * Prediction via model use
     *================================================================================================================*/

    std::size_t predict_cardinality(const DataModel &data) const override;

    private:
    void read_json(Diagnostic &diag, std::istream &in, const char *name_of_database);
    void print(std::ostream &out) const override;
    void buf_append(const char *s) const { while (*s) buf_.emplace_back(*s++); }
    void buf_append(const std::string &s) const {
        buf_.reserve(buf_.size() + s.size());
        buf_append(s.c_str());
    }
    const char * buf_view() const { return &buf_[0]; }
    const char * make_identifier(const QueryGraph &G, const Subproblem S) const;
};

}
