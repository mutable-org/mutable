#pragma once

#include <fstream>
#include <iostream>
#include <mutable/util/ADT.hpp>
#include <nlohmann/json.hpp>
#include <unordered_map>


namespace m {

/* Forward declarations */
struct QueryGraph;
struct Expr;
struct Diagnostic;
struct Operator;
struct LimitOperator;
struct GroupingOperator;
namespace cnf { struct CNF; }
using Subproblem = SmallBitset;

struct CardinalityEstimator
{
    /** A `DataModel` describes a data set.
     *
     * A data set is usually the result of evaluating a subplan.  The `DataModel` describes this result.  The way how
     * the data is descibed depends on the actual kind of model.  A very simplistic model may only descibe the upper
     * limit of the tuples in the data set.  More sophisticated models may express statistical information, such as
     * correlation of attributes and frequency of individual values.
     */
    struct DataModel
    {
        virtual ~DataModel() = 0;
    };


    /** `data_model_exception` is thrown if a `DataModel` implementation does not contain the requested information. */
    struct data_model_exception : m::exception
    {
        private:
        const std::string message_;

        public:
        data_model_exception(const std::string &message) : message_(message) { }

        const char* what() const noexcept override { return message_.c_str(); }
    };

    enum kind_t {
#define DB_CARDINALITY_ESTIMATOR(NAME, _) CE_ ## NAME,
#include "mutable/tables/CardinalityEstimator.tbl"
#undef DB_CARDINALITY_ESTIMATOR
    };

    static const std::unordered_map<std::string, kind_t> STR_TO_KIND;

    /** Create a `CardinalityEstimator` instance given the kind of cardinality estimator. */
    static std::unique_ptr<CardinalityEstimator> Create(kind_t kind, const char *name_of_database);

    /** Create a `CardinalityEstimator` instance given the name of a cardinality estimator. */
    static std::unique_ptr<CardinalityEstimator> Create(const char *kind, const char *name_of_database) {
        return Create(STR_TO_KIND.at(kind), name_of_database);
    }

#define DB_CARDINALITY_ESTIMATOR(NAME, _) \
    static std::unique_ptr<CardinalityEstimator> Create ## NAME(const char *name_of_database);
#include "mutable/tables/CardinalityEstimator.tbl"
#undef DB_CARDINALITY_ESTIMATOR

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
    virtual std::unique_ptr<DataModel> estimate_filter(const DataModel &data, const cnf::CNF &filter) const = 0;

    /** Extracts a subset from a `DataModel`.
     *
     * @param data      the `DataModel` describing the incoming data
     * @param limit     the number of result tuples to extract
     * @param offset    the offset of the first result tuple to extract
     * @return          the estimated size of the limit result when applying `op` on `P`
     */
    virtual std::unique_ptr<DataModel>
    estimate_limit(const DataModel &data, const std::size_t limit, const std::size_t offset) const = 0;

    /** Groups data in the `DataModel`.
     *
     * @param data      the `DataModel` describing the incoming data
     * @param groups    a collection of `Expr`s to group by
     * @return          the `DataModel` describing the grouped data
     */
    virtual std::unique_ptr<DataModel>
    estimate_grouping(const DataModel &data, const std::vector<const Expr*> &groups) const = 0;

    /** From a new `DataModel` from joining two `DataModel`s.
     *
     * @param left      the `DataModel` describing the data coming from the left input
     * @param right     the `DataModel` describing the data coming from the right input
     * @param condition the join condition as `cnf::CNF`
     * @return          the `DataModel` describing the join result
     */
    virtual std::unique_ptr<DataModel>
    estimate_join(const DataModel &left, const DataModel &right, const cnf::CNF &condition) const = 0;


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

/**
 * DummyEstimator that always returns the size of the cartesian product of the given subproblems
 */
struct CartesianProductEstimator : CardinalityEstimator
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
    std::unique_ptr<DataModel> estimate_filter(const DataModel &data, const cnf::CNF &filter) const override;
    std::unique_ptr<DataModel>
    estimate_limit(const DataModel &data, std::size_t limit, std::size_t offset) const override;
    std::unique_ptr<DataModel>
    estimate_grouping(const DataModel &data, const std::vector<const Expr*> &groups) const override;
    std::unique_ptr<DataModel>
    estimate_join(const DataModel &left, const DataModel &right, const cnf::CNF &condition) const override;


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
struct InjectionCardinalityEstimator : CardinalityEstimator
{
    struct InjectionCardinalityDataModel : DataModel
    {
        friend struct InjectionCardinalityEstimator;

        private:
        std::vector<const char*> relations_; // collection of relations, sorted by name
        std::size_t size_;
    };

    private:


    const char *name_of_database_;
    std::unordered_map<std::string, std::size_t> cardinality_table_;
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


    /*==================================================================================================================
     * Model calculation
     *================================================================================================================*/

    std::unique_ptr<DataModel> empty_model() const override;
    std::unique_ptr<DataModel> estimate_scan(const QueryGraph &G, Subproblem P) const override;
    std::unique_ptr<DataModel> estimate_filter(const DataModel &data, const cnf::CNF &filter) const override;
    std::unique_ptr<DataModel>
    estimate_limit(const DataModel &data, std::size_t limit, std::size_t offset) const override;
    std::unique_ptr<DataModel>
    estimate_grouping(const DataModel &data, const std::vector<const Expr*> &groups) const override;
    std::unique_ptr<DataModel>
    estimate_join(const DataModel &left, const DataModel &right, const cnf::CNF &condition) const override;


    /*==================================================================================================================
     * Prediction via model use
     *================================================================================================================*/

    std::size_t predict_cardinality(const DataModel &data) const override;

    private:
    void print(std::ostream &out) const override;
};

}
