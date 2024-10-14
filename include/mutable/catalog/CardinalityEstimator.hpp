#pragma once

#include <fstream>
#include <iostream>
#include <mutable/mutable-config.hpp>
#include <mutable/util/ADT.hpp>
#include <mutable/util/crtp.hpp>
#include <mutable/util/Pool.hpp>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace m {

namespace ast {

struct Expr;

}

namespace cnf { struct CNF; }
struct Diagnostic;
struct GroupingOperator;
struct LimitOperator;
struct Operator;
struct PlanTableLargeAndSparse;
struct PlanTableSmallOrDense;
struct QueryGraph;
struct SpnWrapper;

using Subproblem = SmallBitset;


/** A `DataModel` describes a data set.
 *
 * A data set is usually the result of evaluating a subplan.  The `DataModel` describes this result.  The way how
 * the data is described depends on the actual kind of model.  A very simplistic model may only describe the upper
 * limit of the tuples in the data set.  More sophisticated models may express statistical information, such as
 * correlation of attributes and frequency of individual values.
 */
struct M_EXPORT DataModel
{
    virtual ~DataModel() = 0;

    /** Assigns `this` to the `Subproblem` `s`, i.e. this model now describes the result of evaluating `s`. */
    virtual void assign_to(Subproblem s) = 0;

};


struct M_EXPORT estimate_join_all_tag : const_virtual_crtp_helper<estimate_join_all_tag>::
    returns<std::unique_ptr<DataModel>>::
    crtp_args<const PlanTableSmallOrDense&, const PlanTableLargeAndSparse&>::
    args<const QueryGraph&, Subproblem, const cnf::CNF&> { };

struct M_EXPORT CardinalityEstimator : estimate_join_all_tag::base_type
{
    using estimate_join_all_tag::base_type::operator();
    using group_type = std::pair<std::reference_wrapper<const ast::Expr>, ThreadSafePooledOptionalString>;

    /** `data_model_exception` is thrown if a `DataModel` implementation does not contain the requested information. */
    struct data_model_exception : m::exception
    {
        explicit data_model_exception(std::string message) : m::exception(std::move(message)) { }
    };

    virtual ~CardinalityEstimator() = 0;


    /*==================================================================================================================
     * Model calculation
     *================================================================================================================*/

    /** Returns a `DataModel` representing the empty set. */
    virtual std::unique_ptr<DataModel> empty_model() const = 0;

    /** Returns a `DataModel` containing the same data as the passed one.
     * @param model     the `DataModel` describing the model to be copied
     * @return          the `DataModel` describing the copy
     */
    virtual std::unique_ptr<DataModel> copy(const DataModel &data) const = 0;

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
    estimate_grouping(const QueryGraph &G, const DataModel &data, const std::vector<group_type> &groups) const = 0;

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

    /** Form a new `DataModel` by computing the left semi join between the left and the right `DataModel`.
    *
    * @param left      the `DataModel` describing the data coming from the left input
    * @param right     the `DataModel` describing the data coming from the right input
    * @param condition the join condition as `cnf::CNF`
    * @return          the `DataModel` describing the semi-join result
    */
    virtual std::unique_ptr<DataModel>
    estimate_semi_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const = 0;

    /** Form a new `DataModel` by computing the size of the given `DataModel` after it was reduced by all its neighbors`.
    *
    * @param model     the `DataModel` describing the model from the model to be reduced
    * @param model     the `Subproblem` describing which reducation to ignore
    * @return          the `DataModel` describing the fully reduced result
    */
    virtual std::unique_ptr<DataModel>
    estimate_full_reduction(const QueryGraph &G, const DataModel &model, Subproblem except = Subproblem()) const = 0;

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

M_LCOV_EXCL_START
    friend std::ostream & operator<<(std::ostream &out, const CardinalityEstimator &CE) {
        CE.print(out);
        return out;
    }
M_LCOV_EXCL_STOP

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
struct M_EXPORT CartesianProductEstimator : CardinalityEstimatorCRTP<CartesianProductEstimator>
{
    struct CartesianProductDataModel : DataModel
    {
        std::size_t size;

        CartesianProductDataModel() = default;
        CartesianProductDataModel(std::size_t size) : size(size) { }

        void assign_to(Subproblem) override { /* nothing to be done */ }

    };

    CartesianProductEstimator() { }
    CartesianProductEstimator(ThreadSafePooledString) { }


    /*==================================================================================================================
     * Model calculation
     *================================================================================================================*/

    std::unique_ptr<DataModel> empty_model() const override;
    std::unique_ptr<DataModel> copy(const DataModel &data) const override;
    std::unique_ptr<DataModel> estimate_scan(const QueryGraph &G, Subproblem P) const override;
    std::unique_ptr<DataModel>
    estimate_filter(const QueryGraph &G, const DataModel &data, const cnf::CNF &filter) const override;
    std::unique_ptr<DataModel>
    estimate_limit(const QueryGraph &G, const DataModel &data, std::size_t limit, std::size_t offset) const override;
    std::unique_ptr<DataModel>
    estimate_grouping(const QueryGraph &G, const DataModel &data, const std::vector<group_type> &groups) const override;
    std::unique_ptr<DataModel>
    estimate_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const override;
    std::unique_ptr<DataModel>
    estimate_semi_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const override;
    std::unique_ptr<DataModel>
    estimate_full_reduction(const QueryGraph &G, const DataModel &model, Subproblem except = Subproblem()) const override;

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
struct M_EXPORT InjectionCardinalityEstimator : CardinalityEstimatorCRTP<InjectionCardinalityEstimator>
{
    using Subproblem = SmallBitset;

    struct InjectionCardinalityDataModel : DataModel
    {
        friend struct InjectionCardinalityEstimator;

        private:
        Subproblem subproblem_;
        Subproblem reduced_by_ = Subproblem();
        std::size_t size_;

        public:
        InjectionCardinalityDataModel(Subproblem S, std::size_t size) : subproblem_(S), size_(size) { }
        InjectionCardinalityDataModel(Subproblem S, Subproblem R, std::size_t size) : subproblem_(S), reduced_by_(R), size_(size) { }
        InjectionCardinalityDataModel(const InjectionCardinalityDataModel&) = default;
        InjectionCardinalityDataModel(InjectionCardinalityDataModel&&) = default;
        InjectionCardinalityDataModel & operator=(InjectionCardinalityDataModel &&other) = default;
        InjectionCardinalityDataModel & operator=(const InjectionCardinalityDataModel &other) = default;

        void assign_to(Subproblem s) override { subproblem_ = s; }
    };

    private:
    ///> buffer used to construct identifiers
    mutable std::vector<char> buf_;
    ///> buffer used to construct identifiers
    mutable std::ostringstream oss_;

    struct CardinalityEntry {
        std::size_t size;
        std::unordered_map<ThreadSafePooledString, std::size_t> semi_join_table;
    };

    std::unordered_map<ThreadSafePooledString, CardinalityEntry> cardinality_table_;
    CartesianProductEstimator fallback_;

    public:
    /** Create an `InjectionCardinalityEstimator` for the database `name_of_database` from file that was passed by the
     * user via commandline, saved in `Options::Get().injected_cardinalities_file`
     *
     * @param name_of_database the name of the database to create the `InjectionCardinalityEstimator` for
     */
    InjectionCardinalityEstimator(ThreadSafePooledString name_of_database);

    /** Create an `InjectionCardinalityEstimator` for the database `name_of_database` from the inputstream `in`
     *
     * @param name_of_database the name of the database to create the `InjectionCardinalityEstimator` for
     * @param in inputstream containing the injected cardinalities in JSON format
     */
    InjectionCardinalityEstimator(Diagnostic &diag, ThreadSafePooledString name_of_database, std::istream &in);

    ~InjectionCardinalityEstimator() = default;

    InjectionCardinalityEstimator(const InjectionCardinalityEstimator&) = delete;
    InjectionCardinalityEstimator(InjectionCardinalityEstimator&&) = default;

    InjectionCardinalityEstimator & operator=(InjectionCardinalityEstimator&&) = default;


    /*==================================================================================================================
     * Model calculation
     *================================================================================================================*/

    std::unique_ptr<DataModel> empty_model() const override;
    std::unique_ptr<DataModel> copy(const DataModel &data) const override;
    std::unique_ptr<DataModel> estimate_scan(const QueryGraph &G, Subproblem P) const override;
    std::unique_ptr<DataModel>
    estimate_filter(const QueryGraph &G, const DataModel &data, const cnf::CNF &filter) const override;
    std::unique_ptr<DataModel>
    estimate_limit(const QueryGraph &G, const DataModel &data, std::size_t limit, std::size_t offset) const override;
    std::unique_ptr<DataModel>
    estimate_grouping(const QueryGraph &G, const DataModel &data, const std::vector<group_type> &groups) const override;
    std::unique_ptr<DataModel>
    estimate_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const override;
    std::unique_ptr<DataModel>
    estimate_semi_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const override;
    std::unique_ptr<DataModel>
    estimate_full_reduction(const QueryGraph &G, const DataModel &model, Subproblem except = Subproblem()) const override;

    template<typename PlanTable>
    std::unique_ptr<DataModel>
    operator()(estimate_join_all_tag, PlanTable &&PT, const QueryGraph &G, Subproblem to_join,
               const cnf::CNF &condition) const;

    /*==================================================================================================================
     * Prediction via model use
     *================================================================================================================*/

    std::size_t predict_cardinality(const DataModel &data) const override;

    private:
    void read_json(Diagnostic &diag, std::istream &in, const ThreadSafePooledString &name_of_database);
    void print(std::ostream &out) const override;
    void buf_append(const char *s) const { while (*s) buf_.emplace_back(*s++); }
    void buf_append(const std::string &s) const {
        buf_.reserve(buf_.size() + s.size());
        buf_append(s.c_str());
    }
    const char * buf_view() const { return buf_.data(); }
    ThreadSafePooledString make_identifier(const QueryGraph &G, const Subproblem S) const;
};

/**
 * SpnEstimator that estimates cardinalities based on Sum-Product Networks.
 */
struct M_EXPORT SpnEstimator : CardinalityEstimatorCRTP<SpnEstimator>
{
    using SpnIdentifier = std::pair<ThreadSafePooledString, ThreadSafePooledString>;
    using SpnJoin = std::pair<SpnIdentifier, SpnIdentifier>;
    using table_spn_map = std::unordered_map<ThreadSafePooledString, std::reference_wrapper<const SpnWrapper>>;

    struct SpnDataModel : DataModel
    {
        friend struct SpnEstimator;

        private:
        table_spn_map spns_; ///< a map from table to Spn
        std::vector<std::size_t> max_frequencies_; ///< the maximum frequencies of values of attributes to join
        std::size_t num_rows_;

        public:
        SpnDataModel() = default;
        SpnDataModel(table_spn_map spns, std::size_t num_rows)
            : spns_(std::move(spns))
            , num_rows_(num_rows)
        { }

        void assign_to(Subproblem) override { /* nothing to be done */ }
    };

    private:
    ///> the map from every table to its respective Spn, initially empty
    std::unordered_map<ThreadSafePooledString, SpnWrapper*> table_to_spn_;
    ///> the name of the database, the estimator is built on
    ThreadSafePooledString name_of_database_;

    public:
    explicit SpnEstimator(ThreadSafePooledString name_of_database) : name_of_database_(std::move(name_of_database)) { }

    ~SpnEstimator();

    /** Learn an Spn on every table in the database. Also used to initialize spns after data inserted in tables. */
    void learn_spns();

    /** Add a new Spn for a table in the database. */
    void learn_new_spn(const ThreadSafePooledString &name_of_table);

    private:
    /** Function to compute which of the two join identifiers belongs to the given data model and which attribute to choose.
     *
     * @param data  the data model
     * @param join  the join condition as a pair of identifiers
     * @return      a pair of the spn internal id of the attribute and whether the attribute is a primary key
     */
    static std::pair<unsigned, bool> find_spn_id(const SpnDataModel &data, SpnJoin &join);

    /** Compute the maximum frequency of values of the attribute in the join.
     *
     * @param data  the data model
     * @param join  the join condition as a pair of identifiers
     * @return      the maximum frequency of the values of the attribute
     */
    static std::size_t max_frequency(const SpnDataModel &data, SpnJoin &join);

    /** Compute the maximum frequency of values of the attribute .
     *
     * @param data      the data model
     * @param attribute the attribute
     * @return          the maximum frequency of the values of the attribute
     */
    static std::size_t max_frequency(const SpnDataModel &data, const ThreadSafePooledString &attribute);

    /*==================================================================================================================
     * Model calculation
     *================================================================================================================*/

    public:
    std::unique_ptr<DataModel> empty_model() const override;
    std::unique_ptr<DataModel> copy(const DataModel &data) const override;
    std::unique_ptr<DataModel> estimate_scan(const QueryGraph &G, Subproblem P) const override;
    std::unique_ptr<DataModel>
    estimate_filter(const QueryGraph &G, const DataModel &data, const cnf::CNF &filter) const override;
    std::unique_ptr<DataModel>
    estimate_limit(const QueryGraph &G, const DataModel &data, std::size_t limit, std::size_t offset) const override;
    std::unique_ptr<DataModel>
    estimate_grouping(const QueryGraph &G, const DataModel &data, const std::vector<group_type> &groups) const override;
    std::unique_ptr<DataModel>
    estimate_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const override;
    std::unique_ptr<DataModel>
    estimate_semi_join(const QueryGraph &G, const DataModel &left, const DataModel &right,
                  const cnf::CNF &condition) const override;
    std::unique_ptr<DataModel>
    estimate_full_reduction(const QueryGraph &G, const DataModel &model, Subproblem except = Subproblem()) const override;

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

}
