#pragma once

#include <mutable/util/Pool.hpp>
#include <unordered_map>
#include <util/Spn.hpp>
#include <vector>


namespace m {

/** A wrapper class for an Spn to be used in the context of databases. */
struct SpnWrapper
{
    using Filter = std::unordered_map<unsigned, std::pair<Spn::SpnOperator, float>>;
    using AttrFilter = std::unordered_map<ThreadSafePooledString, std::pair<Spn::SpnOperator, float>>;

    private:
    Spn spn_;
    std::unordered_map<ThreadSafePooledString, unsigned> attribute_to_id_; ///< a map from attribute to spn internal id

    SpnWrapper(Spn spn, std::unordered_map<ThreadSafePooledString, unsigned> attribute_to_id)
        : spn_(std::move(spn))
        , attribute_to_id_(std::move(attribute_to_id))
    { }

    Filter translate_filter(const AttrFilter &attr_filter) const {
        Filter filter;
        for (auto &elem : attr_filter) { filter.emplace(translate_attribute(elem.first), elem.second); }
        return filter;
    };

    unsigned translate_attribute(const ThreadSafePooledString &attribute) const {
        unsigned spn_id = 0;
        if (auto it = attribute_to_id_.find(attribute); it != attribute_to_id_.end()) {
            spn_id = it->second;
        } else { std::cerr << "could not find attribute: " << attribute << std::endl; }
        return spn_id;
    }

    public:
    SpnWrapper(const SpnWrapper&) = delete;
    SpnWrapper(SpnWrapper&&) = default;

    /** Get the reference to the attribute to spn internal id mapping. */
    const std::unordered_map<ThreadSafePooledString, unsigned> & get_attribute_to_id() const { return attribute_to_id_; }

    /** Learn an SPN over the given table.
     *
     * @param name_of_database  the database
     * @param name_of_table     the table in the database
     * @param leaf_types        the types of a leaf for a non-primary key attribute
     * @return                  the learned SPN
     */
    static SpnWrapper learn_spn_table(const ThreadSafePooledString &name_of_database,
                                      const ThreadSafePooledString &name_of_table,
                                      std::vector<Spn::LeafType> leaf_types = decltype(leaf_types)());

    /** Learn SPNs over the tables in the given database.
     *
     * @param name_of_database  the database
     * @param leaf_types        the type of a leaf for a non-primary key attribute in the respective table
     * @return                  the learned SPNs
     */
    static std::unordered_map<ThreadSafePooledString, SpnWrapper*>
    learn_spn_database(
        const ThreadSafePooledString &name_of_database,
        std::unordered_map<ThreadSafePooledString, std::vector<Spn::LeafType>> leaf_types = decltype(leaf_types)()
    );


    /** returns the number of rows in the SPN. */
    std::size_t num_rows() const { return spn_.num_rows(); }

    /** Compute the likelihood of the given filter predicates given by a map from attribute to the
     * respective operator and value. The predicates in the map are seen as conjunctions. */
    float likelihood(const AttrFilter &attr_filter) const { return spn_.likelihood(translate_filter(attr_filter)); };
    /** Compute the likelihood of the given filter predicates given by a map from spn internal id to the
     * respective operator and value. The predicates in the map are seen as conjunctions. */
    float likelihood(const Filter &filter) const { return spn_.likelihood(filter); };

    /** Compute the upper bound probability for continuous domains. */
    float upper_bound(const AttrFilter &attr_filter) const { return spn_.upper_bound(translate_filter(attr_filter)); };
    /** Compute the upper bound probability for continuous domains. */
    float upper_bound(const Filter &filter) const { return spn_.upper_bound(filter); };

    /** Compute the lower bound probability for continuous domains. */
    float lower_bound(const AttrFilter &attr_filter) const { return spn_.lower_bound(translate_filter(attr_filter)); };
    /** Compute the lower bound probability for continuous domains. */
    float lower_bound(const Filter &filter) const { return spn_.lower_bound(filter); };

    /** Compute the expectation of the given attribute. */
    float expectation(const ThreadSafePooledString &attribute, const AttrFilter &attr_filter) const {
        return spn_.expectation(translate_attribute(attribute), translate_filter(attr_filter));
    }
    /** Compute the expectation of the given attribute. */
    float expectation(unsigned attribute_id, const Filter &filter) const {
        return spn_.expectation(attribute_id, filter);
    };

    /** Update the SPN with the given row. */
    void update_row(Eigen::VectorXf &old_row, Eigen::VectorXf &updated_row) { spn_.update_row(old_row, updated_row); };

    /** Insert the given row into the SPN. */
    void insert_row(Eigen::VectorXf &row) { spn_.insert_row(row); };

    /** Delete the given row from the SPN. */
    void delete_row(Eigen::VectorXf &row) { spn_.delete_row(row); };

    /** Estimate the number of distinct values of the given attribute. */
    std::size_t estimate_number_distinct_values(const ThreadSafePooledString &attribute) const {
        return spn_.estimate_number_distinct_values(translate_attribute(attribute));
    }
    /** Estimate the number of distinct values of the given attribute. */
    std::size_t estimate_number_distinct_values(unsigned attribute_id) const {
        return spn_.estimate_number_distinct_values(attribute_id);
    };

    unsigned height() const { return spn_.height(); }
    unsigned breadth() const { return spn_.breadth(); }
    unsigned degree() const { return spn_.degree(); }
    std::size_t memory_usage() const { return spn_.memory_usage(); }
    void dump() const { spn_.dump(); };
    void dump(std::ostream &out) const { spn_.dump(out); };
};

}
