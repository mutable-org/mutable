#pragma once

#include <mutable/mutable-config.hpp>
#include <Eigen/Core>


namespace m {

/** A model for predicting the costs of a physical operator.
 */
class M_EXPORT LinearModel {
private:
    Eigen::VectorXd coefficients_; ///< vector of coefficients for every feature
    /// transformation that is applied on the feature matrix
    std::function<Eigen::MatrixXd(Eigen::MatrixXd)> transformation_;
    unsigned num_features_; ///< number of features this model expects pre-transformation

public:
    /** Create a `LinearModel` instance given a coefficient vector */
    explicit LinearModel(Eigen::VectorXd coefficientVector)
            : coefficients_(std::move(coefficientVector)), num_features_(coefficients_.rows()) {}

    /** Create a `LinearModel` instance given a coefficient vector, a transformation function
     * and the number of expected features. */
    explicit LinearModel(Eigen::VectorXd coefficientVector, unsigned numFeatures,
                         std::function<Eigen::MatrixXd(Eigen::MatrixXd)> transform_function)
            : coefficients_(std::move(coefficientVector)), transformation_(std::move(transform_function)),
              num_features_(numFeatures) {}

    /** Create a `LinearModel` instance by linear regression given a feature maxtrix `X` containing training data of
     * the features and target vector `y` containing expected values for the features in `X`. */
    LinearModel(const Eigen::MatrixXd &X, const Eigen::VectorXd &y);

    /** Create a `LinearModel` instance by linear regression given a feature maxtrix `X` containing training data of
     * the features, target vector `y` containing expected values for the features in `X` and a transformation
     * function for the features. */
    LinearModel(const Eigen::MatrixXd &X, const Eigen::VectorXd &y,
                const std::function<Eigen::MatrixXd(Eigen::MatrixXd)> &transform_function);

    double predict_target(const Eigen::RowVectorXd &feature_vector) const;

    Eigen::VectorXd get_coefficients() const { return coefficients_; }

    friend std::ostream & operator<<(std::ostream &out, const LinearModel &linear_model);

    void dump(std::ostream &out) const;
    void dump() const;
};

}


