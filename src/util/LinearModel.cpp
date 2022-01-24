#include <mutable/util/LinearModel.hpp>

#include "util/eigen_meta.hpp"
#include <mutable/util/Diagnostic.hpp>
#include <mutable/util/Diagnostic.hpp>


using namespace m;


//======================================================================================================================
// LinearModel Methods
//======================================================================================================================

LinearModel::LinearModel(const Eigen::MatrixXd &X, const Eigen::VectorXd &y)
    : coefficients_(regression_linear_closed_form(X, y)), num_features_(coefficients_.rows()) {}

LinearModel::LinearModel(const Eigen::MatrixXd &X, const Eigen::VectorXd &y,
                     const std::function<Eigen::MatrixXd(Eigen::MatrixXd)> &transform_function)
                     {
    /* apply transformations */
    auto X_trans = transform_function(X);

    num_features_ = X.cols();
    transformation_ = transform_function;
    coefficients_ = regression_linear_closed_form(X_trans, y);
}

double LinearModel::predict_target(const Eigen::RowVectorXd& feature_vector) const
{
    M_insist(feature_vector.rows() == 1 and num_features_ - 1 == feature_vector.cols());
    /* The linear regression algorithm requires a column of only ones to properly
     * train the y-intercept of the model. This column is added here. */
    Eigen::RowVectorXd concat_vector(1, feature_vector.cols() + 1);
    concat_vector << 1, feature_vector;
    M_insist(num_features_ == concat_vector.cols());

    if (bool(transformation_)) {
        /* apply transformations */
        concat_vector = transformation_(concat_vector);
    }

    return concat_vector * coefficients_;
}

M_LCOV_EXCL_START
std::ostream & m::operator<<(std::ostream &out, const LinearModel &linear_model)
{
    out << "LinearModel ";
    if (bool(linear_model.transformation_)) {
        out << "with transformations ";
    }
    out << "\n[\n" << linear_model.coefficients_ << "\n]\n";
    return out;
}

void LinearModel::dump(std::ostream &out) const
{
    out << *this;
    out.flush();
}

void LinearModel::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
