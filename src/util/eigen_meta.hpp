#pragma once

#include <Eigen/Core>
#include <Eigen/LU>


namespace m {

/** Use closed-form solution for linear regression
 * @param X the feature matrix
 * @param y the target vector
 * @return a vector of coefficients that represent the linear model
 */
Eigen::VectorXd regression_linear_closed_form(const Eigen::MatrixXd &X, const Eigen::VectorXd &y)
{
    return (X.transpose() * X).inverse() * X.transpose() * y;
}

}
