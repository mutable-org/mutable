#include "catch2/catch.hpp"

#include "util/Kmeans.hpp"
#include <numeric>


using namespace m;


TEST_CASE("kmeans/empty input matrix","[core][util][kmeans]")
{
    Eigen::MatrixXf test_matrix(0, 0);

    std::vector<unsigned> labels = kmeans(test_matrix, 2);
    CHECK(labels.empty());
}

TEST_CASE("kmeans/clustering 3 rows, 4 columns","[core][util][kmeans]")
{
    Eigen::MatrixXf test_matrix(3, 4);
    test_matrix <<
        180, 154, 179, 195.3,
        1.4, 2, 1.1, 1,
        230, 222, 213.6, 205;

    SECTION("two clusters")
    {
        std::vector<unsigned> labels = kmeans(test_matrix, 2);
        CHECK(labels.size() == 3);
        CHECK(labels[0] != labels[1]);
        CHECK(labels[1] != labels[2]);
        CHECK(labels[0] == labels[2]);

        auto [smallest, greatest] = std::minmax_element(labels.begin(), labels.end());
        CHECK(*smallest >= 0); // smallest label must be at least 0
        CHECK(*greatest < 2); // greatest label must be at most 1
    }

    SECTION("three clusters")
    {
        std::vector<unsigned> labels = kmeans(test_matrix, 3);
        CHECK(labels.size() == 3);
        CHECK(labels[0] != labels[1]);
        CHECK(labels[1] != labels[2]);
        CHECK(labels[0] != labels[2]);

        auto [smallest, greatest] = std::minmax_element(labels.begin(), labels.end());
        CHECK(*smallest >= 0);
        CHECK(*greatest < 3);
    }

    SECTION("four clusters")
    {
        std::vector<unsigned> labels = kmeans(test_matrix, 4);
        CHECK(labels.size() == 3);
        CHECK(labels[0] != labels[1]);
        CHECK(labels[1] != labels[2]);
        CHECK(labels[0] != labels[2]);

        auto [smallest, greatest] = std::minmax_element(labels.begin(), labels.end());
        CHECK(*smallest >= 0);
        CHECK(*greatest < 4);
    }
}

TEST_CASE("kmeans/clustering 12 rows of 2 columns into three clusters","[core][util][kmeans]")
{
    Eigen::MatrixXf test_matrix(12, 2);
    test_matrix <<
        /* A */ 1, 1,
        /* B */ 101, 100,
        /* A */ 2, 2,
        /* C */ 50, 51,
        /* B */ 100, 100,
        /* A */ 2, 1,
        /* B */ 100, 101,
        /* C */ 51, 51,
        /* B */ 101, 101,
        /* C */ 50, 50,
        /* A */ 1, 2,
        /* C */ 51, 50;

    std::vector<unsigned> labels = kmeans(test_matrix, 3);
    CHECK(labels.size() == 12);
    const unsigned A = labels[0];
    const unsigned B = labels[1];
    const unsigned C = labels[3];

    /* Clusters must be pairwise distinct. */
    CHECK(A != B);
    CHECK(A != C);
    CHECK(B != C);

    /* Cluster A */
    CHECK(labels[2] == A);
    CHECK(labels[5] == A);
    CHECK(labels[10] == A);

    /* Cluster B */
    CHECK(labels[4] == B);
    CHECK(labels[6] == B);
    CHECK(labels[8] == B);

    /* Cluster C */
    CHECK(labels[7] == C);
    CHECK(labels[9] == C);
    CHECK(labels[11] == C);

    auto [smallest, greatest] = std::minmax_element(labels.begin(), labels.end());
    CHECK(*smallest >= 0);
    CHECK(*greatest < 3);
}
