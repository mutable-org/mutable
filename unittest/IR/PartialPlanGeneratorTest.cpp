#include "catch2/catch.hpp"

#include "IR/PartialPlanGenerator.hpp"
#include <algorithm>
#include <initializer_list>
#include <iostream>
#include <mutable/IR/PlanTable.hpp>
#include <set>


using namespace m;
using PlanTable = PlanTableSmallOrDense;
using Subproblem = PartialPlanGenerator::Subproblem;


std::ostream & operator<<(std::ostream &out, const std::vector<Subproblem> &vec) {
    out << '[';
    for (auto it = vec.begin(); it != vec.end(); ++it) {
        if (it != vec.begin()) out << ", ";
        it->print_fixed_length(out, 4);
    }
    return out << ']';
};


TEST_CASE("PartialPlanGenerator/for_each_complete_partial_plan", "[][unit][IR]")
{
    struct Compare
    {
        bool operator()(Subproblem left, Subproblem right) const { return uint64_t(left) < uint64_t(right); };

        bool operator()(const std::vector<Subproblem> &left, const std::vector<Subproblem> &right) const {
            return std::lexicographical_compare(left.begin(), left.end(), right.begin(), right.end(), *this);
        };
    };

    std::set<std::vector<Subproblem>, Compare> expected_plans;
    auto add_expected_plan = [&expected_plans](std::initializer_list<Subproblem> subproblems) {
        std::vector<Subproblem> partial_plan(subproblems);
        std::sort(partial_plan.begin(), partial_plan.end(), Compare{});
        const auto res = expected_plans.emplace(std::move(partial_plan));
        REQUIRE(res.second);
    };

    std::set<std::vector<Subproblem>, Compare> generated_plans;
    auto add_generated_plan = [&generated_plans](std::vector<Subproblem> partial_plan) {
        std::sort(partial_plan.begin(), partial_plan.end(), Compare{});
        const auto res = generated_plans.emplace(std::move(partial_plan));
        CHECK(res.second); // duplicate?
    };

    PartialPlanGenerator PPG;

    SECTION("single relation")
    {
        PlanTable PT(1);
        add_expected_plan({ Subproblem(1UL) });
        PPG.for_each_complete_partial_plan(PT, add_generated_plan);
        CHECK(expected_plans == generated_plans);
    }

    SECTION("two relations")
    {
        PlanTable PT(2);
        const Subproblem A(1UL);
        const Subproblem B(2UL);

        PT[A|B].left  = A;
        PT[A|B].right = B;

        add_expected_plan({ A, B });
        add_expected_plan({ A|B });

        PPG.for_each_complete_partial_plan(PT, add_generated_plan);
        CHECK(expected_plans == generated_plans);
    }

    SECTION("three relations")
    {
        PlanTable PT(3);
        const Subproblem A(1UL << 0U);
        const Subproblem B(1UL << 1U);
        const Subproblem C(1UL << 2U);

        PT[A|C].left  = A;
        PT[A|C].right = C;
        PT[A|B|C].left  = A|C;
        PT[A|B|C].right = B;

        add_expected_plan({ A, B, C });
        add_expected_plan({ A|C, B });
        add_expected_plan({ A|C|B });

        PPG.for_each_complete_partial_plan(PT, add_generated_plan);
        CHECK(expected_plans == generated_plans);
    }

    SECTION("four relations, bushy plan")
    {
        PlanTable PT(4);
        const Subproblem A(1UL << 0U);
        const Subproblem B(1UL << 1U);
        const Subproblem C(1UL << 2U);
        const Subproblem D(1UL << 3U);

        PT[A|B].left  = A;
        PT[A|B].right = B;
        PT[C|D].left  = C;
        PT[C|D].right = D;
        PT[A|B|C|D].left  = A|B;
        PT[A|B|C|D].right = C|D;

        add_expected_plan({ A, B, C, D });
        add_expected_plan({ A|B, C, D });
        add_expected_plan({ A, B, C|D });
        add_expected_plan({ A|B, C|D });
        add_expected_plan({ A|B|C|D });

        PPG.for_each_complete_partial_plan(PT, add_generated_plan);
        CHECK(expected_plans == generated_plans);
    }
}
