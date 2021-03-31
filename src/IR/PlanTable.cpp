#include "mutable/IR/PlanTable.hpp"

#include <cmath>
#include <iomanip>
#include <ios>


using namespace m;


std::ostream & m::operator<<(std::ostream &out, const PlanTable &PT)
{
    using std::setw;

    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

    std::size_t num_sources = PT.num_sources();
    std::size_t n = 1UL << num_sources;

    /* Compute max length of columns. */
    auto &entry = PT.get_final();
    const uint64_t size_len = std::max<uint64_t>(std::ceil(std::log10(CE.predict_cardinality(*entry.model))), 4);
    const uint64_t cost_len = std::max<uint64_t>(std::ceil(std::log10(entry.cost)), 4);
    const uint64_t sub_len  = std::max<uint64_t>(PT.num_sources(), 5);

    out << std::left << "Plan Table:\n"
        << std::setw(num_sources) << "Sub"    << "  "
        << std::setw(size_len)    << "Size"   << "  "
        << std::setw(cost_len)    << "Cost"   << "  "
        << std::setw(sub_len)     << "Left"   << "  "
        << std::setw(sub_len)     << "Right"  << '\n' << std::right;

    for (std::size_t i = 1; i < n; ++i) {
        PlanTable::Subproblem sub(i);
        sub.print_fixed_length(out, num_sources);
        out << "  ";;
        if (PT.has_plan(sub)) {
            out << std::setw(size_len) << CE.predict_cardinality(*PT.at(sub).model) << "  "
                << std::setw(cost_len) << PT.at(sub).cost << "  "
                << std::setw(sub_len) << uint64_t(PT.at(sub).left) << "  "
                << std::setw(sub_len) << uint64_t(PT.at(sub).right) << '\n';
        } else {
            out << std::setw(size_len) << '-' << "  "
                << std::setw(cost_len) << '-' << "  "
                << std::setw(sub_len)  << '-' << "  "
                << std::setw(sub_len)  << '-' << '\n';
        }
    }
    return out;
}
