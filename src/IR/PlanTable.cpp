#include <mutable/IR/PlanTable.hpp>

#include <algorithm>
#include <cmath>
#include <iomanip>
#include <ios>
#include <mutable/catalog/Catalog.hpp>
#include <vector>


using namespace m;


/*----------------------------------------------------------------------------------------------------------------------
 * PlanTableSmallOrDense
 *--------------------------------------------------------------------------------------------------------------------*/

M_LCOV_EXCL_START
std::ostream & m::operator<<(std::ostream &out, const PlanTableSmallOrDense &PT)
{
    using std::setw;

    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

    std::size_t num_sources = PT.num_sources();
    std::size_t n = 1UL << num_sources;

    /* Compute max length of columns. */
    auto &entry = PT.get_final();
    const uint64_t size_len = std::max<uint64_t>(
        entry.model ? std::ceil(std::log10(CE.predict_cardinality(*entry.model))) : 0,
        4
    );
    const uint64_t cost_len = std::max<uint64_t>(std::ceil(std::log10(entry.cost)), 4);
    const uint64_t sub_len  = std::max<uint64_t>(PT.num_sources(), 5);

    out << std::left << "Plan Table:\n"
        << std::setw(num_sources) << "Sub"    << "  "
        << std::setw(size_len)    << "Size"   << "  "
        << std::setw(cost_len)    << "Cost"   << "  "
        << std::setw(sub_len)     << "Left"   << "  "
        << std::setw(sub_len)     << "Right"  << '\n' << std::right;

    for (std::size_t i = 1; i < n; ++i) {
        Subproblem sub(i);
        sub.print_fixed_length(out, num_sources);
        out << "  ";;
        if (PT.has_plan(sub)) {
            if (PT.at(sub).model)
                out << std::setw(size_len) << CE.predict_cardinality(*PT.at(sub).model);
            else
                out << std::setw(size_len) << '-';
            out << "  "
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

void PlanTableSmallOrDense::dump(std::ostream &out) const { out << *this; out.flush(); }
void PlanTableSmallOrDense::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP


/*----------------------------------------------------------------------------------------------------------------------
 * PlanTableLargeAndSparse
 *--------------------------------------------------------------------------------------------------------------------*/

M_LCOV_EXCL_START
std::ostream & m::operator<<(std::ostream &out, const PlanTableLargeAndSparse &PT)
{
    using std::setw;

    auto &C = Catalog::Get();
    auto &DB = C.get_database_in_use();
    auto &CE = DB.cardinality_estimator();

    std::size_t num_sources = PT.num_sources();

    /* Compute max length of columns. */
    auto &entry = PT.get_final();
    const uint64_t size_len = std::max<uint64_t>(
        entry.model ? std::ceil(std::log10(CE.predict_cardinality(*entry.model))) : 0,
        4
    );
    const uint64_t cost_len = std::max<uint64_t>(std::ceil(std::log10(entry.cost)), 4);
    const uint64_t sub_len  = std::max<uint64_t>(PT.num_sources(), 5);

    out << std::left << "Plan Table:\n"
        << std::setw(num_sources) << "Sub"    << "  "
        << std::setw(size_len)    << "Size"   << "  "
        << std::setw(cost_len)    << "Cost"   << "  "
        << std::setw(sub_len)     << "Left"   << "  "
        << std::setw(sub_len)     << "Right"  << '\n' << std::right;

    std::vector<std::pair<Subproblem, const PlanTableEntry*>> sorted_entries;
    sorted_entries.reserve(PT.table_.size());

    for (auto &e : PT.table_) {
        auto pos = std::upper_bound(sorted_entries.begin(), sorted_entries.end(), e.first,
                                    [](Subproblem s, const std::pair<Subproblem, const PlanTableEntry*> &elem) {
                                        return uint64_t(s) < uint64_t(elem.first);
                                    });
        sorted_entries.emplace(pos, e.first, &e.second);
    }

    for (auto &elem : sorted_entries) {
        Subproblem sub(elem.first);
        sub.print_fixed_length(out, num_sources);
        out << "  ";;
        if (PT.has_plan(sub)) {
            if (elem.second->model)
                out << std::setw(size_len) << CE.predict_cardinality(*(elem.second->model));
            else
                out << std::setw(size_len) << '-';
            out << "  "
                << std::setw(cost_len) << elem.second->cost << "  "
                << std::setw(sub_len) << uint64_t(elem.second->left) << "  "
                << std::setw(sub_len) << uint64_t(elem.second->right) << '\n';
        } else {
            out << std::setw(size_len) << '-' << "  "
                << std::setw(cost_len) << '-' << "  "
                << std::setw(sub_len)  << '-' << "  "
                << std::setw(sub_len)  << '-' << '\n';
        }
    }
    return out;
}

void PlanTableLargeAndSparse::dump(std::ostream &out) const { out << *this; out.flush(); }
void PlanTableLargeAndSparse::dump() const { dump(std::cerr); }
M_LCOV_EXCL_STOP
