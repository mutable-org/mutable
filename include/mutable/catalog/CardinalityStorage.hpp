#pragma once

#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/Operator.hpp>
#include <unordered_map>
#include <memory>
#include <functional>
#include <utility>
#include <cstddef>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace m
{

    struct CardinalityInfo
    {
        double estimated_point = -1.0;                        // from planner
        double true_cardinality = -1.0;                       // from last execution
        std::pair<double, double> estimated_range = {-1, -1}; // for later

        // Pure information without concatenations
        std::vector<std::string> source_tables; // Just table names
        bool has_filter = false;                // Whether a filter exists
        bool has_join = false;                  // Whether a join exists

        // Store subproblem for later reference
        Subproblem subproblem;
    };

    /**
     * @brief Custom data object to attach to PlanTableEntry
     */
    struct PlanTableEntryCardinalityData : public PlanTableEntryData
    {
        double estimated_cardinality = -1.0;
        double true_cardinality = -1.0;
        std::vector<std::string> source_tables;
        bool has_filter = false;
        bool has_join = false;
    };

    class CardinalityStorage
    {
    private:
        // Map from subproblem to cardinality info
        std::unordered_map<Subproblem, CardinalityInfo, SubproblemHash> cardinality_map_;
        bool debug_output_ = true;

    public:
        /**
         * @brief Maps actual cardinalities from physical operators back to logical plan subproblems
         *
         * @param root The root operator of the executed physical plan
         */
        void map_true_cardinalities_to_logical_plan(const Operator &root)
        {
            std::cout << "Starting to map true cardinalities to logical plan..." << std::endl;
            traverse_operator_tree(root);
            std::cout << "Mapping complete. Collected " << cardinality_map_.size() << " cardinality entries." << std::endl;
        }

        /**
         * @brief Traverses the operator tree and collects cardinality information
         *
         * @param op The current operator being processed
         */
        void traverse_operator_tree(const Operator &op)
        {
            // Debug output of the current operator type
            if (debug_output_)
            {
                std::cout << "Processing operator: " << typeid(op).name();
                if (op.has_info())
                {
                    std::cout << ", Subproblem present";
                }
                std::cout << std::endl;
            }

            // Get operator information if available
            if (op.has_info())
            {
                const Subproblem &subproblem = op.info().subproblem;
                std::size_t actual_cardinality = op.get_emitted_tuples();

                // Store or update cardinality info
                auto &card_info = cardinality_map_[subproblem];
                if (card_info.estimated_point < 0 && op.has_info() && op.info().estimated_cardinality > 0)
                {
                    card_info.estimated_point = op.info().estimated_cardinality;
                }
                card_info.true_cardinality = actual_cardinality;
                card_info.subproblem = subproblem; // Store subproblem for later reference

                // Extract metadata
                extract_operator_metadata(op, card_info);

                if (debug_output_)
                {
                    std::cout << "  Stored cardinality for subproblem";
                    std::cout << ": estimated=" << card_info.estimated_point
                              << ", actual=" << card_info.true_cardinality << std::endl;

                    if (!card_info.source_tables.empty())
                    {
                        std::cout << "  Tables: ";
                        for (const auto &table : card_info.source_tables)
                        {
                            std::cout << table << " ";
                        }
                        std::cout << std::endl;
                    }

                    std::cout << "  Has filter: " << (card_info.has_filter ? "yes" : "no") << std::endl;
                    std::cout << "  Has join: " << (card_info.has_join ? "yes" : "no") << std::endl;
                }
            }

            // Recursively traverse children
            if (auto consumer = dynamic_cast<const Consumer *>(&op))
            {
                for (auto child : consumer->children())
                {
                    traverse_operator_tree(*child);
                }
            }
        }

        /**
         * @brief Extract metadata from operators (tables, filters, joins)
         *
         * @param op The operator to extract metadata from
         * @param info The CardinalityInfo to store the metadata in
         */
        void extract_operator_metadata(const Operator &op, CardinalityInfo &info)
        {
            // For ScanOperator - extract table name only
            if (auto scan_op = dynamic_cast<const ScanOperator *>(&op))
            {
                try
                {
                    // Just the table name without any prefixes
                    std::ostringstream ss;
                    ss << scan_op->alias();
                    std::string table_name = ss.str();
                    if (!table_name.empty())
                    {
                        info.source_tables.push_back(table_name);
                        if (debug_output_)
                        {
                            std::cout << "  Added table: " << table_name << std::endl;
                        }
                    }
                }
                catch (...)
                {
                    // Silent failure - we don't want to add fallback names
                    if (debug_output_)
                    {
                        std::cout << "  Failed to extract table name" << std::endl;
                    }
                }
            }

            // For FilterOperator - just mark presence
            if (dynamic_cast<const FilterOperator *>(&op))
            {
                info.has_filter = true;
                if (debug_output_)
                {
                    std::cout << "  Marked filter presence" << std::endl;
                }
            }

            // For JoinOperator - just mark presence and gather source tables
            if (auto join_op = dynamic_cast<const JoinOperator *>(&op))
            {
                info.has_join = true;
                if (debug_output_)
                {
                    std::cout << "  Marked join presence" << std::endl;
                }

                // For joins, gather source tables from children
                if (auto consumer = dynamic_cast<const Consumer *>(&op))
                {
                    for (auto child : consumer->children())
                    {
                        if (child->has_info())
                        {
                            auto it = cardinality_map_.find(child->info().subproblem);
                            if (it != cardinality_map_.end())
                            {
                                // Add source tables from child to this operator's info
                                info.source_tables.insert(
                                    info.source_tables.end(),
                                    it->second.source_tables.begin(),
                                    it->second.source_tables.end());

                                if (debug_output_)
                                {
                                    std::cout << "  Added tables from child subproblem" << std::endl;
                                }
                            }
                        }
                    }
                }
            }
        }

        /**
         * @brief Returns a logical tree representation with both estimated and true cardinalities
         *
         * @param PT The plan table representing the logical plan
         * @return std::unique_ptr<PlanTable> The augmented logical plan
         */
        template <typename PlanTable>
        std::unique_ptr<PlanTable> get_logical_tree_with_cardinalities(const PlanTable &original_PT)
        {
            std::cout << "Creating logical tree with cardinalities..." << std::endl;
            auto PT_copy = std::make_unique<PlanTable>(original_PT);

            // For each entry in the plan table
            for (std::size_t i = 1; i < PT_copy->size(); ++i)
            {
                Subproblem s(i);
                if (PT_copy->has_plan(s))
                {
                    auto &entry = (*PT_copy)[s];

                    // If we have cardinality info for this subproblem
                    auto it = cardinality_map_.find(s);
                    if (it != cardinality_map_.end())
                    {
                        const auto &card_info = it->second;

                        // Create a new data object for this entry with pure information
                        auto data = std::make_unique<PlanTableEntryCardinalityData>();
                        data->estimated_cardinality = card_info.estimated_point;
                        data->true_cardinality = card_info.true_cardinality;
                        data->source_tables = card_info.source_tables; // Just the table names
                        data->has_filter = card_info.has_filter;       // Boolean flag
                        data->has_join = card_info.has_join;           // Boolean flag

                        // Attach to plan table entry
                        entry.data = std::move(data);

                        if (debug_output_)
                        {
                            std::cout << "  Added cardinality data to subproblem";
                            std::cout << ": estimated=" << card_info.estimated_point
                                      << ", actual=" << card_info.true_cardinality << std::endl;
                        }
                    }
                }
            }

            std::cout << "Logical tree with cardinalities created successfully." << std::endl;
            return PT_copy;
        }

        /**
         * @brief Toggle debug output
         */
        void set_debug_output(bool enable)
        {
            debug_output_ = enable;
        }

        /**
         * @brief Singleton access method
         * @return CardinalityStorage& The global instance
         */
        static CardinalityStorage &instance()
        {
            static CardinalityStorage storage;
            return storage;
        }
    };

} // namespace m
