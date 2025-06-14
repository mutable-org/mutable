#pragma once

#include <mutable/IR/PlanTable.hpp>
#include <mutable/IR/Operator.hpp>
#include <unordered_map>
#include <memory>
#include <vector>
#include <string>
#include <cstddef>
#include <iostream>

namespace m
{

    /**
     * @brief Complete data structure containing all relevant cardinality information for easy extraction
     */
    struct CardinalityData
    {
        // Cardinality information
        double estimated_cardinality = -1.0; // Estimated by optimizer
        double true_cardinality = -1.0;      // Actual from execution

        // Subproblem identification
        Subproblem subproblem; // Complete bitmask

        // Tables involved
        std::vector<std::string> table_names; // Table names in this subproblem

        // Table position mapping (which bit position corresponds to which table)
        std::vector<std::pair<unsigned, std::string>> table_positions; // Bit position -> table name

        // Operation information
        bool has_filter = false; // Has filter operation
        bool has_join = false;   // Has join operation

        // Performance metrics
        double selectivity = -1.0;   // Computed selectivity if available
        double error_percent = -1.0; // Estimation error percentage

        // Get the full bitmask as a string (e.g., "1101")
        std::string get_bitmask_string(size_t max_positions) const
        {
            std::string result(max_positions, '0');
            for (auto &[pos, _] : table_positions)
            {
                if (pos < max_positions)
                    result[pos] = '1';
            }
            return result;
        }

        // Get a table name by its bit position
        std::string get_table_name_by_position(unsigned position) const
        {
            for (const auto &[pos, name] : table_positions)
            {
                if (pos == position)
                    return name;
            }
            return "";
        }

        // Check if a specific table is included
        bool includes_table(const std::string &table_name) const
        {
            for (const auto &name : table_names)
            {
                if (name == table_name)
                    return true;
            }
            return false;
        }
    };

    /**
     * @brief Custom data object to attach to PlanTableEntry
     */
    struct PlanTableEntryCardinalityData : public PlanTableEntryData
    {
        std::shared_ptr<CardinalityData> data; // Use shared_ptr for efficient copying

        PlanTableEntryCardinalityData(std::shared_ptr<CardinalityData> data_ptr) : data(data_ptr) {}
    };

    class CardinalityStorage
    {
    private:
        // Efficiently store data with shared_ptr to avoid duplication
        std::vector<std::shared_ptr<CardinalityData>> all_cardinality_data_;

        // Map from subproblem to data index for fast lookup
        std::unordered_map<Subproblem, std::size_t, SubproblemHash> subproblem_to_data_;

        // Global table position mapping
        std::vector<std::pair<unsigned, std::string>> global_table_positions_;
        size_t max_bit_positions_ = 0;

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

            // Clear previous data
            all_cardinality_data_.clear();
            subproblem_to_data_.clear();
            global_table_positions_.clear();

            // First pass: collect all table names and assign bit positions
            collect_table_positions(root);

            // Second pass: traverse and collect cardinalities
            traverse_operator_tree(root);

            // Debug output
            if (debug_output_)
            {
                std::cout << "\nTable to bit position mapping:" << std::endl;
                for (const auto &[pos, table] : global_table_positions_)
                {
                    std::cout << "  Bit " << pos << " = " << table << std::endl;
                }

                std::cout << "\nCollected " << all_cardinality_data_.size() << " cardinality entries." << std::endl;
                for (const auto &data : all_cardinality_data_)
                {
                    std::cout << "  Subproblem [" << data->get_bitmask_string(max_bit_positions_)
                              << "]: estimated=" << data->estimated_cardinality
                              << ", actual=" << data->true_cardinality << std::endl;

                    std::cout << "    Tables: ";
                    for (const auto &name : data->table_names)
                    {
                        std::cout << name << " ";
                    }
                    std::cout << std::endl;
                }
            }
        }

        /**
         * @brief First pass: collect all table names and assign bit positions
         *
         * @param root The root operator
         */
        void collect_table_positions(const Operator &root)
        {
            unsigned next_position = 0;

            std::function<void(const Operator &)> traverse = [&](const Operator &op)
            {
                if (auto scan_op = dynamic_cast<const ScanOperator *>(&op))
                {
                    try
                    {
                        std::ostringstream ss;
                        ss << scan_op->alias();
                        std::string table_name = ss.str();

                        if (!table_name.empty())
                        {
                            // Check if we already have a position for this table
                            bool found = false;
                            for (const auto &[pos, name] : global_table_positions_)
                            {
                                if (name == table_name)
                                {
                                    found = true;
                                    break;
                                }
                            }

                            // If not found, assign the next available position
                            if (!found)
                            {
                                global_table_positions_.emplace_back(next_position, table_name);
                                next_position++;
                                max_bit_positions_ = next_position; // Update max positions

                                if (debug_output_)
                                {
                                    std::cout << "  Assigned bit position " << (next_position - 1)
                                              << " to table " << table_name << std::endl;
                                }
                            }
                        }
                    }
                    catch (...)
                    {
                        // Ignore failures
                    }
                }

                // Recursively traverse children
                if (auto consumer = dynamic_cast<const Consumer *>(&op))
                {
                    for (auto child : consumer->children())
                    {
                        traverse(*child);
                    }
                }
            };

            traverse(root);

            if (debug_output_)
            {
                std::cout << "Collected " << global_table_positions_.size() << " tables." << std::endl;
            }
        }

        /**
         * @brief Traverses the operator tree and collects cardinality information
         *
         * @param op The current operator being processed
         */
        void traverse_operator_tree(const Operator &op)
        {
            if (debug_output_)
            {
                std::cout << "Processing operator: " << typeid(op).name() << std::endl;
            }

            if (op.has_info())
            {
                const Subproblem &subproblem = op.info().subproblem;

                // Create or find the cardinality data for this subproblem
                std::shared_ptr<CardinalityData> data;
                auto it = subproblem_to_data_.find(subproblem);

                if (it == subproblem_to_data_.end())
                {
                    // Create new data entry
                    data = std::make_shared<CardinalityData>();
                    data->subproblem = subproblem;

                    // Add to our collections
                    subproblem_to_data_[subproblem] = all_cardinality_data_.size();
                    all_cardinality_data_.push_back(data);
                }
                else
                {
                    // Use existing entry
                    data = all_cardinality_data_[it->second];
                }

                // Update cardinality information
                if (op.has_info() && op.info().estimated_cardinality > 0)
                {
                    data->estimated_cardinality = op.info().estimated_cardinality;
                }
                data->true_cardinality = op.get_emitted_tuples();

                // Calculate error percentage if both values are available
                if (data->estimated_cardinality > 0 && data->true_cardinality > 0)
                {
                    data->error_percent = std::abs(data->estimated_cardinality - data->true_cardinality) /
                                          data->true_cardinality * 100.0;
                }

                // Calculate selectivity if processed count is available
                size_t processed = op.get_processed_tuples();
                if (processed > 0)
                {
                    data->selectivity = static_cast<double>(data->true_cardinality) / processed;
                }

                // Extract metadata from operator
                extract_operator_metadata(op, *data);

                // Update table positions based on the subproblem bitmask
                update_table_positions(*data);
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
         * @brief Update table positions for a CardinalityData based on its subproblem
         *
         * @param data The CardinalityData to update
         */
        void update_table_positions(CardinalityData &data)
        {
            // Clear existing positions
            data.table_positions.clear();

            // For each potential bit position (using the full range)
            for (unsigned pos = 0; pos < max_bit_positions_; ++pos)
            {
                // If this bit is set in the subproblem
                if (data.subproblem[pos])
                {
                    // Look up the table name for this position
                    for (const auto &[map_pos, table_name] : global_table_positions_)
                    {
                        if (map_pos == pos)
                        {
                            data.table_positions.emplace_back(pos, table_name);
                            break;
                        }
                    }
                }
            }
        }

        /**
         * @brief Extract metadata from operators (tables, filters, joins)
         *
         * @param op The operator to extract metadata from
         * @param data The CardinalityData to update
         */
        void extract_operator_metadata(const Operator &op, CardinalityData &data)
        {
            // For ScanOperator - extract table name
            if (auto scan_op = dynamic_cast<const ScanOperator *>(&op))
            {
                try
                {
                    std::ostringstream ss;
                    ss << scan_op->alias();
                    std::string table_name = ss.str();

                    if (!table_name.empty())
                    {
                        // Only add if not already present
                        if (std::find(data.table_names.begin(), data.table_names.end(), table_name) == data.table_names.end())
                        {
                            data.table_names.push_back(table_name);
                        }
                    }
                }
                catch (...)
                {
                    // Silent failure
                }
            }

            // For FilterOperator - just mark presence
            if (dynamic_cast<const FilterOperator *>(&op))
            {
                data.has_filter = true;
            }

            // For JoinOperator - mark presence and gather source tables
            if (auto join_op = dynamic_cast<const JoinOperator *>(&op))
            {
                data.has_join = true;

                // For joins, merge source tables from children
                if (auto consumer = dynamic_cast<const Consumer *>(&op))
                {
                    for (auto child : consumer->children())
                    {
                        if (child->has_info())
                        {
                            auto it = subproblem_to_data_.find(child->info().subproblem);
                            if (it != subproblem_to_data_.end())
                            {
                                const auto &child_data = all_cardinality_data_[it->second];

                                // Add source tables from child to this operator's info
                                for (const auto &table : child_data->table_names)
                                {
                                    if (std::find(data.table_names.begin(), data.table_names.end(), table) == data.table_names.end())
                                    {
                                        data.table_names.push_back(table);
                                    }
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
                    auto it = subproblem_to_data_.find(s);
                    if (it != subproblem_to_data_.end())
                    {
                        // Attach the data to the plan table entry
                        entry.data = std::make_unique<PlanTableEntryCardinalityData>(
                            all_cardinality_data_[it->second]);

                        if (debug_output_)
                        {
                            const auto &data = all_cardinality_data_[it->second];
                            std::cout << "  Added cardinality data to subproblem ["
                                      << data->get_bitmask_string(max_bit_positions_) << "]" << std::endl;
                        }
                    }
                }
            }

            std::cout << "Logical tree with cardinalities created successfully." << std::endl;
            return PT_copy;
        }

        /**
         * @brief Get all collected cardinality data
         *
         * @return const std::vector<std::shared_ptr<CardinalityData>>& All cardinality data
         */
        const std::vector<std::shared_ptr<CardinalityData>> &get_all_cardinality_data() const
        {
            return all_cardinality_data_;
        }

        /**
         * @brief Get cardinality data for a specific subproblem
         *
         * @param subproblem The subproblem to lookup
         * @return std::shared_ptr<CardinalityData> The data (or nullptr if not found)
         */
        std::shared_ptr<CardinalityData> get_cardinality_data(const Subproblem &subproblem) const
        {
            auto it = subproblem_to_data_.find(subproblem);
            if (it != subproblem_to_data_.end())
            {
                return all_cardinality_data_[it->second];
            }
            return nullptr;
        }

        /**
         * @brief Get tables that appear in all cardinality data entries
         *
         * @return std::vector<std::string> List of all table names
         */
        std::vector<std::string> get_all_tables() const
        {
            std::vector<std::string> result;
            for (const auto &[pos, name] : global_table_positions_)
            {
                result.push_back(name);
            }
            return result;
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
