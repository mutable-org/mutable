#include "IR/PDDL.hpp"

#include <mutable/util/fn.hpp>
#include <sys/stat.h>
#include <unordered_set>


using namespace m;


/** Class to represent a resultSizeFact to be able to hold them in a set and identify whether the fact has already
 * been created */
struct ResultSizeFact
{
    std::string left;
    std::string right;
    std::string result;

    ResultSizeFact(std::string left, std::string right, std::string result) : left(left), right(right),
                                                                              result(result) { }

    bool operator==(const ResultSizeFact &other) const
    {
        return (this->left == other.left && this->right == other.right && this->result == other.result)
               || (this->left == other.right && this->right == other.left && this->result == other.result);
    }

};

namespace std {
template<>
struct hash<ResultSizeFact>
{
    inline std::size_t operator()(const ResultSizeFact &rsf) const
    {
        std::size_t hash = 0;
        std::hash<std::string> str_hash;
        hash += str_hash(rsf.left);
        hash += str_hash(rsf.right);
        hash += str_hash(rsf.result);
        return hash;
    }
};
}

/**
 * Class for the recursive generation of the resultSize and sum facts
 * As a byproduct contains all size literals used after fact generation
 */
struct RecursiveFactsGenerator
{
    std::unordered_set<std::string> seen_;
    std::unordered_set<std::string> seen_sets;
    std::unordered_set<ResultSizeFact> seen_facts;
    std::ostringstream sum_facts_stream;
    std::ostringstream resultSize_facts_stream;
    std::unordered_set<std::string> sizes;
    const CardinalityEstimator &CE;

    RecursiveFactsGenerator(const CardinalityEstimator &CE) : CE(CE) { }

    /**
     * Generates the sum and resultSize facts for all subproblems that can be reached from @param relations
     * Looks for all join-partners of the relations, creates the facts and then calls itself recursively to generate the
     * facts for next bigger subproblem
     * Saves the generated facts in the `RecursiveFactsGenerator` and makes sure to eliminate duplicates
     * @param G the `QueryGraph` of the problem
     * @param relations the current subproblem that is to be expanded
     */
    void generate_facts(const QueryGraph &G, std::vector<DataSource *> relations){
        std::vector<DataSource *> join_partners = find_join_partners(relations, std::vector<DataSource *>());
        for (auto join_partner: join_partners) {
            std::vector<DataSource *> join_partner_vector = {join_partner};
            std::vector<std::vector<DataSource *>> subproblems_to_join = compute_subproblems_to_join(relations,
                                                                                                     join_partner_vector);
            seen_sets.clear();

            for (auto subproblem_to_join: subproblems_to_join) {
                std::vector<DataSource *> relations_join_subproblem;
                relations_join_subproblem.insert(relations_join_subproblem.end(), relations.begin(), relations.end());
                relations_join_subproblem.insert(relations_join_subproblem.end(), subproblem_to_join.begin(),
                                                 subproblem_to_join.end());

                ResultSizeFact rsf(make_fact_name(G, relations), make_fact_name(G, subproblem_to_join),
                                   make_fact_name(G, relations_join_subproblem));
                if (seen_facts.find(rsf) != seen_facts.end()) continue;
                seen_facts.insert(rsf);
                generate_resultSize_fact(G, relations, subproblem_to_join);
                generate_sum_fact(G, relations, subproblem_to_join);
                if (seen_.find(make_identifier(relations_join_subproblem)) != seen_.end()) continue;
                generate_facts(G, relations_join_subproblem);
            }

        }
        seen_.insert(make_identifier(relations));
    }

    /*==================================================================================================================
    * Helper Methods for generate_facts
    *=================================================================================================================*/
    /**
     * Computes all possible sets of relations that can be joined with @param relations containing the `DataSource`s in
     * @param partner
     * @param relations the set of relations to compute all possible join partner sets for
     * @param partner the `DataSource`s that should be contained in every set
     * @return a list of sets of relations that can be joined with @param relations
     */
    std::vector<std::vector<DataSource *>> compute_subproblems_to_join(std::vector<DataSource *> &relations,
                                                                       std::vector<DataSource *> &partner){
        std::vector<std::vector<DataSource *>> subproblems_to_join;
        //Include the original solo Partner
        if (seen_sets.find(make_identifier(partner)) == seen_sets.end()) {
            subproblems_to_join.push_back(partner);
            seen_sets.insert(make_identifier(partner));
        }
        std::vector<DataSource *> join_partners = find_join_partners(partner, relations);

        for (auto join_partner: join_partners) {
            std::vector<DataSource *> new_partner = {join_partner};
            new_partner.insert(new_partner.end(), partner.begin(), partner.end());
            auto sets = compute_subproblems_to_join(relations, new_partner);
            subproblems_to_join.insert(subproblems_to_join.end(), sets.begin(), sets.end());
        }
        return subproblems_to_join;
    }
    /**
     * finds all join-partners that can be joined with a least one relation from @param relations that are not included
     * in @param exclude
     * @param relations the relations to find the join partners for
     * @param exclude the relations that must not be contained in the resulting list
     * @return the list of join partners
     */
    std::vector<DataSource *> find_join_partners(std::vector<DataSource *> &relations,
                                                 std::vector<DataSource *> exclude){
        std::unordered_set<std::size_t> exclude_ids;
        for (auto excl: exclude) {
            exclude_ids.insert(excl->id());
        }
        std::unordered_set<std::size_t> join_partner_ids;
        for (auto relation: relations) {
            join_partner_ids.insert(relation->id());
        }
        std::vector<DataSource *> join_partners;
        for (auto relation: relations) {
            std::vector<Join *> joins = relation->joins();
            for (auto join: joins) {
                for (auto partner: join->sources()) {
                    if (partner->id() != relation->id() &&
                        join_partner_ids.find(partner->id()) == join_partner_ids.end() &&
                        exclude_ids.find(partner->id()) == exclude_ids.end()) {
                        join_partner_ids.insert(partner->id());
                        join_partners.push_back(partner);
                    }
                }
            }
        }
        return join_partners;
    }

    /**
     * Generates a unique identifier for a set of `DataSource`s by concatenating their sorted names comma-seperated
     * @param relations the `DataSource`s to make the identifier for
     * @return the identifier
     */
    std::string make_identifier(std::vector<DataSource *> &relations){
        std::vector<std::string> relation_names;
        for (auto relation: relations) {
            auto base_table = cast<BaseTable>(relation);
            relation_names.emplace_back(base_table->table().name);
        }
        std::sort(relation_names.begin(), relation_names.end());
        std::ostringstream oss;
        std::size_t i = 0;
        for (; i < relation_names.size() - 1; i++) {
            oss << relation_names[i];
            oss << ",";
        }
        oss << relation_names[i];
        return oss.str();
    }

    /**
     * Generates the fact name for a given set of `DataSource`s by concatenating their sorted names without sepeartor
     * and appending their size e.g. for names of the `DataSource`s "A", "B", "C" return "ABC1200"
     * @param relations the `DataSource`s to make the factname for
     * @return the fact name
     */
    std::string make_fact_name(const QueryGraph &G, std::vector<DataSource *> &relations){
        std::vector<std::string> relation_names;
        Subproblem P(0);
        for (auto relation: relations) {
            auto base_table = cast<BaseTable>(relation);
            relation_names.emplace_back(base_table->table().name);
            P.set(base_table->id(), true);
        }
        std::sort(relation_names.begin(), relation_names.end());
        std::ostringstream oss;
        for (auto &relation_name : relation_names) {
            oss << relation_name;
        }
        auto model = make_model(G, P);
        oss << CE.predict_cardinality(*model);
        return oss.str();
    }

    /**
     * For a given `Subproblem` P, generate the `DataModel` required by the `CardinalityEstimator` to estimate the size
     * of P
     * @param G the `QueryGraph` of the problem
     * @param P the `Subproblem` to compute the `DataModel` for
     * @return the computed `DataModel`
     */
    std::unique_ptr<CardinalityEstimator::DataModel> make_model(const QueryGraph &G, Subproblem P) {
        std::vector<std::unique_ptr<CardinalityEstimator::DataModel>> data_models;
        for (auto P_it = P.begin(); P_it != P.end(); ++P_it) {
            Subproblem P_single(0);
            P_single.set(*P_it, true);
            data_models.push_back(CE.estimate_scan(G, P_single));
        }
        int data_models_size = data_models.size();
        for (int i = 0; i < data_models_size-1; i++) {
            data_models.push_back(CE.estimate_join(*data_models[i], *data_models[data_models.size()-1], cnf::CNF()));
        }
        return std::move(data_models[data_models.size()-1]);
    }

    /**
     * Generate the resultSize fact for a join of two lists of `DataSource`s, updates `RecursiveFactsGenerator`.sizes
     * with the size literals created in the process and appends it to the sum_facts_stream
     * @param left left join partner
     * @param right right join partner
     * @return the generated resultSize fact
     */
    void generate_resultSize_fact(const QueryGraph &G, std::vector<DataSource *> left, std::vector<DataSource *> right){
        std::string left_identifier = "s" + make_fact_name(G, left);
        std::string right_identifier = "s" + make_fact_name(G, right);
        std::vector<DataSource *> result;
        result.insert(result.end(), left.begin(), left.end());
        result.insert(result.end(), right.begin(), right.end());
        std::string result_identifier = "s" + make_fact_name(G, result);
        resultSize_facts_stream << "\t\t(resultSize "
                                << left_identifier
                                << " "
                                << right_identifier
                                << " "
                                << result_identifier
                                << ")\n";
        if (sizes.find(left_identifier) == sizes.end()) sizes.insert(left_identifier);
        if (sizes.find(right_identifier) == sizes.end()) sizes.insert(right_identifier);
        if (sizes.find(result_identifier) == sizes.end()) sizes.insert(result_identifier);
    }

    /**
     * Generates the sum fact for a join of two lists of DataSource`s by envoking the `CardinalityEstimator` of
     * `RecursiveFactsGenerator` and appends it to the sum_facts_stream
     * @param G the `QueryGraph` of the problem, needed for the estimation in the `CardinalityEstimator`
     * @param left left join partner
     * @param right right join partner
     * @return the generated sum fact
     */
    void generate_sum_fact(const QueryGraph &G, std::vector<DataSource *> &left, std::vector<DataSource *> &right){
        std::string left_identifier = make_fact_name(G, left);
        std::string right_identifier = make_fact_name(G, right);
        Subproblem P_left(0);
        for (auto l:left) {
            P_left.set(l->id(), true);
        }
        Subproblem P_right(0);
        for (auto r:right) {
            P_right.set(r->id(), true);
        }
        std::ostringstream sum_fact;

//        auto left_model = CE.estimate_scan(G, P_left);
auto left_model = make_model(G, P_left);
        std::size_t left_size = CE.predict_cardinality(*left_model);
//        auto right_model = CE.estimate_scan(G, P_right);
        auto right_model = make_model(G, P_right);
        std::size_t right_size = CE.predict_cardinality(*right_model);
        std::size_t size_of_result = left_size + right_size;

        sum_facts_stream << "\t\t(= (sum s"
                            << left_identifier
                            << " s"
                            << right_identifier
                            << ") "
                            << size_of_result
                            << ")\n";
    }
};

/*======================================================================================================================
* Helper Methods for file generation
*=====================================================================================================================*/
namespace {
/** return the corresponding string for a number of actions to be used in the names of the files*/
std::string get_actions_string(std::size_t number_of_actions) {
    switch (number_of_actions) {
        case 2: {
            return "Two";
        }
        case 3: {
            return "Three";
        }
        default: {
            return "Four";
        }
    }
}

/** return the domain name, given the number of actions and relations of a query*/
std::string get_domain_name(std::size_t number_of_actions, std::size_t number_of_relations) {

    std::ostringstream domain_name;
    switch (number_of_actions) {
        case 2: {
            domain_name << "Two";
            break;
        }
        case 3: {
            domain_name << "Three";
            break;
        }
        default: {
            domain_name << "Four";
            break;
        }
    }
    domain_name << "ActionsDomain"
                << std::to_string(number_of_relations);
    return domain_name.str();
}
}

/*======================================================================================================================
* File Generation
*=====================================================================================================================*/
void PDDLGenerator::generate_files(const QueryGraph &G, std::size_t number_of_actions, std::filesystem::path domain_path,
                                   std::filesystem::path problem_path)
{
    //Make sure all variables are empty before starting the generation of a new PDDL
    views_.clear();
    sizes_.clear();
    relations_.clear();
    domain_file_content_.str(std::string());
    problem_file_content_.str(std::string());
    init_section_problem_file_.str(std::string());
    Position pos("PDDLGenerator");

    //Only accept QueryGraphs that have no nested subqueries
    for (auto source: G.sources()) {
        if (auto query = cast<Query>(source)) {
            diag_.e(pos) << "PDDL files can only be created for Queries that do not have nested subqueries.";
            return;
        }
    }

    //make directories
    mkdir(domain_path.c_str(), 0777);
    mkdir(problem_path.c_str(), 0777);

    std::size_t number_of_relations = G.sources().size();
    // number of actions 0 means make all 3 models
    if (number_of_actions == 0) {
        generate_domain_file(number_of_relations, 2, domain_path);
        generate_domain_file(number_of_relations, 3, domain_path);
        generate_domain_file(number_of_relations, 4, domain_path);
    } else {
        generate_domain_file(number_of_relations, number_of_actions, domain_path);
    }

    generate_problem_file(G, number_of_actions, problem_path);
}

void PDDLGenerator::generate_domain_file(std::size_t number_of_relations, std::size_t number_of_actions,
                                         std::filesystem::path domain_path)
{
    std::string domain_name = get_domain_name(number_of_actions, number_of_relations);
    std::ostringstream filename;
    filename << domain_path.string()
             << domain_name
             << ".pddl";
    //Generate Header
    domain_file_content_ << "(define (domain " << domain_name << ")\n";
    generate_requirements();
    domain_file_content_ << "\n";
    generate_types();
    domain_file_content_ << "\n";
    generate_constants(number_of_relations, number_of_actions);
    domain_file_content_ << "\n";
    generate_predicates();
    domain_file_content_ << "\n";
    generate_functions();
    domain_file_content_ << "\n";
    generate_actions(number_of_actions);
    domain_file_content_ << ")\n";
    std::ofstream domain_file;
    domain_file.open(filename.str());
    domain_file << domain_file_content_.str();
    domain_file.close();
    domain_file_content_.str("");
}



void PDDLGenerator::generate_problem_file(const QueryGraph &G, std::size_t number_of_actions,
                                          std::filesystem::path &problem_path)
{

    generate_init_part(G);

    generate_objects_part(relations_, views_, sizes_);
    problem_file_content_ << "\n"
                            << init_section_problem_file_.str()
                            << "\n";
    generate_goal_part(G.sources().size());
    problem_file_content_ << "\n"
                            << "\t(:metric minimize (total-cost))\n"
                            << ")\n";

    //for number_of_actions 0, make all three models
    if (number_of_actions == 0) {
        for (std::size_t actions = 2; actions <= 4; actions++) {
            std::ostringstream problem_name;
            problem_name << get_actions_string(actions)
                         << "ActionsProblem"
                         << std::to_string(G.sources().size());
            std::ostringstream filename;
            filename << problem_path.string()
                     << problem_name.str()
                     << ".pddl";

            std::ofstream problem_file;
            problem_file.open(filename.str());
            problem_file << "(define (problem " << problem_name.str() << ") (:domain "
                         << get_domain_name(actions, G.sources().size()) << ")\n";
            problem_file << problem_file_content_.str();
            problem_file.close();
        }
    } else {
        std::ostringstream problem_name;
        problem_name << get_actions_string(number_of_actions)
                     << "ActionsProblem"
                     << std::to_string(G.sources().size());
        std::ostringstream filename;
        filename << problem_path.string()
                 << problem_name.str()
                 << ".pddl";

        std::ofstream problem_file;
        problem_file.open(filename.str());
        problem_file << "(define (problem " << problem_name.str() << ") (:domain "
                     << get_domain_name(number_of_actions, G.sources().size()) << ")\n";
        problem_file << problem_file_content_.str();
        problem_file.close();
    }
}

/*======================================================================================================================
* Domain File Generation - HelperMethods
*=====================================================================================================================*/
void PDDLGenerator::generate_requirements()
{
    domain_file_content_ << "\t(:requirements\n "
                            << "\t\t:strips\n"
                            << "\t\t:typing\n"
                            << "\t\t:negative-preconditions\n"
                            << "\t\t:equality\n"
                            << "\t\t:action-costs\n"
                            << "\t)\n";
}

void PDDLGenerator::generate_types()
{
    domain_file_content_ << "\t(:types\n "
                            << "\t\tview relation size count state - object\n"
                            << "\t)\n";
}

void PDDLGenerator::generate_predicates()
{
    domain_file_content_ << "\t(:predicates\n "
                            << "\t\t(stillToDo ?c - count)\n"
                            << "\t\t(biggestCount ?c - count)\n"
                            << "\t\t(currentFrom ?v - view)\n"
                            << "\t\t(currentTo ?v - view)\n"
                            << "\t\t(currentState ?state - state)\n"
                            << "\n"
                            << "\t\t(hasJoin ?r ?s - relation)\n"
                            << "\t\t(isInView ?r - relation ?v - view)\n"
                            << "\t\t(currentSize ?v - view ?s - size)\n"
                            << "\t\t(currentCount ?v - view ?c - count)\n"
                            << "\n"
                            << "\t\t(resultSize ?s1 - size ?s2 - size ?s3 - size)\n"
                            << "\t\t(plus ?c1 ?c2 ?result - count)\n"
                            << "\t)\n";
}

void PDDLGenerator::generate_functions()
{
    domain_file_content_ << "\t(:functions\n "
                            << "\t\t(sum ?s1 - size ?s2 - size) - number\n"
                            << "\t\t(total-cost) - number\n"
                            << "\t)\n";
}

void PDDLGenerator::generate_constants(std::size_t number_of_relations, std::size_t number_of_actions)
{

    domain_file_content_ << "\t(:constants\n";
    // count-constants
    domain_file_content_ << "\t\tzero ";
    for (std::size_t i = 1; i < number_of_relations+1; i++) {
        domain_file_content_ << counts_[i] << " ";
    }
    domain_file_content_ << "- count\n";
    // state-constants
    //default case is 4
    domain_file_content_ << "\t\tstart ";
    switch (number_of_actions) {
        case 2: {
            domain_file_content_ << "process - state\n";
            break;
        }
        case 3: {
            domain_file_content_ << "setCount process - state\n";
            break;
        }
        default: {
            domain_file_content_ << "setSize setCount process - state\n";
            break;
        }
    }
    domain_file_content_ << "\t)\n";
}

void PDDLGenerator::generate_actions(std::size_t number_of_actions)
{
    //4,3,2 startJoin, different versions
    //default case is 4
    switch (number_of_actions) {
        case 2: {
            domain_file_content_ << "\t(:action startJoin\n "
                                    << "\t\t:parameters (?x ?y - relation ?a ?b - view ?s1 ?s2 ?resultSize - size "
                                    << "?ca ?cb ?result - count)\n"
                                    << "\t\t:precondition (and\n"
                                    << "\t\t\t(not (= ?a ?b))\n"
                                    << "\t\t\t(not (= ?x ?y))\n"
                                    << "\n"
                                    << "\t\t\t(isInView ?x ?a)\n"
                                    << "\t\t\t(isInView ?y ?b)\n"
                                    << "\n"
                                    << "\t\t\t(hasJoin ?x ?y)\n"
                                    << "\n"
                                    << "\t\t\t(currentState start)\n"
                                    << "\n"
                                    << "\t\t\t(stillToDo zero)\n"
                                    << "\n"
                                    << "\t\t\t(currentSize ?a ?s1)\n"
                                    << "\t\t\t(currentSize ?b ?s2)\n"
                                    << "\n"
                                    << "\t\t\t(resultSize ?s1 ?s2 ?resultSize)\n"
                                    << "\n"
                                    << "\t\t\t(currentCount ?a ?ca)\n"
                                    << "\t\t\t(currentCount ?b ?cb)\n"
                                    << "\n"
                                    << "\t\t\t(plus ?ca ?cb ?result)\n"
                                    << "\t\t)\n"
                                    << "\t\t:effect (and\n"
                                    << "\t\t\t(currentFrom ?b)\n"
                                    << "\t\t\t(currentTo ?a)\n"
                                    << "\n"
                                    << "\t\t\t(currentState process)\n"
                                    << "\t\t\t(not (currentState start))\n"
                                    << "\n"
                                    << "\t\t\t(currentSize ?a ?resultSize)\n"
                                    << "\t\t\t(not (currentSize ?a ?s1))\n"
                                    << "\n"
                                    << "\t\t\t(biggestCount ?result)\n"
                                    << "\n"
                                    << "\t\t\t(currentCount ?b zero)\n"
                                    << "\t\t\t(not (currentCount ?b ?cb))\n"
                                    << "\n"
                                    << "\t\t\t(currentCount ?a ?result)\n"
                                    << "\t\t\t(not (currentCount ?a ?ca))\n"
                                    << "\n"
                                    << "\t\t\t(stillToDo ?cb)\n"
                                    << "\t\t\t(not (stillToDo zero))\n"
                                    << "\n"
                                    << "\t\t\t(increase (total-cost) (sum ?s1 ?s2))\n"
                                    << "\t\t)\n"
                                    << "\t)\n"
                                    << "\n";
            break;
        }
        case 3: {
            domain_file_content_ << "\t(:action startJoin\n "
                                    << "\t\t:parameters (?x ?y - relation ?a ?b - view ?s1 ?s2 ?result - size)\n"
                                    << "\t\t:precondition (and\n"
                                    << "\t\t\t(not (= ?a ?b))\n"
                                    << "\t\t\t(not (= ?x ?y))\n"
                                    << "\n"
                                    << "\t\t\t(isInView ?x ?a)\n"
                                    << "\t\t\t(isInView ?y ?b)\n"
                                    << "\n"
                                    << "\t\t\t(hasJoin ?x ?y)\n"
                                    << "\n"
                                    << "\t\t\t(currentState start)\n"
                                    << "\n"
                                    << "\t\t\t(stillToDo zero)\n"
                                    << "\n"
                                    << "\t\t\t(currentSize ?a ?s1)\n"
                                    << "\t\t\t(currentSize ?b ?s2)\n"
                                    << "\n"
                                    << "\t\t\t(resultSize ?s1 ?s2 ?result)\n"
                                    << "\t\t)\n"
                                    << "\t\t:effect (and\n"
                                    << "\t\t\t(currentFrom ?b)\n"
                                    << "\t\t\t(currentTo ?a)\n"
                                    << "\n"
                                    << "\t\t\t(currentState setCount)\n"
                                    << "\t\t\t(not (currentState start))\n"
                                    << "\n"
                                    << "\t\t\t(currentSize ?a ?result)\n"
                                    << "\t\t\t(not (currentSize ?a ?s1))\n"
                                    << "\n"
                                    << "\t\t\t(increase (total-cost) (sum ?s1 ?s2))\n"
                                    << "\t\t)\n"
                                    << "\t)\n"
                                    << "\n";
            break;
        }
        default: {
            domain_file_content_ << "\t(:action startJoin\n "
                                    << "\t\t:parameters (?x ?y - relation ?a ?b - view)\n"
                                    << "\t\t:precondition (and\n"
                                    << "\t\t\t(not (= ?a ?b))\n"
                                    << "\t\t\t(not (= ?x ?y))\n"
                                    << "\n"
                                    << "\t\t\t(isInView ?x ?a)\n"
                                    << "\t\t\t(isInView ?y ?b)\n"
                                    << "\n"
                                    << "\t\t\t(hasJoin ?x ?y)\n"
                                    << "\n"
                                    << "\t\t\t(currentState start)\n"
                                    << "\n"
                                    << "\t\t\t(stillToDo zero)\n"
                                    << "\t\t)\n"
                                    << "\t\t:effect (and\n"
                                    << "\t\t\t(currentFrom ?b)\n"
                                    << "\t\t\t(currentTo ?a)\n"
                                    << "\n"
                                    << "\t\t\t(currentState setSize)\n"
                                    << "\t\t\t(not (currentState start))\n"
                                    << "\n"
                                    << "\t\t\t(increase (total-cost) 0)\n"
                                    << "\t\t)\n"
                                    << "\t)\n"
                                    << "\n";
            break;
        }
    }
    //4 setResultsizes
    if (number_of_actions == 4) {
        domain_file_content_ << "\t(:action setResultSize\n "
                                << "\t\t:parameters (?a ?b - view ?s1 ?s2 ?result - size)\n"
                                << "\t\t:precondition (and\n"
                                << "\t\t\t(not (= ?a ?b))\n"
                                << "\n"
                                << "\t\t\t(currentFrom ?b)\n"
                                << "\t\t\t(currentTo ?a)\n"
                                << "\n"
                                << "\t\t\t(currentSize ?a ?s1)\n"
                                << "\t\t\t(currentSize ?b ?s2)\n"
                                << "\n"
                                << "\t\t\t(resultSize ?s1 ?s2 ?result)\n"
                                << "\n"
                                << "\t\t\t(currentState setSize)\n"
                                << "\n"
                                << "\t\t\t(stillToDo zero)\n"
                                << "\t\t)\n"
                                << "\t\t:effect (and\n"
                                << "\t\t\t(currentSize ?a ?result)\n"
                                << "\t\t\t(not (currentSize ?a ?s1))\n"
                                << "\n"
                                << "\t\t\t(currentState setCount)\n"
                                << "\t\t\t(not (currentState setSize))\n"
                                << "\n"
                                << "\t\t\t(increase (total-cost) (sum ?s1 ?s2))\n"
                                << "\t\t)\n"
                                << "\t)\n"
                                << "\n";
    }

    //4,3: setCounts
    if (number_of_actions == 3 || number_of_actions == 4) {
        domain_file_content_ << "\t(:action setCounts\n "
                                << "\t\t:parameters (?a ?b - view ?ca ?cb ?result - count)\n"
                                << "\t\t:precondition (and\n"
                                << "\t\t\t(not (= ?a ?b))\n"
                                << "\n"
                                << "\t\t\t(currentFrom ?b)\n"
                                << "\t\t\t(currentTo ?a)\n"
                                << "\n"
                                << "\t\t\t(currentCount ?a ?ca)\n"
                                << "\t\t\t(currentCount ?b ?cb)\n"
                                << "\n"
                                << "\t\t\t(plus ?ca ?cb ?result)\n"
                                << "\n"
                                << "\t\t\t(currentState setCount)\n"
                                << "\n"
                                << "\t\t\t(stillToDo zero)\n"
                                << "\t\t)\n"
                                << "\t\t:effect (and\n"
                                << "\t\t\t(currentState process)\n"
                                << "\t\t\t(not (currentState setCount))\n"
                                << "\t\t\t(biggestCount ?result)\n"
                                << "\n"
                                << "\t\t\t(currentCount ?b zero)\n"
                                << "\t\t\t(not (currentCount ?b ?cb))\n"
                                << "\n"
                                << "\t\t\t(currentCount ?a ?result)\n"
                                << "\t\t\t(not (currentCount ?a ?ca))\n"
                                << "\n"
                                << "\t\t\t(stillToDo ?cb)\n"
                                << "\t\t\t(not (stillToDo zero))\n"
                                << "\n"
                                << "\t\t\t(increase (total-cost) 0)\n"
                                << "\t\t)\n"
                                << "\t)\n"
                                << "\n";
    }
    //add join_step
    domain_file_content_ << "\t(:action joinStep\n "
                            << "\t\t:parameters (?a ?b - view ?r - relation ?cb ?cLow - count)\n"
                            << "\t\t:precondition (and\n"
                            << "\t\t\t(not (= ?a ?b))\n"
                            << "\n"
                            << "\t\t\t(plus ?cLow one ?cb)\n"
                            << "\n"
                            << "\t\t\t(currentFrom ?b)\n"
                            << "\t\t\t(currentTo ?a)\n"
                            << "\n"
                            << "\t\t\t(currentState process)\n"
                            << "\n"
                            << "\t\t\t(isInView ?r ?b)\n"
                            << "\n"
                            << "\t\t\t(stillToDo ?cb)\n"
                            << "\t\t)\n"
                            << "\t\t:effect (and\n"
                            << "\t\t\t(stillToDo ?cLow)\n"
                            << "\t\t\t(not (stillToDo ?cb))\n"
                            << "\n"
                            << "\t\t\t(not (isInView ?r ?b))\n"
                            << "\t\t\t(isInView ?r ?a)\n"
                            << "\n"
                            << "\t\t\t(when (= ?cb one)\n"
                            << "\t\t\t\t(and (currentState start)\n"
                            << "\t\t\t\t\t(not (currentState process))\n"
                            << "\t\t\t\t\t(not (currentFrom ?b))\n"
                            << "\t\t\t\t\t(not (currentTo ?a))))\n"
                            << "\n"
                            << "\t\t\t(increase (total-cost) 0)\n"
                            << "\t\t)\n"
                            << "\t)\n";
}

/*======================================================================================================================
* Problem File Generation - HelperMethods
*=====================================================================================================================*/

void PDDLGenerator::generate_init_part(const QueryGraph &G)
{
    std::vector<std::string> relations = get_relations(G);
    this->relations_ = relations;
    std::vector<std::string> views = generate_views(relations);
    this->views_ = views;

    init_section_problem_file_ << "\t(:init\n"
            << "\t\t(stillToDo zero)\n"
            << "\t\t(currentState start)\n"
            << "\t\t(biggestCount zero)\n"
            << "\n";

    generate_isInView_facts(relations, views);
    init_section_problem_file_ << "\n";

    generate_currentSize_facts(G);
    init_section_problem_file_ << "\n";

    generate_currentCount_facts(views);
    init_section_problem_file_ << "\n";

    generate_hasJoin_facts(G.joins());
    init_section_problem_file_ << "\n";

    generate_plus_facts(relations.size());
    init_section_problem_file_ << "\n"
            << "\t\t(=total-cost 0)\n";

    std::pair<std::string, std::string> sums_sizes = generate_sum_and_resultSize_facts(G);
    init_section_problem_file_ << sums_sizes.first
                                << "\n"
                                << sums_sizes.second
                                << "\t)\n";
}

void PDDLGenerator::generate_objects_part(std::vector<std::string> &relations, std::vector<std::string> &views,
                                                 std::vector<std::string> &sizes)
{
    problem_file_content_ << "\t(:objects\n"
                            << "\t\t";
    for(const auto &relation: relations) problem_file_content_ << relation << " ";
    problem_file_content_ << "- relation\n"
                            << "\t\t";
    for(const auto &view: views) problem_file_content_ << view << " ";
    problem_file_content_ << "- view\n"
                            << "\t\t";
    for(const auto &size: sizes) problem_file_content_ << size << " ";
    problem_file_content_ << "- size\n"
                            << "\t)\n";
}

void PDDLGenerator::generate_goal_part(std::size_t number_of_relations)
{
    problem_file_content_ << "\t(:goal\n"
                            << "\t\t(and\n"
                            << "\t\t\t(stillToDo zero)\n"
                            << "\t\t\t(biggestCount " << counts_[number_of_relations] << ")\n"
                            << "\t\t)\n"
                            << "\t)\n";
}

/*======================================================================================================================
* Problem File Generation - HelperMethods for generate_init_part
*=====================================================================================================================*/

void PDDLGenerator::generate_isInView_facts(std::vector<std::string> relations,
                                                                std::vector<std::string> views)
{
    for (std::size_t i = 0; i < relations.size(); ++i) {
        init_section_problem_file_ << "\t\t(isInView "
                                    << relations[i]
                                    << " "
                                    << views[i]
                                    << ")\n";
    }
}

void PDDLGenerator::generate_currentSize_facts(const QueryGraph &G)
{
    for (auto source : G.sources()) {
        auto base_table = cast<BaseTable>(source);
        Subproblem P(0);
        P.set(source->id(), true);
        auto model = cardinality_estimator_.estimate_scan(G, P);
        init_section_problem_file_ << "\t\t(currentSize v"
                                   << base_table->table().name
                                   << " s"
                                   << base_table->table().name
                                   << cardinality_estimator_.predict_cardinality(*model)
                                   << ")\n";
    }
}

void PDDLGenerator::generate_currentCount_facts(std::vector<std::string> &views)
{
    for (auto &view : views) {
        init_section_problem_file_ << "\t\t(currentCount "
                                    << view
                                    << " one)\n";
    }
}

void PDDLGenerator::generate_hasJoin_facts(const std::vector<Join *> &joins)
{
    for (auto &join : joins) {
        auto join_partners = join->sources();
        auto left_table = cast<BaseTable>(join_partners[0]);
        auto right_table = cast<BaseTable>(join_partners[1]);
        std::string left_table_name = left_table->table().name;
        std::string right_table_name = right_table->table().name;
        init_section_problem_file_ << "\t\t(hasJoin "
                                    << left_table_name
                                    << " "
                                    << right_table_name
                                    << ")\n"
                                    << "\t\t(hasJoin "
                                    << right_table_name
                                    << " "
                                    << left_table_name
                                    << ")\n";
    }
}

void PDDLGenerator::generate_plus_facts(std::size_t number_of_relations)
{
    init_section_problem_file_ << "\t\t(plus zero one one)\n";
    for (std::size_t i = 1; i < number_of_relations; ++i) {
        for (std::size_t j = 1; j < number_of_relations - i + 1; ++j) {
            init_section_problem_file_ << "\t\t(plus "
                                          << counts_[i]
                                          << " "
                                          << counts_[j]
                                          << " "
                                          << counts_[i + j]
                                          << ")\n";
        }
    }
}

std::pair<std::string, std::string>
PDDLGenerator::generate_sum_and_resultSize_facts(const QueryGraph &G)
{
    RecursiveFactsGenerator rfg(cardinality_estimator_);
    std::vector<DataSource *> sources = G.sources();
    for (auto source: sources) {
        std::vector<DataSource *> source_vector = {source};
        rfg.generate_facts(G, source_vector);
    }
    std::ostringstream &sum_facts = rfg.sum_facts_stream;
    std::ostringstream &resultSize_facts = rfg.resultSize_facts_stream;
    for (const auto &size: rfg.sizes) sizes_.push_back(size);
    std::sort(sizes_.begin(), sizes_.end());
    return std::make_pair(sum_facts.str(), resultSize_facts.str());
}

/*======================================================================================================================
* Helper methods
*=====================================================================================================================*/
std::vector<std::string> PDDLGenerator::get_relations(const QueryGraph &G)
{
    std::vector<DataSource *> sources = G.sources();
    std::vector<std::string> relations;
    for (auto source: sources) {
        auto base_table = cast<BaseTable>(source);
        relations.emplace_back(base_table->table().name);
    }
    std::sort(relations.begin(), relations.end());
    return relations;
}

std::vector<std::string> PDDLGenerator::generate_views(std::vector<std::string> &relations)
{
    std::vector<std::string> views(relations.size());
    for (std::size_t i = 0; i < relations.size(); i++) {
        std::ostringstream view;
        view << "v" << relations[i];
        views[i].assign(view.str());
    }
    return views;
}




void PDDLGenerator::dump() const { std::cout << "This is a PDDLGenerator"; }
