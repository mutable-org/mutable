#pragma once

#include <vector>
#include <string>
#include <mutable/catalog/CardinalityEstimator.hpp>
#include <mutable/mutable.hpp>


namespace m {

/** Translates a QueryGraph into a PDDL domain file and a PDDL problem file */
struct PDDLGenerator
{
    private:
    std::vector<std::string> counts_ = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight",
                                        "nine", "ten", "eleven", "twelve", "thirteen", "fourteen", "fifteen",
                                        "sixteen", "seventeen", "eightteen", "nineteen", "twenty", "twentyone",
                                        "twentytwo", "twentythree", "twentyfour", "twentyfive"};
                                        ///< counts facts for the generation
    std::vector<std::string> views_; ////< the view facts for the problem file generation
    std::vector<std::string> sizes_; ////< the size facts for the problem file generation
    std::vector<std::string> relations_; ////< the relation facts for the problem file generation
    std::ostringstream domain_file_content_; ////< stringstream for the domain file
    std::ostringstream problem_file_content_; ////< stringstream for the problem file
    std::ostringstream init_section_problem_file_; ////< stringstream for the init section of the problem file
    const CardinalityEstimator &cardinality_estimator_; ///< the estimator used for the generation
                                                        ///< of the PDDL problem file
    Diagnostic diag_; ////< Diagnostic object used for error logs in this PPDLGenerator

    public:
    PDDLGenerator(const CardinalityEstimator &CE, Diagnostic diag) : cardinality_estimator_(CE), diag_(diag) { }

    /**
     * Generates the PDDL domain and the PDDL problem file for the given `QueryGraph`
     * @param G the `QueryGraph` for which the files should be created
     * @param number_of_actions type of file that should be created, 2,3 or 4 Actions
     */
    void generate_files(const QueryGraph &G, std::size_t number_of_actions, std::filesystem::path domain_path,
                        std::filesystem::path problem_path);
    void generate_files(const QueryGraph &G, std::filesystem::path domain_path, std::filesystem::path problem_path);

    private:
    /**
     * Generates a PDDL domain file with the domainname:<number_of_actions>ActionsDomain<number_of_relations>
     * @param number_of_relations number of relations in the problem
     * @param number_of_actions type of model to create a domain for: 2,3 oder 4 actions
     * @param domain_path directory in which to save the files
     */
    void generate_domain_file(std::size_t number_of_relations, std::size_t number_of_actions, std::filesystem::path domain_path);

    /**
     * Generates a PDDL problem file for the `QueryGraph` G and saves it in problem_path with the domainname:
     * <number_of_actions>ActionsDomain<number_of_relations>, where number_of_relations is obtained from `G`
     * @param G the `QueryGraph` to create the problem file for
     * @param number_of_actions for the creation of the filename, 2,3 or 4; 0 will create all three models
     * @param problem_path the directory in which to save the problem file
     */
    void generate_problem_file(const QueryGraph &G, std::size_t number_of_actions, std::filesystem::path &problem_path);


    /*==================================================================================================================
     * Domain File Generation - HelperMethods
     *================================================================================================================*/
    /** Generates the requirements section for the PDDL domain file and appends it to the domain file stringstream*/
    void generate_requirements();

    /** Generates the types section for the PDDL domain file and appends it to the domain file stringstream*/
    void generate_types();

    /** Generates the predicates section for the PDDL domain file and appends it to the domain file stringstream*/
    void generate_predicates();

    /** Generates the functions section for the PDDL domain file and appends it to the domain file stringstream*/
    void generate_functions();

    /**
     * Generates the constants section for the PDDL domain file and appends it to the domain file stringstream
     * @param number_of_relations number of relations in the problem
     * @param number_of_actions type of domain that should be created, 2,3 or 4 Actions
     */
    void generate_constants(std::size_t number_of_relations, std::size_t number_of_actions);

    /**
     * Generates the actions for the PDDL domain file and appends it to the domain file stringstream
     * @param number_of_actions type of domain that should be created, 2,3, or 4 Actions
     */
    void generate_actions(std::size_t number_of_actions);


    /*==================================================================================================================
     * Problem File Generation - HelperMethods
     *================================================================================================================*/
    /** Generates the init section for the PDDL problem file for the `QueryGraph` G and appends it to the problem file
     * stringstream */
    void generate_init_part(const QueryGraph &G);

    /**
     * Generates the objects section for the PDDL problem file and appends it to the problem file stringstream
     * @param relations relations in this problem, e.g. A, B, C, ...
     * @param views view literals in this problem, e.g. vA, vB, vC, ...
     * @param sizes size literals in this problem, e.g. sA, sB, sC, sAB, sABC, ...
     */
    void generate_objects_part(std::vector<std::string> &relations, std::vector<std::string> &views,
                                      std::vector<std::string> &sizes);

    /** Generates the goal section for the PDDL problem file depending on the number of relations in the problem
     * and adds it to the problem file stringstream */
    void generate_goal_part(std::size_t number_of_relations);


    /*==================================================================================================================
     * Problem File Generation - HelperMethods for generate_init_part
     *================================================================================================================*/
    /**
     * Generates the isInView facts, expects relations and views to be sorted in the same order,
     * i.e. generates the facts (isInView relations[i] views[i]) and appends them to the init section stringstream
     * @param relations the relation literals in this problem
     * @param views the view literals in this problem
     */
    void generate_isInView_facts(std::vector<std::string> relations, std::vector<std::string> views);

    /**
     * Generates the currentSize facts for the problem by constructing the size literals with the help of the cardinality
     * estimator, i.e. generates the facts (currentSize "v"+relation_name "s"+relation_name+size) and appends them to
     * the  init section stringstream
     * @param G the `QueryGraph` of this problem
     */
    void generate_currentSize_facts(const QueryGraph &G);


    /**
     * Generates the currentCount facts, initializes all with count one, i.e. (currentCount views[i] one) and appends
     * them to the  init section stringstream
     * @param views the view literals of this problem
     * @return the list of currentCount facts for this problem
     */
    void generate_currentCount_facts(std::vector<std::string> &views);

    /**
     * Generates the hasJoin facts, always generates two facts for a Join(A,B): (hasJoin A B), (hasJoin B A) and appends
     * them to the  init section stringstream
     * @param joins the list of joins for this problem
     */
    void generate_hasJoin_facts(const std::vector<Join*> &joins);

    /**
     * Generates the plus facts, uses the first number_of_relations entries of `PDDLGenerator`.counts_ and appends
     * them to the  init section stringstream
     * @param number_of_relations the number of relations for this problem
     */
    void generate_plus_facts(std::size_t number_of_relations);

    /**
     * Generates the sum and resultSize facts for a given `QueryGraph` by using an instance of`RecursiveFactsGenerator`
     * @param G the `QueryGraph` to create the facts for
     * @return a pair of lists, the first containing the sum facts, the second containing the resultSize facts
     */
    std::pair<std::string, std::string> generate_sum_and_resultSize_facts(const QueryGraph &G);


    /*================================================================================================================
     * Helper methods
     *================================================================================================================*/
    /**
     * gets all relation table names from a given `QueryGraph`
     * @param G the `QueryGraph` to extract the relations from, can only have `BaseTable`s
     * @return the list of tables names for the relations in G
     */
    std::vector<std::string> get_relations(const QueryGraph &G);

    /**
     * Generates a view literal for each passed relations, e.g. view literal "vA" for relation "A"
     * @param relations the list of relations for this problem
     * @return the list of view literals for this problem
     */
    std::vector<std::string> generate_views(std::vector<std::string> &relations);

    void dump() const;
};

}
