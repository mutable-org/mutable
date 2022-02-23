#!/bin/bash

# Trap on CTRL-C / SIGINT
trap "exit 1" SIGINT

set -o pipefail -o nounset


########################################################################################################################
# Globals
########################################################################################################################

# Reset timer
SECONDS=0

# Initialize Bash's PRNG engine
RANDOM=42

# timeout for single invocations
TIMEOUT=15s

# number of repetitions per query
QUERY_REPEAT_COUNT=3

BIN=build/release/bin/shell
CSV=planner-benchmark.csv
CORRELATED=1
MIN_RELATIONS=10
REPETITIONS_PER_NUM_RELATIONS=10

MIN_CARDINALITY=10
MAX_CARDINALITY=10000

# Associative array mapping topologies to their max. number of relations tested
declare -A TOPOLOGIES=(
    [chain]=63
    [cycle]=63
    [clique]=19
    [star]=28
)

declare -A PLANNER_CONFIGS=(
    ###### Traditional Planners #####
    [DPccp]="--plan-enumerator DPccp"
    [DPsub]="--plan-enumerator DPsubOpt"
    [IKKBZ]="--plan-enumerator IKKBZ"
    [linDP]="--plan-enumerator LinearizedDP"
    [GOO]="--plan-enumerator GOO"
    ##### Heuristic Search #####
    [A*-zero]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic zero --ai-search AStar"
    [beam-zero]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic zero --ai-search monotone_beam_search"
    [dynamic_beam-zero]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic zero --ai-search monotone_dynamic_beam_search"
    [A*-checkpoints]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic checkpoints --ai-search AStar"
    [beam-checkpoints]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic checkpoints --ai-search monotone_beam_search"
    [beam-scaled_sum]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic scaled_sum --ai-search monotone_beam_search"
    [dynamic_beam-scaled_sum]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic scaled_sum --ai-search monotone_dynamic_beam_search"
    [A*-GOO]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic GOO --ai-search AStar"
    [beam-GOO]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic GOO --ai-search monotone_beam_search"
    [dynamic_beam-GOO]="--plan-enumerator HeuristicSearch --ai-state SubproblemsBottomUp --ai-heuristic GOO --ai-search monotone_dynamic_beam_search"
)

declare -A SKIP_CONFIGS=(
    [chain-DPsub]=27
    [cycle-DPsub]=25
    [clique-DPsub]=16
    [clique-DPccp]=16
    [star-DPsub]=18
    [star-DPccp]=22
    [star-linDP]=24
    [star-dynamic_beam-scaled_sum]=24
)

declare -A TOPOLOGY_STEPS=(
    [chain]=3
    [cycle]=3
    [clique]=1
    [star]=2
)


########################################################################################################################
# Helper functions
########################################################################################################################

trim() {
    local var="$*"
    # remove leading whitespace characters
    var="${var#"${var%%[![:space:]]*}"}"
    # remove trailing whitespace characters
    var="${var%"${var##*[![:space:]]}"}"
    printf '%s' "$var"
}


########################################################################################################################
# main
########################################################################################################################

main() {
    ##### parse command line arguments #################################################################################
    POSITIONAL_ARGS=()
    while [[ $# -gt 0 ]]; do
        case $1 in
            --uncorrelated)
                CORRELATED=0
                shift # past argument
                ;;
            --correlated)
                CORRELATED=1
                shift # past argument
                ;;
            -*|--*)
                echo "Unknown option $1"
                exit 1
                ;;
            *)
                POSITIONAL_ARGS+=("$1") # save positional arg
                shift # past argument
                ;;
        esac
    done
    set -- "${POSITIONAL_ARGS[@]}" # restore positional parameters
    case $# in
        0)
            # nothing to be done
            ;;
        1)
            CSV=$1
            ;;
        *)
            >&2 echo "error: too many positional arguments"
            exit 1
            ;;
    esac

    ##### assemble command line arguments for shell invocation #########################################################
    FLAGS=
    case ${CORRELATED} in
        0) FLAGS="${FLAGS} --uncorrelated";;
        1) ;; # nothing to be done
    esac

    echo -n "Generating cardinalities with "
    case ${CORRELATED} in
        0) echo -n "uncorrelated";;
        1) echo -n "correlated";;
    esac
    echo " selectivities."

    # Truncate file
    echo "Writing measurements to '${CSV}'"
    echo "topology,size,planner,cost,time,seed" > "${CSV}"

    for TOPOLOGY in "${!TOPOLOGIES[@]}";
    do
        MAX_RELATIONS=${TOPOLOGIES[$TOPOLOGY]}
        STEP=${TOPOLOGY_STEPS[$TOPOLOGY]}
        for ((N=${MIN_RELATIONS}; N <= ${MAX_RELATIONS}; N = N + ${STEP}));
        do
            NAME="${TOPOLOGY}-${N}"
            echo "Evaluate ${NAME}"

            for ((R=0; R < ${REPETITIONS_PER_NUM_RELATIONS}; ++R));
            do
                # Generate problem
                SEED=${RANDOM}
                echo -n '` '
                python3 querygen.py -t "${TOPOLOGY}" -n ${N} --count=${QUERY_REPEAT_COUNT}
                build/release/bin/cardinality_gen \
                    ${FLAGS} \
                    --seed "${SEED}" \
                    --min "${MIN_CARDINALITY}" \
                    --max "${MAX_CARDINALITY}" \
                    "${NAME}.schema.sql" \
                    "${NAME}.query.sql" \
                    > "${NAME}.cardinalities.json"

                # Evaluate problem with each planner
                echo '` Running planner'
                for PLANNER in "${!PLANNER_CONFIGS[@]}";
                do
                    if [ ${SKIP_CONFIGS[${TOPOLOGY}-${PLANNER}]+_} ];
                    then
                        # entry found, check whether to skip
                        M=${SKIP_CONFIGS[${TOPOLOGY}-${PLANNER}]}
                        if [ ${N} -ge ${M} ];
                        then
                            continue # skip this planner
                        fi
                    fi

                    # set -x;
                    PLANNER_CONFIG=${PLANNER_CONFIGS[$PLANNER]}

                    unset COST
                    unset TIME
                    set +m;
                    timeout --signal=KILL ${TIMEOUT} taskset -c 2 ${BIN} \
                        --quiet --dryrun --times \
                        --plan-table-las \
                        ${PLANNER_CONFIG} \
                        --cardinality-estimator InjectionCardinalityEstimator \
                        --use-cardinality-file "${NAME}.cardinalities.json" \
                        "${NAME}.schema.sql" \
                        "${NAME}.query.sql" \
                        | grep -e 'Plan enumeration:' -e 'Calculated cost:' \
                        | cut --delimiter=':' --fields=2 \
                        | paste -sd ' \n' \
                        | while read -r COST TIME; do echo "${TOPOLOGY},${N},${PLANNER},${COST},${TIME},${SEED}" >> "${CSV}"; done
                    ERR=$?
                    if [ $ERR -ne 0 ]; then >&2 echo ${PLANNER_CONFIG}; fi

                    # set +x;
                done
            done

            echo '` Cleanup files.'
            rm "${NAME}.schema.sql"
            rm "${NAME}.query.sql"
            rm "${NAME}.cardinalities.json"
        done
    done

    TIME_TOTAL=$SECONDS
    echo "All measurements have been written to '${CSV}'"
    echo "Evaluation took " $(date -ud "@${TIME_TOTAL}" '+%T')
}

main "$@"
