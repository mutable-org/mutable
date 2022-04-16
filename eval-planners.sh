#!/bin/bash

# Trap on CTRL-C / SIGINT
trap "exit 1" SIGINT

set -o pipefail -o nounset

source planner-configs.sh


########################################################################################################################
# Globals
########################################################################################################################

# Reset timer
SECONDS=0

# Initialize Bash's PRNG engine
RANDOM=42

# Timeout for single invocations
# TIMEOUT=15s
TIMEOUT=5s

# Maximum number of timeouts allowed per planner configuration.  If this value is reached, the configuration is skipped.
MAX_TIMEOUTS_PER_CONFIG=1

# Number of repetitions per query
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
    # [chain]=63
    # [cycle]=63
    # [star]=28
    # [clique]=19
    [chain]=31
    [cycle]=31
    [star]=22
    [clique]=14
)

ORDERED_PLANNERS=(
    ###### Traditional Planners #####
    # "DPsub"
    "DPccp"
    # "TDMinCutAGaT"
    # "IKKBZ"
    "linDP"
    "GOO"
    ##### Heuristic Search #####
    # BottomUp
    ## A*
    "BU-A*-zero"
    "BU-A*-avg_sel"
    "BU-A*-GOO"
    ## beam
    "BU-beam-zero"
    "BU-beam-avg_sel"
    "BU-beam-GOO"
    ## relative beam
    "BU-rel_beam-zero"
    # TopDown
    ## A*
    "TD-A*-zero"
    "TD-A*-sum"
    "TD-A*-GOO"
    ## beam
    "TD-beam-zero"
    "TD-beam-sum"
    "TD-beam-GOO"
    ## relative beam
    "TD-rel_beam-zero"
)

declare -A TOPOLOGY_STEPS=(
    [chain]=3
    [cycle]=3
    [star]=2
    [clique]=1
)


########################################################################################################################
# Helper functions
########################################################################################################################

function trim()
{
    local var="$*"
    # remove leading whitespace characters
    var="${var#"${var%%[![:space:]]*}"}"
    # remove trailing whitespace characters
    var="${var%"${var##*[![:space:]]}"}"
    printf '%s' "$var"
}

function has_planners_to_run()
{
    for COUNT in "${PLANNER_TIME_OUTS[@]}";
    do
        if [ ${COUNT} -lt ${MAX_TIMEOUTS_PER_CONFIG} ];
        then
            return 0 # SUCCESS: found planner to run
        fi
    done
    return 1 # FAILURE: no planners to run
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

        # Initialize timeout counters
        declare -A PLANNER_TIME_OUTS
        for PLANNER in "${ORDERED_PLANNERS[@]}";
        do
            PLANNER_TIME_OUTS[${PLANNER}]=0
        done

        for ((N=${MIN_RELATIONS}; N <= ${MAX_RELATIONS}; N = N + ${STEP}));
        do
            NAME="${TOPOLOGY}-${N}"
            echo "Evaluate ${NAME}"

            for ((R=0; R < ${REPETITIONS_PER_NUM_RELATIONS}; ++R));
            do
                if ! has_planners_to_run;
                then
                    >&2 echo '` No more planners to run.  Skipping rest of topology.'
                    break 2;
                fi

                # Generate problem
                SEED=${RANDOM}
                echo -n '` '
                python3 querygen.py -t "${TOPOLOGY}" -n ${N} --count=${QUERY_REPEAT_COUNT} | tr -d '\n'
                (time build/release/bin/cardinality_gen \
                    ${FLAGS} \
                    --seed "${SEED}" \
                    --min "${MIN_CARDINALITY}" \
                    --max "${MAX_CARDINALITY}" \
                    "${NAME}.schema.sql" \
                    "${NAME}.query.sql") \
                    3>&1 1>"${NAME}.cardinalities.json" 2>&3 3>&- | ack real | cut -d$'\t' -f 2 | (read -r TIME; echo " (${TIME})";)

                # Evaluate problem with each planner
                echo '` Running planner'
                for PLANNER in "${ORDERED_PLANNERS[@]}";
                do
                    if [ ${PLANNER_TIME_OUTS[${PLANNER}]} -ge ${MAX_TIMEOUTS_PER_CONFIG} ];
                    then
                        >&2 echo "  \` Skipping configuration '${PLANNER}' because of too many timeouts."
                        continue
                    fi

                    if [ ! -v 'PLANNER_CONFIGS[$PLANNER]' ];
                    then
                        >&2 echo "ERROR: no configuration found for ${PLANNER}"
                        continue
                    fi
                    PLANNER_CONFIG=${PLANNER_CONFIGS[$PLANNER]}

                    unset COST
                    unset TIME
                    set +m
                    # The following command needs pipefail
                    timeout --signal=TERM --kill-after=3s ${TIMEOUT} taskset -c 2 ${BIN} \
                        --quiet --dryrun --times \
                        --plan-table-las \
                        ${PLANNER_CONFIG} \
                        --cardinality-estimator InjectionCardinalityEstimator \
                        --use-cardinality-file "${NAME}.cardinalities.json" \
                        "${NAME}.schema.sql" \
                        "${NAME}.query.sql" \
                        | grep -e 'Plan enumeration:' -e 'Plan cost:' \
                        | cut --delimiter=':' --fields=2 \
                        | paste -sd ' \n' \
                        | while read -r COST TIME; do echo "${TOPOLOGY},${N},${PLANNER},${COST},${TIME},${SEED}" >> "${CSV}"; done
                    # Save and aggregate PIPESTATUS
                    SAVED_PIPESTATUS=("${PIPESTATUS[@]}")
                    TIMEOUT_RET=${SAVED_PIPESTATUS[0]}
                    ERR=0
                    for RET in "${SAVED_PIPESTATUS[@]}";
                    do
                        if [ ${RET} -ne 0 ];
                        then
                            ERR=${RET}
                            break
                        fi
                    done

                    if [ ${TIMEOUT_RET} -eq 124 ] || [ ${TIMEOUT_RET} -eq 137 ]; # timed out
                    then
                        >&2 echo "  \` Configuration '${PLANNER}' timed out."
                        ((PLANNER_TIME_OUTS[${PLANNER}]++)) # increment number of timeouts
                    elif [ ${ERR} -ne 0 ];
                    then
                        >&2 echo "  \` Unexpected termination: ERR=${ERR}, PIPESTATUS=(${SAVED_PIPESTATUS[@]}), configuration '${PLANNER}':"
                        >&2 cat << EOF
    timeout --signal=TERM --kill-after=3s ${TIMEOUT} taskset -c 2 ${BIN} \
--quiet --dryrun --times \
--plan-table-las \
${PLANNER_CONFIG} \
--cardinality-estimator InjectionCardinalityEstimator \
--use-cardinality-file "${NAME}.cardinalities.json" \
"${NAME}.schema.sql" \
"${NAME}.query.sql"
EOF
                    fi

                    # set +x;
                done
            done

            echo '` Cleanup files.'
            rm -f "${NAME}.schema.sql"
            rm -f "${NAME}.query.sql"
            rm -f "${NAME}.cardinalities.json"
        done
    done

    TIME_TOTAL=$SECONDS
    echo "All measurements have been written to '${CSV}'"
    echo "Evaluation took " $(date -ud "@${TIME_TOTAL}" '+%T')
}

main "$@"
