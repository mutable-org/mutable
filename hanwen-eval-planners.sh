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
TIMEOUT=1800s

# Maximum number of timeouts allowed per planner configuration.  If this value is reached, the configuration is skipped.
MAX_TIMEOUTS_PER_CONFIG=3

# Number of repetitions per query
# Used in the querygen, need more investigation
QUERY_REPEAT_COUNT=1


HANWEN_MAKE_FOLDER=cmake-build-debug
BIN=cmake-build-debug/bin/shell
CSV=planner-benchmark.csv
CORRELATED=1


# outside large loop execution time
REPETITIONS_PER_NUM_RELATIONS=1

MIN_CARDINALITY=10
MAX_CARDINALITY=10000


MIN_RELATIONS=3
# Associative array mapping topologies to their max. number of relations tested
declare -A TOPOLOGIES=(
#   [chain]=63
#   [cycle]=63
    [star]=15
   [clique]=15
)
#declare -A T0OPOLOGIES=(
#    [chain]=50
#    [cycle]=100
#    [star]=100
#    [clique]=100
#)

declare -A TOPOLOGY_STEPS=(
    [chain]=1
    [cycle]=1
    [star]=1
    [clique]=1
)

ORDERED_PLANNERS=(
    ###### HANWEN Manuelly Test #####
     "DPccp"
     "BU-A*-zero"
     "BIDIRECTIONAL"
     "BIDIRECTIONAL"
     "BIDIRECTIONAL"
     "BIDIRECTIONAL"
     "BIDIRECTIONAL"

#    "IKKBZ"
    # "GOO"
#    "TD-cleanAStar-zero"
#    "BU-cleanAStar-zero"

#    "BU-IDDFS-zero"
#    "BU-A*-zero"
#    "BU-A*-sum"
#  "BU-hanwen-layered-zero"
#  "BU-hanwen-layered2-zero"
#  "BU-hanwen-layered3-zero"

#  "BU-hanwen-layered-sorted-zero"
#  "BU-hanwen-layered-sorted2-zero"
# "BU-hanwen-layered-sorted2-zero"
# "BU-hanwen-layered-sorted3-zero"
# "BU-hanwen-layered-sorted2-sum"
# "BU-hanwen-layered-sorted3-sum"
#  "BU-hanwen-layered-sorted4-zero"
#  "BU-hanwen-layered-sorted5-zero"
#  "BU-hanwen-layered-sorted6-zero"

#  "BU-hanwen-layered-sorted-dynamic2-zero"
#   "BU-hanwen-layered-sorted-dynamic3-zero"

#"TD-hanwen-layered-sorted2-zero"
#"TD-hanwen-layered-sorted3-zero"
#"TD-hanwen-layered-sorted6-zero"
#"TD-hanwen-layered-sorted10-zero"
#"TD-hanwen-layered-sorted15-zero"
#"TD-hanwen-layered-sorted25-zero"
#
#"TD-hanwen-layered-sorted2-sum"
#"TD-hanwen-layered-sorted3-sum"
#"TD-hanwen-layered-sorted6-sum"
#"TD-hanwen-layered-sorted10-sum"
#"TD-hanwen-layered-sorted15-sum"
#"TD-hanwen-layered-sorted25-sum"

#  "TD-hanwen-layered-zero"

#    "BU-BIDIRECTIONAL-zero"
#    "BU-LAYEREDBIDIRECTIONAL-zero"

#    "BU-A*-sum"
#    "BU-beam-zero"
#    "TD-beam-zero"
#    "BU-beam-hanwen-zero"
#    "TD-beam-hanwen-zero"
#    "BU-beam-sum"
#    "BU-A*-sqrt_sum"
#    "BU-A*-scaled_sum"
#
#    "TD-A*-sum"
#    "TD-A*-sqrt_sum"
#    "TD-A*-scaled_sum"


#    ###### Traditional Planners #####
#    "DPsub"
#    "DPccp"
#    "TDMinCutAGaT"
#    "IKKBZ"
#    "linDP"
#    "GOO"
#    ##### Heuristic Search #####
#    # BottomUp
#    ## A*
#    "BU-A*-zero"
#    "BU-A*-avg_sel"
#    "BU-A*-GOO"
#    "BU-A*-sum"
#    "BU-A*-sqrt_sum"
#    "BU-A*-scaled_sum"
#    ## beam
#    "BU-beam-zero"
#    "BU-beam-avg_sel"
#    "BU-beam-GOO"
#    ## relative beam
#    "BU-rel_beam-zero"
#    # TopDown
#    ## A*
#    "TD-A*-zero"
#    "TD-A*-sum"
#    "TD-A*-GOO"
#    ## beam
#    "TD-beam-zero"
#    "TD-beam-sum"
#    "TD-beam-GOO"
#    ## relative beam
#    "TD-rel_beam-zero"
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

##### parse command line arguments #####################################################################################
POSITIONAL_ARGS=()
unset RND
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
        RANDOM=$1
        echo "Seeding PRNG with $1."
        ;;
    2)
        RANDOM=$1
        CSV=$2
        echo "Seeding PRNG with $1."
        ;;
    *)
        >&2 echo "error: too many positional arguments"
        exit 1
        ;;
esac

##### assemble command line arguments for shell invocation #############################################################
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
        echo -e "\n\nEvaluate ${NAME}"

        for ((R=0; R < ${REPETITIONS_PER_NUM_RELATIONS}; ++R));
        do
            if ! has_planners_to_run;
            then
                >&2 echo '` No more planners to run.  Skipping rest of topology.'
                break 2;
            fi

            # Generate problem
            SEED=${RANDOM}
            echo -n "${TOPOLOGY} ${N}"
#            python3 querygen.py -t "${TOPOLOGY}" -n ${N} --count=${QUERY_REPEAT_COUNT} | tr -d '\n'
#            (time ${HANWEN_MAKE_FOLDER}/bin/cardinality_gen \
#                ${FLAGS} \
#                --seed "${SEED}" \
#                --min "${MIN_CARDINALITY}" \
#                --max "${MAX_CARDINALITY}" \
#                "${NAME}.schema.sql" \
#                "${NAME}.query.sql") \
#                3>&1 1>"${NAME}.cardinalities.json" 2>&3 3>&- | ack real | cut -d$'\t' -f 2 | (read -r TIME; echo " (${TIME})";)

            # Evaluate problem with each planner
            echo "\t Running planner"
            for PLANNER in "${ORDERED_PLANNERS[@]}";
            do
                if [ ${PLANNER_TIME_OUTS[${PLANNER}]} -ge ${MAX_TIMEOUTS_PER_CONFIG} ];
                then
                    >&2 echo '  `'" Skipping configuration '${PLANNER}' because of too many timeouts."
                    continue
                fi

                if [ ! -v 'PLANNER_CONFIGS[$PLANNER]' ];
                then
                    >&2 echo "ERROR: no configuration found for ${PLANNER}"
                    continue
                fi
                PLANNER_CONFIG=${PLANNER_CONFIGS[$PLANNER]}

                echo "${BIN} --quiet --dryrun --times --plan-table-las ${PLANNER_CONFIG} --cardinality-estimator Injected --use-cardinality-file " ${NAME}.cardinalities.json" --statistics "${NAME}.schema.sql" "${NAME}.query.sql" "

                unset COST
                unset TIME
                set +m
                # The following command needs pipefail
                ${BIN} \
                    --quiet --dryrun --times \
                    --plan-table-las \
                    ${PLANNER_CONFIG} \
                    --cardinality-estimator Injected \
                    --use-cardinality-file "${NAME}.cardinalities.json" \
                    --statistics \
                    "${NAME}.schema.sql" \
                    "${NAME}.query.sql" \
                    | grep -e '^Plan cost:' -e '^Plan enumeration:' \
                    | cut --delimiter=':' --fields=2 \
                    | tr -d ' ' \
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
                    >&2 echo '  `'" Configuration '${PLANNER}' timed out."
                    ((PLANNER_TIME_OUTS[${PLANNER}]++)) # increment number of timeouts
                elif [ ${ERR} -ne 0 ];
                then
                    >&2 echo '  `'" Unexpected termination: ERR=${ERR}, PIPESTATUS=(${SAVED_PIPESTATUS[@]}), configuration '${PLANNER}':"
                    >&2 cat << EOF
${BIN} \
--quiet --dryrun --times \
--plan-table-las \
${PLANNER_CONFIG} \
--cardinality-estimator Injected \
--use-cardinality-file "${NAME}.cardinalities.json" \
"${NAME}.schema.sql" \
"${NAME}.query.sql"
EOF
                fi

                # set +x;
            done
            echo "">>"${CSV}";
        done

#        echo '` Cleanup files.'
#        rm -f "${NAME}.schema.sql"
#        rm -f "${NAME}.query.sql"
#        rm -f "${NAME}.cardinalities.json"
    done
done

TIME_TOTAL=$SECONDS
echo "All measurements have been written to '${CSV}'"
echo "Evaluation took " $(date -ud "@${TIME_TOTAL}" '+%T')
