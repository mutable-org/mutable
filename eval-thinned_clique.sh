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

# Timeout for single invocations
TIMEOUT=5s

BIN=build/release/bin/shell
CSV=planner-benchmark.csv
CORRELATED=1

MIN_RELATIONS=6
MAX_RELATIONS=38
STEP_RELATIONS=2

NUM_REPETITIONS=10
MAX_TIMEOUTS=3

MIN_CARDINALITY=10
MAX_CARDINALITY=10000

function usage() {
    cat <<EOF
USAGE:
    $0 <CSV> <SEED>
EOF
}

if [ $# != 2 ];
then
    >&2 echo "ERROR: requires seed"
    usage;
    exit 1;
fi

CSV=$1

RANDOM=$2
echo "Seeding PRNG with $2."


function run_experiment()
{
    SIZE=$1
    THINNING=$2
    SKEWNESS=$3
    SEED=$4

    OUT=$(python querygen.py --quiet --seed ${SEED} -t clique -n ${SIZE} --thinning=${THINNING} --skew=${SKEWNESS})
    RES=$?
    # echo "${OUT}"
    if [ ${RES} -ne 0 ];
    then
        >&2 echo "An error occurred, skipping"
        return
    fi

    SCHEMA=$(echo "${OUT}" | cut -d ' ' -f 1)
    QUERY=$(echo "${OUT}" | cut -d ' ' -f 2)

    DENSITY=$(echo "${OUT}" | cut -d ' ' -f 3)
    SKEWNESS=$(echo "${OUT}" | cut -d ' ' -f 4)
    P_VALUE=$(echo "${OUT}" | cut -d ' ' -f 5)
    STDDEV=$(echo "${OUT}" | cut -d ' ' -f 6)
    ENTROPY=$(echo "${OUT}" | cut -d ' ' -f 7)

    BASE=$(basename "${SCHEMA}" .schema.sql)
    JSON="${BASE}.cardinalities.json"

    echo '    ` '"Evaluate clique-${SIZE} with seed ${SEED} and thinning out ${THINNING} edges. "
    echo "        Density: ${DENSITY} Skew: ${SKEWNESS} p-value: ${P_VALUE} Stddev: ${STDDEV} Entropy: ${ENTROPY}"

    build/release/bin/cardinality_gen --seed ${SEED} "${SCHEMA}" "${QUERY}" > "${JSON}"

    unset COST
    unset TIME
    timeout --signal=TERM --kill-after=2s ${TIMEOUT} taskset -c 2 ${BIN} \
        --dryrun \
        --statistics \
        --cardinality-estimator Injected \
        --use-cardinality-file "${JSON}" \
        --plan-enumerator HeuristicSearch \
        --hs-expand BottomUpComplete \
        --hs-heuristic zero \
        --times \
        "${SCHEMA}" "${QUERY}" \
        | grep -e '^Plan cost:' -e '^Plan enumeration:' \
        | cut --delimiter=':' --fields=2 \
        | tr -d ' ' \
        | paste -sd ' \n' \
        | while read -r COST TIME; do \
            echo "clique,${SIZE},${THINNING},${DENSITY},${SKEWNESS},${P_VALUE},${STDDEV},${ENTROPY},BU-A*-zero,${COST},${TIME},${SEED}" >> "${CSV}"; \
        done
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
        >&2 echo '    `'" BU-A*-zero timed out."
        rm -f "${SCHEMA}" "${QUERY}" "${JSON}"
        return 1 # signal timeout to caller
    elif [ ${ERR} -ne 0 ];
    then
        >&2 echo '    `'" Unexpected termination: ERR=${ERR}, PIPESTATUS=(${SAVED_PIPESTATUS[@]}), configuration: BU-A*-zero"
        rm -f "${SCHEMA}" "${QUERY}" "${JSON}"
        return 137 # signal error to caller
    fi

    unset COST
    unset TIME
    timeout --signal=TERM --kill-after=3s ${TIMEOUT} taskset -c 2 ${BIN} \
        --dryrun \
        --statistics \
        --cardinality-estimator Injected \
        --use-cardinality-file "${JSON}" \
        --plan-enumerator DPccp \
        --times \
        "${SCHEMA}" "${QUERY}" \
        | grep -e '^Plan cost:' -e '^Plan enumeration:' \
        | cut --delimiter=':' --fields=2 \
        | tr -d ' ' \
        | paste -sd ' \n' \
        | while read -r COST TIME; do \
            echo "clique,${SIZE},${THINNING},${DENSITY},${SKEWNESS},${P_VALUE},${STDDEV},${ENTROPY},DPccp,${COST},${TIME},${SEED}" >> "${CSV}"; \
        done
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
        >&2 echo '    `'" DPccp timed out."
        rm -f "${SCHEMA}" "${QUERY}" "${JSON}"
        return 1 # signal timeout to caller
    elif [ ${ERR} -ne 0 ];
    then
        >&2 echo '    `'" Unexpected termination: ERR=${ERR}, PIPESTATUS=(${SAVED_PIPESTATUS[@]}), configuration: DPccp"
        rm -f "${SCHEMA}" "${QUERY}" "${JSON}"
        return 137 # signal error to caller
    fi

    # Clean up generated files
    rm -f "${SCHEMA}" "${QUERY}" "${JSON}"
}



echo "Writing measurements to '${CSV}'"
echo "topology,size,thinning,density,skewness,p_value,stddev,entropy,planner,cost,time,seed" > "${CSV}"

for ((size=MIN_RELATIONS; size <= MAX_RELATIONS; size+=STEP_RELATIONS));
do
    echo "Evaluating graph with ${size} relations."
    ((num_edges=size * (size - 1) / 2))
    ((min_edges=size - 1))
    ((max_thinning=num_edges - min_edges))
    # thinning_step=$(bc <<< "(${max_thinning} + 4) / 5")
    # echo "Evaluate clique-${size} with thinning out 1 to ${max_thinning} edges"
    # for ((thinning=thinning_step; thinning <= ${max_thinning}; thinning+=thinning_step));
    thinning_step=1
    timeouts=0
    skew=0
    for ((i_skew=0; i_skew <= 5; ++i_skew));
    do
        echo '`'" Skew ${skew}"
        for ((thinning=max_thinning; thinning > 0; thinning-=thinning_step, ++thinning_step));
        do
            echo '  `'" Thinning out by ${thinning} edges."
            for ((i_rep=0; i_rep < NUM_REPETITIONS; ++i_rep));
            do
                run_experiment ${size} ${thinning} ${skew} ${RANDOM}
                RET=$?
                if [ ${RET} -eq 1 ];
                then # timeout
                    ((timeouts++))
                    if [ ${timeouts} -ge ${MAX_TIMEOUTS} ];
                    then
                        echo '`'" Reached maximum of ${MAX_TIMEOUTS} timeouts."\
                                " Stopping evaluation of ${size} relations."
                        break 3; # skip *less* thinning, we already have timeouts
                    fi
                fi
            done
        done
        skew=$(bc <<< "${skew} + .4")
    done
done
