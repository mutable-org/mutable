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
TIMEOUT=15s

BIN=build/release/bin/shell
CSV=planner-benchmark.csv
CORRELATED=1

MIN_RELATIONS=12
MAX_RELATIONS=18

NUM_REPETITIONS=5

MIN_CARDINALITY=10
MAX_CARDINALITY=10000

SIZE=4
THINNING=2

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
SEED=$2



function run_experiment()
{
    SIZE=$1
    THINNING=$2
    SEED=$3

    OUT=$(python querygen.py --quiet --seed ${SEED} -t clique -n ${SIZE} --thinning=${THINNING})
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
    SKEW=$(echo "${OUT}" | cut -d ' ' -f 4)

    BASE=$(basename "${SCHEMA}" .schema.sql)
    JSON="${BASE}.cardinalities.json"

    echo "  \` Evaluate clique-${SIZE} with thinning out ${THINNING} edges and seed ${SEED}."
    # echo -n "    SIZE=${size} THINNING=${thinning} SEED=${SEED} : "

    build/release/bin/cardinality_gen "${SCHEMA}" "${QUERY}" > "${JSON}"

    timeout --signal=TERM --kill-after=3s ${TIMEOUT} taskset -c 2 ${BIN} \
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
            echo "clique,${SIZE},${THINNING},${DENSITY},${SKEW},A*-BU,${COST},${TIME},${SEED}" >> "${CSV}"; \
        done

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
            echo "clique,${SIZE},${THINNING},DPccp,${COST},${TIME},${SEED}" >> "${CSV}"; \
        done

    rm "${SCHEMA}" "${QUERY}" "${JSON}"
}



echo "Writing measurements to '${CSV}'"
echo "topology,size,thinning,density,skew,planner,cost,time,seed" > "${CSV}"

for ((size=${MIN_RELATIONS}; size <= ${MAX_RELATIONS}; ++size));
do
    ((max_edges=size * (size - 1) / 2))
    ((min_edges=size - 1))
    ((max_thinning=max_edges - min_edges))
    thinning_step=$(bc <<< "(${max_thinning} + 4) / 5")
    echo "Evaluate clique-${size} with thinning out 1 to ${max_thinning} edges"
    for ((thinning=thinning_step; thinning <= ${max_thinning}; thinning+=thinning_step));
    do
        echo '`'" Thinning out by ${thinning} edges"
        for ((i=0; i < ${NUM_REPETITIONS}; ++i));
        do
            run_experiment ${size} ${thinning} ${RANDOM}
        done
    done
done
