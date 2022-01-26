#!/bin/bash

# Trap on CTRL-C / SIGINT
trap "exit 1" SIGINT

# Reset timer
SECONDS=0

# Initialize Bash's PRNG engine
RANDOM=42

# timeout for single invocations
TIMEOUT=15s

BIN=build/release/bin/shell
CSV=planner-benchmark.csv
MIN_RELATIONS=10
STEP_RELATIONS=1
REPETITIONS_PER_NUM_RELATIONS=10

MIN_CARDINALITY=10
MAX_CARDINALITY=10000

# Associative array mapping topologies to their max. number of relations tested
declare -A TOPOLOGIES=(
    [chain]=63
    [cycle]=63
    [clique]=19
    [star]=23
)

declare -A PLANNER_CONFIGS=(
    [DPccp]="--plan-enumerator DPccp"
    [DPsub]="--plan-enumerator DPsubOpt"
    [IKKBZ]="--plan-enumerator IKKBZ"
    [A*-checkpoints]="--plan-enumerator AIPlanning --ai-state BottomUp --ai-heuristic checkpoints --ai-search AStar"
    [A*-checkpoints-opt]="--plan-enumerator AIPlanning --ai-state BottomUpOpt --ai-heuristic checkpoints --ai-search AStar"
    [beam-checkpoints]="--plan-enumerator AIPlanning --ai-state BottomUp --ai-heuristic checkpoints --ai-search beam_search"
    [beam-checkpoints-opt]="--plan-enumerator AIPlanning --ai-state BottomUpOpt --ai-heuristic checkpoints --ai-search beam_search"
    [beam_acyclic-checkpoints]="--plan-enumerator AIPlanning --ai-state BottomUp --ai-heuristic checkpoints --ai-search acyclic_beam_search"
    [beam_acyclic-checkpoints-opt]="--plan-enumerator AIPlanning --ai-state BottomUpOpt --ai-heuristic checkpoints --ai-search acyclic_beam_search"
)

declare -A SKIP_CONFIGS=(
    [cycle-DPsub]=25
    [chain-DPsub]=27
    [clique-DPsub]=17
    [clique-DPccp]=17
    [star-DPsub]=18
    [star-DPccp]=22
)

trim() {
    local var="$*"
    # remove leading whitespace characters
    var="${var#"${var%%[![:space:]]*}"}"
    # remove trailing whitespace characters
    var="${var%"${var##*[![:space:]]}"}"
    printf '%s' "$var"
}

if [ $# -gt 0 ];
then
    CSV=$1;
fi
echo "Writing measurements to '${CSV}'"

# Truncate file
echo "topology,size,planner,cost,time,seed" > "${CSV}"

for TOPOLOGY in "${!TOPOLOGIES[@]}";
do
    MAX_RELATIONS=${TOPOLOGIES[$TOPOLOGY]}
    for ((N=${MIN_RELATIONS}; N <= ${MAX_RELATIONS}; N = N + ${STEP_RELATIONS}));
    do
        NAME="${TOPOLOGY}-${N}"
        echo "Evaluate ${NAME}"

        for ((R=0; R < ${REPETITIONS_PER_NUM_RELATIONS}; ++R));
        do
            # Generate problem
            SEED=${RANDOM}
            echo -n '` '
            # python3 problemgen.py -n ${N} -t "${TOPOLOGY}" -o "${NAME}" --count 1 --seed "${SEED}"
            python3 querygen.py -t "${TOPOLOGY}" -n ${N}
            build/release/bin/cardinality_gen \
                --seed "${SEED}" \
                --uncorrelated \
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
                    if [ ${N} -gt ${M} ];
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

