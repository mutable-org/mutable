#!/bin/bash

# Trap on CTRL-C / SIGINT
trap "exit 1" SIGINT

set -o pipefail -o nounset

source planner-configs.sh

BINDIR=build/release/bin
BIN=${BINDIR}/shell

ORDERED_PLANNERS=(
    "BU-A*-zero"
    "BU-A*-GOO"
    "TD-A*-zero"
    "TD-A*-sum"
    "TD-A*-GOO"
)
NUM_REPETITIONS=5

echo "planner,topology,relations,iteration,vertices"
for T in {chain,cycle,star,clique};
do
    for I in $(seq 1 ${NUM_REPETITIONS});
    do
        for N in {5..15}; do
            NAME="$T-${N}"

            python3 querygen.py -t $T -n ${N} > /dev/null
            build/release/bin/cardinality_gen \
                --seed ${RANDOM} \
                "${NAME}.schema.sql" \
                "${NAME}.query.sql" \
            > "${NAME}.cardinalities.json"

            for PLANNER in "${ORDERED_PLANNERS[@]}";
            do
                if [ ! -v 'PLANNER_CONFIGS[$PLANNER]' ];
                then
                    >&2 echo "ERROR: no configuration found for ${PLANNER}"
                    continue
                fi
                PLANNER_CONFIG=${PLANNER_CONFIGS[$PLANNER]}

                echo -n "${PLANNER},${T},${N},${I},"
                timeout --signal=SIGTERM --kill-after=7s 5s ${BIN} \
                    --quiet --dryrun \
                    --plan-table-las \
                    --cardinality-estimator InjectionCardinalityEstimator \
                    --use-cardinality-file "${NAME}.cardinalities.json" \
                    ${PLANNER_CONFIG} \
                    "${NAME}.schema.sql" \
                    "${NAME}.query.sql" \
                    2>&1 | ack --nocolor '^States generated:' | cut -d ':' -f 2 | tr -d ' '
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
                    echo "0"
                fi
            done

            rm -f "${NAME}.schema.sql" "${NAME}.query.sql" "${NAME}.cardinalities.json"
        done
    done
done
