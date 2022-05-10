#!/bin/bash

# Trap on CTRL-C / SIGINT
trap "exit 1" SIGINT

set -o pipefail -o nounset

source planner-configs.sh

function printbox()
{
    NAME=$1

    echo -n '##'
    printf '#%.0s' $(seq 1 ${#NAME})
    echo '##'

    echo "# ${NAME} #"

    echo -n '##'
    printf '#%.0s' $(seq 1 ${#NAME})
    echo '##'
}

ORDERED_PLANNERS=(
    ##### Classical #####
    "DPccp"
    # "linDP"
    # "GOO"
    ##### Heuristic Search #####
    # bottom-up
    ## A*
    "BU-A*-zero"
    # "BU-A*-avg_sel"
    # "BU-A*-GOO"
    ## beam
    # "BU-beam-zero"
    # "BU-beam-avg_sel"
    # "BU-beam-GOO"
    ## rel. beam
    # "BU-rel_beam-zero"
    # top-down
    ## A*
    # "TD-A*-zero"
    # "TD-A*-sum"
    # "TD-A*-GOO"
    ## beam
    # "TD-beam-zero"
    # "TD-beam-sum"
    # "TD-beam-GOO"
    ## rel. beam
    # "TD-rel_beam-zero"
)

if [ $# -lt 3 ];
then
    >&2 echo "ERROR: expected three arguments, topology and number of relations"
    >&2 echo -e "USAGE:\n\trun.sh <BUILDDIR> <TOPOLOGY> <NUM_RELATIONS>"
    exit 1
fi

BUILDDIR=$1
T=$2
N=$3
SEED=42

if [ $# -ge 4 ];
then
    SEED=$3
fi


NAME="$T-$N"
SCHEMA="${NAME}.schema.sql"
QUERY="${NAME}.query.sql"
CARDINALITIES="${NAME}.cardinalities.json"

python3 querygen.py -t $T -n $N

${BUILDDIR}/bin/cardinality_gen "${SCHEMA}" "${QUERY}" --seed ${SEED} > "${CARDINALITIES}"

for PLANNER in "${ORDERED_PLANNERS[@]}";
do
    if [ ! -v 'PLANNER_CONFIGS[$PLANNER]' ];
    then
        >&2 echo "ERROR: no configuration found for ${PLANNER}"
        continue
    fi
    PLANNER_CONFIG=${PLANNER_CONFIGS[$PLANNER]}

    printbox "${PLANNER}"

    read -r -d '' CMD <<EOF
env UBSAN_OPTIONS=print_stacktrace=1 ASAN_OPTIONS=detect_stack_use_after_return=1 \
${BUILDDIR}/bin/shell \
--noprompt --times --dryrun \
--cardinality-estimator Injected \
--use-cardinality-file "${CARDINALITIES}" \
${PLANNER_CONFIG} \
"${SCHEMA}" "${QUERY}" | tail -n +$((2*N+6))
EOF

    if [ ! -z "${VERBOSE-}" ];
    then
        echo "\$> ${CMD}"
    fi

    eval "${CMD}"
done

rm -f "${SCHEMA}" "${QUERY}" "${CARDINALITIES}"
