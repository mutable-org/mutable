#!/bin/bash

# Trap on CTRL-C / SIGINT
trap "exit 1" SIGINT

set -o pipefail -o nounset

BINDIR=build/release/bin

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

declare -A PLANNER_CONFIGS=(
    ###### Traditional Planners #####
    [DPsub]="--plan-enumerator DPsubOpt"
    [DPccp]="--plan-enumerator DPccp"
    [TDMinCutAGaT]="--plan-enumerator TDMinCutAGaT"
    [IKKBZ]="--plan-enumerator IKKBZ"
    [linDP]="--plan-enumerator LinearizedDP"
    [GOO]="--plan-enumerator GOO"
    ##### Heuristic Search #####
    # BottomUp
    ## A*
    [BU-A*-zero]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search AStar"
    [BU-A*-avg_sel]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic avg_sel --hs-search AStar"
    [BU-A*-GOO]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic GOO --hs-search AStar"
    ## beam
    [BU-beam-zero]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search monotone_beam_search"
    [BU-beam-avg_sel]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic avg_sel --hs-search monotone_beam_search"
    [BU-beam-GOO]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic GOO --hs-search monotone_beam_search"
    ## relative beam
    [BU-rel_beam-zero]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search monotone_dynamic_beam_search"
    # TopDown
    ## A*
    [TD-A*-zero]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic zero --hs-search AStar"
    [TD-A*-sum]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic sum --hs-search AStar"
    [TD-A*-GOO]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic GOO --hs-search AStar"
    ## beam
    [TD-beam-zero]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic zero --hs-search monotone_beam_search"
    [TD-beam-sum]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic sum --hs-search monotone_beam_search"
    [TD-beam-GOO]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic GOO --hs-search monotone_beam_search"
    ## relative beam
    [TD-rel_beam-zero]="--plan-enumerator HeuristicSearch --hs-state SubproblemsArray --hs-expand TopDownComplete  --hs-heuristic zero --hs-search monotone_dynamic_beam_search"
)

if [ $# -lt 2 ];
then
    >&2 echo "ERROR: expected two arguments, topology and number of relations"
    >&2 echo -e "USAGE:\n\trun.sh <TOPOLOGY> <NUM_RELATIONS>"
    exit 1
fi

T=$1
N=$2
SEED=42

if [ $# -ge 3 ];
then
    SEED=$3
fi


NAME="$T-$N"
SCHEMA="${NAME}.schema.sql"
QUERY="${NAME}.query.sql"
CARDINALITIES="${NAME}.cardinalities.json"

python3 querygen.py -t $T -n $N

${BINDIR}/cardinality_gen "${SCHEMA}" "${QUERY}" --seed ${SEED} > "${CARDINALITIES}"


for PLANNER in "${ORDERED_PLANNERS[@]}";
do
    if [ ! -v 'PLANNER_CONFIGS[$PLANNER]' ];
    then
        >&2 echo "ERROR: no configuration found for ${PLANNER}"
        continue
    fi
    PLANNER_CONFIG=${PLANNER_CONFIGS[$PLANNER]}

    printbox "${PLANNER}"

    env UBSAN_OPTIONS=print_stacktrace=1 ASAN_OPTIONS=detect_stack_use_after_return=1 \
        ${BINDIR}/shell \
        --noprompt --times --dryrun \
        --cardinality-estimator InjectionCardinalityEstimator \
        --use-cardinality-file "${CARDINALITIES}" \
        ${PLANNER_CONFIG} \
        "${SCHEMA}" "${QUERY}" | tail -n +$((2*N+6))
done
