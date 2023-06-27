#!/bin/bash

# Trap on CTRL-C / SIGINT
trap "exit 1" SIGINT

set -o pipefail -o nounset

# Initialize Bash's PRNG engine
RANDOM=42
if [ $# -ge 1 ];
then
    RANDOM=$1
fi

BIN=build/debug_shared/bin/shell
MIN_CARDINALITY=10
MAX_CARDINALITY=10000

echo "planner,topology,relations,states"

for T in {chain,cycle,star,clique};
do
    for N in {5,10,15};
    do
        NAME="$T-$N"

        python3 utils/querygen.py -t $T -n $N > /dev/null
        build/release/bin/cardinality_gen \
            --min ${MIN_CARDINALITY} \
            --max ${MAX_CARDINALITY} \
            --seed ${RANDOM} \
            "${NAME}.schema.sql" \
            "${NAME}.query.sql" \
        > "${NAME}.cardinalities.json"

        echo -n "DPccp,$T,$N,"
        ${BIN} \
            --quiet --dryrun \
            --plan-table-las \
            --cardinality-estimator Injected \
            --use-cardinality-file "${NAME}.cardinalities.json" \
            --statistics \
            --plan-enumerator DPccp \
            "${NAME}.schema.sql" \
            "${NAME}.query.sql" \
            2>&1 | ack --nocolor '\d+ CCPs' | cut -d ',' -f 2 | cut -d ' '  -f 2


            # Dijkstra↑
            echo -n "Dijkstra↑,$T,$N,"
            ${BIN} \
                --quiet --dryrun \
                --plan-table-las \
                --cardinality-estimator Injected \
                --use-cardinality-file "${NAME}.cardinalities.json" \
                --statistics \
                --plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand BottomUpComplete --hs-heuristic zero --hs-search AStar \
                "${NAME}.schema.sql" \
                "${NAME}.query.sql" \
                2>&1 | ack 'Vertices generated:' | cut -d ':' -f 2 | tr -d ' '

        if [ "${NAME}" != "clique-15" ];
        then
            # Dijkstra↓
            echo -n "Dijkstra↓,$T,$N,"
            ${BIN} \
                --quiet --dryrun \
                --plan-table-las \
                --cardinality-estimator Injected \
                --use-cardinality-file "${NAME}.cardinalities.json" \
                --statistics \
                --plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic zero --hs-search AStar \
                "${NAME}.schema.sql" \
                "${NAME}.query.sql" \
                2>&1 | ack 'Vertices generated:' | cut -d ':' -f 2 | tr -d ' '
        fi

        # A*
        echo -n "A*↓ + h_sum,$T,$N,"
        ${BIN} \
            --quiet --dryrun \
            --plan-table-las \
            --cardinality-estimator Injected \
            --use-cardinality-file "${NAME}.cardinalities.json" \
            --statistics \
            --plan-enumerator HeuristicSearch --hs-vertex SubproblemsArray --hs-expand TopDownComplete --hs-heuristic sum --hs-search AStar \
            "${NAME}.schema.sql" \
            "${NAME}.query.sql" \
            2>&1 | ack 'Vertices generated:' | cut -d ':' -f 2 | tr -d ' '
        set +x

        rm -f \
            "${NAME}.schema.sql" \
            "${NAME}.query.sql" \
            "${NAME}.cardinalities.json"
    done
done
