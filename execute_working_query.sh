#!/bin/bash

# Define flags in an array for easy activation/deactivation
FLAGS=(
    # --help
    --statistics
    --echo
    --graph
    --benchmark
    --times
    --backend WasmV8
    --data-layout PAX4M
)

# Build the command with the active flags
CMD="build/debug_shared/bin/shell ${FLAGS[*]} -"

# Define the query in a separate variable
QUERY=$(cat <<EOF
CREATE DATABASE operators;
USE operators;
CREATE TABLE Relation (id INT(4) NOT NULL PRIMARY KEY, fid INT(4) NOT NULL, n2m INT(4) NOT NULL);
IMPORT INTO Relation DSV "benchmark/operators/data/Relation.csv" ROWS 10000 DELIMITER "," HAS HEADER SKIP HEADER;
SELECT COUNT(*) FROM Relation R, Relation S WHERE R.n2m = S.n2m;
EOF
)

echo "$CMD"
echo
echo "$QUERY"
echo


# Execute the command with the query
echo "$QUERY" | $CMD