#!/bin/bash

# Define path to DuckDB CLI
DUCKDB=duckdb_cli

{ ${DUCKDB} | grep 'Run Time' | cut -d ' ' -f 4 | awk '{print $1 * 1000;}'; } << EOF
CREATE TABLE Attribute_i32 ( id INT, val INT);
COPY Attribute_i32 FROM 'benchmark/operators/data/Attribute_i32.csv' ( HEADER );
.timer on
SELECT COUNT(*) FROM Attribute_i32 WHERE -2104533975 < val AND val < -2061584302;
SELECT COUNT(*) FROM Attribute_i32 WHERE -1932735283 < val AND val < -1889785610;
SELECT COUNT(*) FROM Attribute_i32 WHERE -1717986918 < val AND val < -1675037245;
SELECT COUNT(*) FROM Attribute_i32 WHERE -1288490188 < val AND val < -1245540515;
SELECT COUNT(*) FROM Attribute_i32 WHERE  -858993459 < val AND val <  -816043786;
SELECT COUNT(*) FROM Attribute_i32 WHERE  -429496729 < val AND val <  -386547056;
SELECT COUNT(*) FROM Attribute_i32 WHERE           0 < val AND val <    42949672;
SELECT COUNT(*) FROM Attribute_i32 WHERE   429496729 < val AND val <   472446402;
SELECT COUNT(*) FROM Attribute_i32 WHERE   858993459 < val AND val <   901943132;
SELECT COUNT(*) FROM Attribute_i32 WHERE  1288490188 < val AND val <  1331439861;
SELECT COUNT(*) FROM Attribute_i32 WHERE  1717986918 < val AND val <  1760936591;
SELECT COUNT(*) FROM Attribute_i32 WHERE  1932735283 < val AND val <  1975684956;
SELECT COUNT(*) FROM Attribute_i32 WHERE  2104533975 < val AND val <  2147483647;
EOF
