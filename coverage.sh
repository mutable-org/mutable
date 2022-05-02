#!/bin/bash

set -e -o pipefail

trap "exit" INT

BASE_DIR=$(pwd)

# Ignore List: Remove data for files that should be ignored in the final coverage report
IGNORE_LIST=(
    # subdirs
    '*unittest*'
    '*third-party/*'
    # individual files
    '*QueryGraph2SQL.*'
    '*PDDL.*'
    '*TrainedCostFunction.*'
    '*CostModel.*'
    '*src/storage/Linearization.cpp'
)

# Make coverage build
mkdir -p build/coverage
cd build/coverage

set -x

# Produce gcov alias for llvm-cov gcov
GCOV="$(pwd)/gcov.sh"
echo -e '#!/bin/bash\nllvm-cov gcov "$@"' > "${GCOV}"
chmod a+x "${GCOV}"

LCOV_FLAGS="\
    --quiet \
    --gcov-tool ${GCOV} \
    --base-directory ${BASE_DIR} \
    --no-external \
    --rc lcov_branch_coverage=1"

cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DBUILD_SHARED_LIBS=On \
    -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DCMAKE_COMPILE_FLAGS=--coverage \
    ../..
# ninja clean
ninja gitversion
ninja -v unittest

# Cleanup lcov
lcov ${LCOV_FLAGS} --zerocounters --directory src --directory unittest
# Create baseline to make sure untouched files show up in the report
lcov ${LCOV_FLAGS} --capture --initial --directory src --directory unittest --output-file base.info
# lcov ${LCOV_FLAGS} --capture --initial --output-file unittest.base
# Run tests
bin/unittest --durations yes --reporter junit --out catch.xml '[core]'
# Capture lcov counters and generate report
lcov ${LCOV_FLAGS} --capture --directory src --directory unittest --output-file test.info
# lcov ${LCOV_FLAGS} --capture --directory unittest --output-file unittest.capture
# Add baseline counters
lcov ${LCOV_FLAGS} --directory src --directory unittest --add-tracefile base.info --add-tracefile test.info --output-file total.info
# Filter collected data to final coverage report
lcov ${LCOV_FLAGS} --directory src --directory unittest --remove total.info "${IGNORE_LIST[@]}" --output-file coverage.info

# Generate HTML output
rm -fr html
genhtml --branch-coverage --rc lcov_branch_coverage=1 coverage.info -o html

# Cleanup
rm base.info
rm test.info
rm total.info
rm coverage.info
rm "${GCOV}"
