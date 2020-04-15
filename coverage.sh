#!/bin/bash

# Make coverage build
mkdir -p build/coverage
cd build/coverage
cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_C_COMPILER=clang \
    -DCMAKE_CXX_COMPILER=clang++ \
    -DCMAKE_C_FLAGS_DEBUG=--coverage \
    -DCMAKE_CXX_FLAGS_DEBUG=--coverage \
    ../..
ninja

# Produce gcov alias for llvm-cov gcov
echo -e '#!/bin/bash\nllvm-cov gcov "$@"' > gcov.sh
chmod a+x gcov.sh

# Cleanup lcov
lcov --quiet --gcov-tool ./gcov.sh --directory src --rc lcov_branch_coverage=1 --zerocounters
lcov --quiet --gcov-tool ./gcov.sh --directory unittest --rc lcov_branch_coverage=1 --zerocounters
# Create baseline to make sure untouched files show up in the report
lcov --quiet --gcov-tool ./gcov.sh --directory src --capture --initial --output-file src.base
lcov --quiet --gcov-tool ./gcov.sh --directory unittest --capture --initial --output-file unittest.base
# Run tests
bin/unittest --durations yes --reporter junit --out catch.xml \[core\]
# Capture lcov counters and generate report
lcov --quiet --gcov-tool ./gcov.sh --directory src --rc lcov_branch_coverage=1 \
     --capture --output-file src.capture
lcov --quiet --gcov-tool ./gcov.sh --directory unittest --rc lcov_branch_coverage=1 \
     --capture --output-file unittest.capture
# Add baseline counters
lcov --quiet --gcov-tool ./gcov.sh --directory src --rc lcov_branch_coverage=1 \
     --add-tracefile src.base \
     --add-tracefile src.capture \
     --add-tracefile unittest.base \
     --add-tracefile unittest.capture \
     --output-file cov.total
# Filter collected data to final coverage report
lcov --quiet --gcov-tool ./gcov.sh --directory src --rc lcov_branch_coverage=1 \
     --remove cov.total '*unittest*' '/usr/include/*' '*third-party/*' \
     --output-file cov.info

# Generate HTML output
rm -fr html
genhtml --branch-coverage --rc lcov_branch_coverage=1 cov.info -o html

# Cleanup
rm src.base
rm src.capture
rm unittest.base
rm unittest.capture
rm cov.total
rm cov.info
rm gcov.sh
