#!/bin/bash

pipenv sync # Install python


# fake the versioning
git tag v0.0.0
git push origin v0.0.0

# Fake the tbl file
echo "X(GIT_REV, a828cd0a6c5a92966417d4eddf2ca52409dab2e7)" > /Users/oliver/TU_BERLIN/MASTER/mutable/include/mutable/gitversion.tbl
echo "X(GIT_BRANCH, main)" >> /Users/oliver/TU_BERLIN/MASTER/mutable/include/mutable/gitversion.tbl
echo "X(SEM_VERSION, v0.0.0)" >> /Users/oliver/TU_BERLIN/MASTER/mutable/include/mutable/gitversion.tbl
