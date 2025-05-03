#!/bin/bash

echo "Running all tests"

for f in out/* ; do
    echo "Running test $f"
    ./$f
    if [ $? -ne 0 ]; then
        echo "Test $f failed"
        exit 1
    fi
done

echo "All tests passed"
exit 0
