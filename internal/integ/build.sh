#!/bin/bash

mkdir -p out

for d in example/*/ ; do
    pushd "$d" || exit 1
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ../../out/$(basename "$d") main.go
    echo "Build completed for $d"
    popd || exit 1
done
