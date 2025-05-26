#!/bin/bash

# Check if GOARCH is supported
ARCH=$(go env GOARCH)
if [ "$ARCH" != "amd64" ] && [ "$ARCH" != "arm64" ]; then
    echo "Error: Unsupported architecture $ARCH. Supported architectures are amd64 and arm64."
    exit 1
fi

mkdir -p out

for d in example/*/ ; do
    pushd "$d" || exit 1
    CGO_ENABLED=0 GOOS=linux go build -o ../../out/$(basename "$d") main.go
    echo "Build completed for $d"
    popd || exit 1
done
