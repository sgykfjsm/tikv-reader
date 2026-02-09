mod_update:
    git submodule update --init --recursive
    go mod tidy
    go work vendor

test:
    go test -v ./pkg/... -cover

build:
    go mod tidy
    go build -o bin/tikv-reader main.go