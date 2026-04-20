#!/usr/bin/env bash
mkdir -p dist
go mod download
go build -o dist/kafka
./dist/kafka producer