#!/bin/bash

grep -R '^2019' out | grep -v lock | sort -o processed.out
go run draw/main.go| tee debug.out