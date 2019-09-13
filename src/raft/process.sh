#!/bin/bash

grep -v lock out | sort -o processed.out
go run draw/main.go| tee debug.out