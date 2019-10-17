#!/bin/bash

CMD="go test -v -race -timeout 10s -run TestFailNoAgree2B | tee out"
for i in {0..100}; do
    # printf '%s\n' "$i"
    # false
    eval $CMD
    endline=$(tail -n 1 out)
    echo "坠吼的"
    echo $endline
    if [[ $endline == "FAIL"* ]]; then
        printf '%s\n' "var does not end with sub_string."
        exit 1
    fi
    sleep 2
    # if [[ $? > 0 ]]; then
    #     exit 1
    # fi
done
