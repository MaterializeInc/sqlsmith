#!/usr/bin/env bash
NUM_INSTANCES=${1:-16}
while (( --NUM_INSTANCES >= 0 )); do
  ./sqlsmith --max-joins=1 --verbose --target="host=localhost port=6875 dbname=materialize user=materialize" > $NUM_INSTANCES.log &
done
wait
