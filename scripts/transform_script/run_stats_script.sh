#!/bin/bash

set -e 

POETRY_EXECUTABLE="/home/mikosa/.local/bin/poetry"
SPARK_SUBMIT_PATH="$(which spark-submit)"
SPARK_HOME="$(dirname $(dirname $SPARK_SUBMIT_PATH))"

$POETRY_EXECUTABLE build

INPUT_FILE_PATH="/home/mikosa/NBA_Data/resources/raw_data/json/stats/deployment/stats.json"
OUTPUT_PATH="/home/mikosa/NBA_Data/resources/clean_data/deployment/"
JOB="/home/mikosa/NBA_Data/job/save_clean_stats.py"

rm -rf $OUTPUT_PATH

"$SPARK_HOME/bin/spark-submit" \
    --master local \
    --py-files dist/transformations-*.whl \
    $JOB \
    $INPUT_FILE_PATH \
    $OUTPUT_PATH
