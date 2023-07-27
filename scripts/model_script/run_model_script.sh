#!/bin/bash


INPUT_FILE_PATH="/home/mikosa/NBA_Data/resources/clean_data/stats/clean_stats.csv"
OUTPUT_FILE_PATH='/home/mikosa/NBA_Data/model/pickle_file'
JOB="/home/mikosa/NBA_Data/job/save_model.py"

python3  \
    $JOB \
    $INPUT_FILE_PATH\
    $OUTPUT_FILE_PATH