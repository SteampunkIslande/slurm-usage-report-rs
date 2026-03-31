#!/bin/bash

[[ -z "$1" ]] && { echo "usage: ./daily_eff.sh 2026-03-15";exit 1; }

./slurm-usage-report-rs daily-efficiency --database SACCT_DB --date "${1}" --json-output "${1}.json" --html-output "${1}.html"