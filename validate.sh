#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "Generating all blacklist products"
${DIR}/generate_blacklist.py
echo "Submitting all poeorb enumeration jobs"
${DIR}/submit_enumeration_jobs.py
