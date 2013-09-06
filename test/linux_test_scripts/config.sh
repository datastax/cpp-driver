
# This script contains configuration information 
# for test framework.

# Directory containing tests scripts
declare -r TEST_ROOT="$(dirname $(readlink -f $0))"

# Test working directory (MUST BE FULL PATH)
declare -r TEST_DIR="/home/ubuntu/cpp-driver/tmp"
declare -r REPO_DIR="${TEST_DIR}/repo"
declare -r CMAKE_INSTALL_DIR="${REPO_DIR}/install"

# We auth useing SSH Pub/Prv keys
declare -r REPO_HTTPS_ADDRESS="git@bitbucket.org:pkaplanski/cpp-driver"


# Filename of file that will contain full commands output
declare -r REPORT_DIR="${TEST_DIR}/report"
declare -r FULL_OUTPUT_REPORT="${REPORT_DIR}/report.txt"

# Fail marker (if this file is created in report directory,
# then sth wend wrong)
declare -r FAILURE_MARKER="${REPORT_DIR}/failure"

# Unit tests make target
declare -r UNIT_TEST_TARGET="UnitTests_cql"

# List of people that get mail notifcation
# EVERY_REPORT - send report for every test run (one per day)
# UNIT_TEST_FAIL - send report only when unit tests fail or
# project is not compiling...
# Lists should be in form: "joe@pol.pl bill@waw.pl jack@tt.cn"

declare -r EVERY_REPORT_EMAIL="foo1736@gmail.com"
declare -r UNIT_TESTS_FAIL_EMAIL="m.chwedczuk@cognitum.eu"


