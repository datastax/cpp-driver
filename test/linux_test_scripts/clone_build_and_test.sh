#!/bin/bash

# This scipt is used to automatically download
# cpp-driver, build it and run unit tests.
# Result of this commands are send to programmers
# working on project.

# Read test configuration
. config.sh

# Create report directory
echo "Creating report directory..."

if [[ ! -d "${REPORT_DIR}" ]]; then
	if ! mkdir "${REPORT_DIR}"; then
		echo "Cannot create report directory..."
		exit 1
	fi
fi

echo "Creating report file..."
echo "TEST REPORT (`date -u`)" > "${FULL_OUTPUT_REPORT}"

echo "Removing failure marker..."
if [[ -f "${FAILURE_MARKER}" ]]; then
	rm -f "${FAILURE_MARKER}"
fi

function set_failure_marker {
	touch "${FAILURE_MARKER}"
}

# adds preamble to report
# add_preamble title
function add_preamble {
	echo >> "${FULL_OUTPUT_REPORT}"
	printf -- '-%.0s' {1..40} >> "${FULL_OUTPUT_REPORT}"
	echo >> "${FULL_OUTPUT_REPORT}"
	echo "SECTION: $1" >> "${FULL_OUTPUT_REPORT}"
	echo >> "${FULL_OUTPUT_REPORT}"
}

# Download project code

echo "Downloading git repository ${REPO_HTTPS_ADDRESS}..."

add_preamble "git clone"
git clone "${REPO_HTTPS_ADDRESS}" "${REPO_DIR}" &>> "${FULL_OUTPUT_REPORT}"
if (( $? != 0 )); then
	echo "Cannot clone git repository..."
	"${TEST_ROOT}/clean_up.sh"
	exit 1
fi

echo "Repository successfuly downloaded..."

# CMAKE project

echo "CMAKE'ing project..."
add_preamble "cmake"
cmake "${REPO_DIR}/CMakeLists.txt" \
	-DCMAKE_INSTALL_PREFIX:STRING="${CMAKE_INSTALL_DIR}" \
	&>> "${FULL_OUTPUT_REPORT}"

if (( $? != 0 )); then
	echo "Errors during CMAKE'ing project..."
	set_failure_marker
	"${TEST_ROOT}/clean_up.sh"
	exit 2
fi

echo "Project successfully CMAKE'ed..."

# Build poject using autotools

echo "MAKE'ing project..."
add_preamble "make"
make -C "${REPO_DIR}" "${UNIT_TEST_TARGET}" \
	&>> "${FULL_OUTPUT_REPORT}"

if (( $? != 0 )); then
	echo "Errors during build..."
	set_failure_marker
	"${TEST_ROOT}/clean_up.sh"
	exit 3
fi

# Execute unit tests...

echo "Executing unit tests..."

echo TODO EXECUTE UNIT TESTS


"${TEST_DIR}/clean_up.sh"
exit 0

