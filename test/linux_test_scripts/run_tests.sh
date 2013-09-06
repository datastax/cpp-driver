#!/bin/bash

# Read configuration
. config.sh

# Download code, cmake, make, unit 
# if something goes wrong failure marker will be set
"${TEST_ROOT}/clone_build_and_test.sh"

# Send result to email lists

# send_email email_addr email_contents_file(full path)
function send_email {
	EMAIL_FILE="${REPORT_DIR}/email.txt"
	
	# Create email headers
	echo "To: $1" > "${EMAIL_FILE}"
	echo "From: DataStax Test Machine" >> "${EMAIL_FILE}"
	echo "Subject: TEST REPORT (`date -u "+%d/%m/%Y %H:%M"` UTC)" >> "${EMAIL_FILE}"
	echo "" >> "${EMAIL_FILE}"
	
	# Attach contents
	cat "$2" >> "${EMAIL_FILE}"

	# Send email
	ssmtp "$1" < "${EMAIL_FILE}"
}

# Send emails to subscribers
echo "Sending email reports...."

for email in ${EVERY_REPORT_EMAIL}; do
	send_email "$email" ${FULL_OUTPUT_REPORT}
done

if [[ -f ${FAILURE_MARKER} ]]; then
	for email in ${UNIT_TESTS_FAIL_EMAIL}; do
		send_email "$email" ${FULL_OUTPUT_REPORT}
	done
fi


