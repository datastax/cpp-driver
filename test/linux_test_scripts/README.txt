
Version 1.0 of linux test scripts for cpp-driver.

cofing.sh - Script contains configuration (including email addresses)
run_tests.sh - This script should be put as task into cron table,
               it runs clone_build_and_test script and sends email.
               
Scripts are using ssmpt utility to send emails see man ssmtp.conf for
details.

Currently we using following account to send emails:
user: foo1736@gmail.com
pass: [Password can be seen in /etc/ssmpt/ssmtp.conf file]


