DataStax C/C++ Driver - Invalid Certificates for Integration Tests
==================================================================

The files contained in this directory are for running integration tests that
require SSL authentication.  The files consist of invalid public and private
keys in order to throughly test the SSL authentication of the driver.  These
certificates will result in no connection being established with the
Cassandra server.


```
#Cassandra (Peer)
keytool -genkeypair -noprompt -keyalg RSA -validity 36500 \
	-alias invalid \
	-keystore ssl/invalid/keystore-invalid.jks \
	-storepass invalid \
	-keypass invalid \
	-ext SAN="DNS:INVALID" \
	-dname "CN=INVALID, OU=INVALID, O=INVALID, L=INVALID, ST=INVALID, C=INVALID"
keytool -exportcert -rfc -noprompt \
	-alias invalid \
	-keystore ssl/invalid/keystore-invalid.jks \
	-storepass invalid \
	-file ssl/invalid/cassandra-invalid.pem

#Driver (Client)
keytool -genkeypair -noprompt -keyalg RSA -validity 36500 \
	-alias driver-invalid \
	-keystore ssl/invalid/keystore-driver-invalid.jks \
	-storepass invalid \
	-keypass invalid \
	-ext SAN="DNS:DRIVER-INVALID" \
	-dname "CN=DRIVER-INVALID, OU=DRIVER-INVALID, O=DRIVER-INVALID, L=DRIVER-INVALID, ST=DRIVER-INVALID, C=DRIVER-INVALID"
keytool -exportcert -rfc -noprompt \
	-alias driver-invalid \
	-keystore ssl/invalid/keystore-driver-invalid.jks \
	-storepass invalid \
	-file ssl/invalid/driver-invalid.pem
keytool -importkeystore -noprompt -srcalias certificatekey -deststoretype PKCS12 \
	-srcalias driver-invalid \
	-srckeystore ssl/invalid/keystore-driver-invalid.jks \
	-srcstorepass invalid \
	-storepass invalid \
	-destkeystore ssl/invalid/keystore-driver-invalid.p12
openssl pkcs12 -nomacver -nocerts -nodes \
	-in ssl/invalid/keystore-driver-invalid.p12 \
	-password pass:invalid | \
	tail -n +4 > ssl/invalid/driver-private-invalid.pem
chmod 600 ssl/invalid/driver-private-invalid.pem
ssh-keygen -p \
	-N invalid \
	-f ssl/invalid/driver-private-invalid.pem
```
