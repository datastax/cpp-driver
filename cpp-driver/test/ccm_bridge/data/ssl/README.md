DataStax C/C++ Driver - SSL for Integration Tests
=================================================

The files contained in this directory are for running integration tests that
require SSL authentication.  The files consist of the public and private key
in order to enable SSL authentication with Cassandra (using CCM) and the driver
certificate in order to establish a client connection.

# Generating Public and Private Keys for Cassandra

The following commands will create the public and private keys for Cassandra.

NOTE: The filenames and passwords are important and should remain the same if
      using CCM to create your Cassandra cluster.

```
keytool -genkeypair -noprompt -keyalg RSA -validity 36500 \
	-alias node \
	-keystore ssl/keystore.jks \
	-storepass cassandra \
	-keypass cassandra \
	-ext SAN="IP:127.0.0.1" \
	-dname "CN=127.0.0.1, OU=Drivers and Tools, O=DataStax Inc., L=Santa Clara, ST=California, C=US"

keytool -exportcert -noprompt \
	-alias node \
	-keystore ssl/keystore.jks \
	-storepass cassandra \
	-file ssl/cassandra.crt
```

## Generating Cassandra (Peer) PEM Formatted Certificate

The following commands will create the peer certificate that should be used for
establishing client peer connections within the driver.

NOTE: The filename and passwords are important and should remain the same when
      running the integration tests that require SSL authentication.

```
keytool -exportcert -rfc -noprompt \
	-alias node \
	-keystore ssl/keystore.jks \
	-storepass cassandra \
	-file ssl/cassandra.pem
```
OR
```
openssl x509 -inform der -outform pem \
	-in ssl/cassandra.crt \
	-out ssl/cassandra.pem
```

# Generating Public and Private Keys for Driver

The following commands will create the public and private keys for the driver.
This will allow the Cassandra server to authenticate the driver (client)
connection.

NOTE: The filenames and passwords are important and should remain the same if
      using CCM to create your Cassandra cluster.

```
keytool -genkeypair -noprompt -keyalg RSA -validity 36500 \
	-alias driver \
	-keystore ssl/keystore-driver.jks \
	-storepass cassandra \
	-keypass cassandra \
	-ext SAN="IP:127.0.0.1" \
	-dname "CN=127.0.0.1, OU=Drivers and Tools, O=DataStax Inc., L=Santa Clara, ST=California, C=US"
```

## Generating Driver (Client) Public and Private Key Certificate

The following commands will create the client certificate that should be used
for establishing authenticated client connections with the Cassandra server.

NOTE: The filename and passwords are important and should remain the same when
      running the integration tests that require SSL authentication.

### Generating Public PEM Formatted Certificate

```
keytool -exportcert -noprompt \
	-alias driver \
	-keystore ssl/keystore-driver.jks \
	-storepass cassandra \
	-file ssl/cassandra-driver.crt

keytool -exportcert -rfc -noprompt \
	-alias driver \
	-keystore ssl/keystore-driver.jks \
	-storepass cassandra \
	-file ssl/driver.pem
```
OR
```
openssl x509 -inform der -outform pem \
	-in ssl/cassandra-driver.crt \
	-out ssl/driver.pem
```

### Generating Private Key PEM Formated Certificate

```
keytool -importkeystore -noprompt -srcalias certificatekey -deststoretype PKCS12 \
	-srcalias driver \
	-srckeystore ssl/keystore-driver.jks \
	-srcstorepass cassandra \
	-storepass cassandra \
	-destkeystore ssl/keystore-driver.p12

#Tail is required to remove additional header information from PEM
openssl pkcs12 -nomacver -nocerts -nodes \
	-in ssl/keystore-driver.p12 \
	-password pass:cassandra | \
	tail -n +4 > ssl/driver-private.pem

#Encrypt the private key with a password
chmod 600 ssl/driver-private.pem
ssh-keygen -p \
	-N driver \
	-f ssl/driver-private.pem
```
OR
```
openssl pkcs12 -nomacver -nocerts \
	-in ssl/keystore-driver.p12 \
	-password pass:cassandra \
	-passout pass:driver \
	-out ssl/driver-private.pem
```

## Adding Driver (Client) Certificate to Cassandra Truststore

The following commands will add the client certificate to the Cassandra
truststore that should be used for establishing trusted client connections
between the Cassandra server instance and the driver (client).

NOTE: The filename and passwords are important and should remain the same when
      running the integration tests that require SSL authentication.

```
keytool -import -noprompt \
	-alias truststore \
	-keystore ssl/truststore.jks \
	-storepass cassandra \
	-file ssl/cassandra-driver.crt
```

# Additional Steps for Remote Machine (SSH) Configuration

Since the integration tests use libssh to establish a connection to a remote
host before issuing CCM commands, the ssl directory with the public and private
keys must be copied to the $HOME directory in order for CCM to enable SSL
authentication with Cassandra.

Also the IP address for validating identity must be changed if running CCM on a
remote machine.
