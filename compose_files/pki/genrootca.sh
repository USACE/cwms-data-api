#/bin/bash

if [ ! -d certs ]; then
	mkdir certs
fi
echo "Generating RootCA"
openssl genrsa  -out certs/rootca.key 2048

openssl req -x509 -new -nodes -key certs/rootca.key -sha256 -days 4096 -out certs/rootca.pem <<END
US
CA
Davis
HECLAB
.
HEC LAB CA
.
END
echo

openssl x509 -in certs/rootca.pem -inform pem -out certs/rootca.der -outform der
keytool -importcert -alias data-api-dev-rootca -keystore certs/rootca.ks -storepass badpassword -file certs/rootca.der
