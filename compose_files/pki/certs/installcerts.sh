#!/bin/bash
# install certs for use in docker dev environment
CERT_DIR="/etc/ssl/certs/java/cacerts"

if keytool -list -keystore ${CERT_DIR} -alias cda_lab_root -storepass changeit > /dev/null; then
    echo "Alias exists, deleting..."
    keytool -delete -alias cda_lab_root -keystore ${CERT_DIR} -storepass changeit
fi
echo "Importing certificate..."
keytool -trustcacerts -importcert -alias cda_lab_root -keystore ${CERT_DIR} -storepass changeit -file /conf/rootca.pem -noprompt
