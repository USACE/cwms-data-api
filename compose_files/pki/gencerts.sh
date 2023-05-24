#/bin/bash

if [ ! -d certs ]; then
	./genrootca.sh
fi

function generate_serverkey()
{
	SERVICE_NAME=$1
	shift
	SERVICE_HOSTNAME=$2
	shift
	
	KEY=certs/$SERVICE_NAME.key
	CSR=certs/$SERVICE_NAME.csr
	CRT=certs/$SERVICE_NAME.crt
	CONF=certs/$SERVICE_NAME.conf
	P12=certs/$SERVICE_NAME.p12
	KEYSTORE=certs/$SERVICE_NAME.ks
	echo
	echo "Generating Server Key for $SERVICE_NAME"
	openssl genrsa -out $KEY 2048
	openssl req -new -key $KEY -out $CSR <<END
US
CA
Davis
HECLAB
.
$SERVICE_HOSTNAME
.


END	
	echo "Setting up Config and SAN information"

	cat > $CONF <<END
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, nonRepudiation, keyEncipherment, dataEncipherment
subjectAltName = @alt_names

[alt_names]
END
	i=1
	for san in "$@"
	do
		echo "DNS.$i = $san" >> $CONF
		i=$((i+1))
	done

	openssl x509 -req -in $CSR -CAkey certs/rootca.key -CA certs/rootca.pem -CAcreateserial -out $CRT -days 4096 -sha256 -extfile $CONF

	echo "Creating Java Keystores"
	cat $CRT certs/rootca.pem > $SERVICE_NAME.pem.withchain
	mv $SERVICE_NAME.pem.withchain $CRT
	openssl pkcs12 -export -in $CRT -inkey $KEY -out $P12 -name $SERVICE_NAME -CAfile certs/rootca.pem -caname root -chain --password pass:badpassword

	if [ -f $KEYSTORE ]; then rm $KEYSTORE; fi
	keytool -importkeystore -deststorepass badpassword -destkeypass badpassword -destkeystore $KEYSTORE -srckeystore $P12 -srcstoretype PKCS12 -srcstorepass badpassword -alias $SERVICE_NAME
	keytool -import -noprompt -trustcacerts -alias root -file certs/rootca.pem -keystore $KEYSTORE -storepass badpassword
}

generate_serverkey main traefik.localhost $HOSTNAME auth.localhost cwms-data.localhost