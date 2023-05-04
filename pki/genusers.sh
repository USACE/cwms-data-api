#/bin/bash

if [ ! -d certs ]; then
	./genrootca.sh
fi

function generate_client_cert() {
    name=$1
    edipi=$2
    echo "Generating User $name"
    openssl genrsa -out certs/$name.key 2048
    openssl req -new -key certs/$name.key -out certs/$name.csr <<END
US
CA
Davis
HECLAB
.
$name.User.$edipi



END

    openssl x509 -req -in certs/$name.csr -CAkey certs/rootca.key -CA certs/rootca.pem -CAcreateserial -out certs/$name.crt -days 1080 -sha256
    echo "Making pcks12 format"
    openssl pkcs12 -export -out certs/$name.p12 -inkey certs/$name.key -in certs/$name.crt --password pass:test
}

echo "Generating Users"
#generate_client_cert client1 1000000001
#generate_client_cert pduser ""

#generate_client_cert normaluser 1000000002
#generate_client_cert notpresent 2000000001
#generate_client_cert noprivs    4000000001
#generate_client_cert dupedipi   3000000001
generate_client_cert useradmin  5000000001
