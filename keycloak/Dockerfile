FROM quay.io/keycloak/keycloak:19.0.1
COPY dls-keycloak.jar /opt/keycloak/providers/dls-keycloak.jar
COPY server.keystore /conf/server.keystore

EXPOSE 8443

ENTRYPOINT [ "/opt/keycloak/bin/kc.sh" ] 