FROM gradle:8.5-jdk11 AS builder
USER $USER
RUN --mount=type=cache,target=/home/gradle/.gradle
WORKDIR /builddir
COPY . /builddir/
RUN apt update && apt install -y curl
ENV NVM_DIR="/root/.nvm"
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
ENV NODE_VERSION=22.17.0
# Ensure nvm is sourced for subsequent commands
SHELL ["/bin/bash", "-c"]
RUN . "$NVM_DIR/nvm.sh" && nvm install $NODE_VERSION && nvm use $NODE_VERSION
ENV NODE_PATH=$NVM_DIR/v$NODE_VERSION/lib/node_modules
ENV PATH=$NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH
RUN gradle prepareDockerBuild --info --no-daemon

FROM alpine:3.21.3 AS tomcat_base
RUN apk --no-cache upgrade && \
    apk --no-cache add \
        openjdk11-jre \
        curl \
        bash


RUN mkdir /download && \
    cd /download && \
    wget https://archive.apache.org/dist/tomcat/tomcat-9/v9.0.112/bin/apache-tomcat-9.0.112.tar.gz && \
    echo "fc55589f28bf6659928167461c741649b6005b64285dd81df05bb5ee40f4c6de59b8ee3af84ff756ae1513fc47f5f73070e29313b555e27f096f25881c69841d *apache-tomcat-9.0.112.tar.gz" > checksum.txt && \
    sha512sum -c checksum.txt && \
    tar xzf apache-tomcat-*tar.gz && \
    mv apache-tomcat-9.0.112 /usr/local/tomcat/ && \
    cd / && \
    rm -rf /download && \
    rm -rf /usr/local/tomcat/webapps/* && \
    mkdir /usr/local/tomcat/webapps/ROOT && \
    echo "<html><body>Nothing to see here</body></html>" > /usr/local/tomcat/webapps/ROOT/index.html
CMD ["/usr/local/tomcat/bin/catalina.sh","run"]

FROM tomcat_base AS api

COPY --from=builder /builddir/cwms-data-api/build/docker/cda/ /usr/local/tomcat
COPY --from=builder /builddir/cwms-data-api/build/docker/context.xml /usr/local/tomcat/conf
COPY --from=builder /builddir/cwms-data-api/build/docker/server.xml /usr/local/tomcat/conf
COPY --from=builder /builddir/cwms-data-api/build/docker/setenv.sh /usr/local/tomcat/bin
COPY --from=builder /builddir/cwms-data-api/build/docker/libs/ /usr/local/tomcat/lib

ENV CDA_JDBC_DRIVER="oracle.jdbc.driver.OracleDriver"
ENV CDA_JDBC_URL=""
ENV CDA_JDBC_USERNAME=""
ENV CDA_JDBC_PASSWORD=""
ENV CDA_POOL_INIT_SIZE="5"
ENV CDA_POOL_MAX_ACTIVE="30"
ENV CDA_POOL_MAX_IDLE="10"
ENV CDA_POOL_MIN_IDLE="5"
ENV cwms.dataapi.access.providers="KeyAccessManager,OpenID"
ENV cwms.dataapi.access.openid.wellKnownUrl="https://<prefix>/.well-known/openid-configuration"
ENV cwms.dataapi.access.openid.issuer="<issuer>"
ENV cwms.dataapi.access.openid.timeout="604800"
#ENV cwms.dataapi.access.openid.altAuthUrl="https://identityc-test.cwbi.us/auth/realms/cwbi"

# used to simplify redeploy in certain contexts. Update to match -<marker> in image label
ENV IMAGE_MARKER="a"
EXPOSE 7000
