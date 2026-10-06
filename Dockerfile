FROM gradle:8.5-jdk17 AS builder
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
RUN ./gradlew installDist -x test --info --no-daemon
RUN ls
RUN ls build/
RUN ls cwms-data-api/build/install/

FROM eclipse-temurin:17-jre-alpine-3.24 AS api
RUN adduser -D cda
COPY --from=builder /builddir/cwms-data-api/build/install/cwms-data-api /cda
COPY --from=builder /builddir/cwms-data-api/src/docker/logback.xml /logback.xml

ENV CDA_JDBC_DRIVER="oracle.jdbc.driver.OracleDriver"
ENV CDA_JDBC_URL=""
ENV CDA_JDBC_USERNAME=""
ENV CDA_JDBC_PASSWORD=""
ENV CDA_POOL_INIT_SIZE="5"
ENV CDA_POOL_MAX_ACTIVE="30"
ENV CDA_POOL_MAX_IDLE="10"
ENV CDA_POOL_MIN_IDLE="5"
ENV cwms.dataapi.access.providers="KeyAccessManager,OpenID"
ENV cwms.dataapi.access.providers.surpress=CwmsAAACacAuth
ENV cwms.dataapi.access.openid.wellKnownUrl="https://<prefix>/.well-known/openid-configuration"
ENV cwms.dataapi.access.openid.issuer="<issuer>"
ENV cwms.dataapi.access.openid.timeout="604800"
# Putting default values here to easy configuration
ENV cwms.dataapi.access.openid.clientId=cwms
ENV cwms.dataapi.access.openid.idpHint=federation-eams
#ENV cwms.dataapi.access.openid.altAuthUrl="https://identityc-test.cwbi.us/auth/realms/cwbi"

# used to simplify redeploy in certain contexts. Update to match -<marker> in image label
ENV IMAGE_MARKER="a"
EXPOSE 7000
USER cda
CMD ["/cda/bin/cwms-data-api", "/cwms-data", "/cda/wars/ui.war"]
