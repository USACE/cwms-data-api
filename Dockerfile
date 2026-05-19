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
    wget https://archive.apache.org/dist/tomcat/tomcat-9/v9.0.118/bin/apache-tomcat-9.0.118.tar.gz && \
    echo "737367433486757ccd687329d99b1188506cdec57a53a29b415173909c38cdf9da4fc9fd73b8cd17cd0a8def8610ad43bab8e84393dc904a0bd1121da8cab2fc *apache-tomcat-9.0.118.tar.gz" > checksum.txt && \
    sha512sum -c checksum.txt && \
    tar xzf apache-tomcat-*tar.gz && \
    mv apache-tomcat-9.0.118 /usr/local/tomcat/ && \
    cd / && \
    rm -rf /download && \
    rm -rf /usr/local/tomcat/webapps/* && \
    mkdir /usr/local/tomcat/webapps/ROOT && \
    printf "<%% response.sendRedirect(\"/cwms-data/\"); %%>\n" > /usr/local/tomcat/webapps/ROOT/index.jsp && \
    printf "User-agent: *\nAllow: /cwms-data/\nDisallow: /cwms-data/auth/\nDisallow: /cwms-data/catalog/\nDisallow: /cwms-data/timeseries/\nDisallow: /cwms-data/swagger-docs\nDisallow: /auth/\nSitemap: https://cwms-data.usace.army.mil/sitemap.xml\n" > /usr/local/tomcat/webapps/ROOT/robots.txt
CMD ["/usr/local/tomcat/bin/catalina.sh","run"]

FROM tomcat_base AS api

COPY --from=builder /builddir/cda-gui/dist/sitemap.xml /usr/local/tomcat/webapps/ROOT/sitemap.xml
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
# Putting default values here to easy configuration
ENV cwms.dataapi.access.openid.clientId=cwms
ENV cwms.dataapi.access.openid.idpHint=federation-eams
#ENV cwms.dataapi.access.openid.altAuthUrl="https://identityc-test.cwbi.us/auth/realms/cwbi"

# used to simplify redeploy in certain contexts. Update to match -<marker> in image label
ENV IMAGE_MARKER="a"
EXPOSE 7000
