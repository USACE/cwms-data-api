FROM gradle:8.5-jdk8 AS builder
USER $USER
RUN --mount=type=cache,target=/home/gradle/.gradle
WORKDIR /builddir
COPY . /builddir/
RUN  gradle prepareDockerBuild --info --no-daemon

FROM alpine:3.21.0 AS tomcat_base
RUN apk --no-cache upgrade && \
    apk --no-cache add \
        openjdk8-jre \
        curl \
        bash


RUN mkdir /download && \
    cd /download && \
    wget https://archive.apache.org/dist/tomcat/tomcat-9/v9.0.102/bin/apache-tomcat-9.0.102.tar.gz && \
    echo "cbe407f17c813d9f83cab459e603df171f2e5782c3a0cdb4cfa00b0391a89cedf865c6d8972fc7e12210c69a8467ede5939f35bb0f3b41fa173b9ee83199768a *apache-tomcat-9.0.102.tar.gz" > checksum.txt && \
    sha512sum -c checksum.txt && \
    tar xzf apache-tomcat-*tar.gz && \
    mv apache-tomcat-9.0.102 /usr/local/tomcat/ && \
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
