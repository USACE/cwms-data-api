FROM gradle:8.5-jdk8 as builder
USER $USER
RUN --mount=type=cache,target=/home/gradle/.gradle
WORKDIR /builddir
COPY . /builddir/
RUN  gradle clean prepareDockerBuild --info --no-daemon

FROM alpine:3.21.0 as tomcat_base
RUN apk --no-cache upgrade && \
    apk --no-cache add \
        openjdk8-jre \
        curl \
        bash


RUN mkdir /download && \
    cd /download && \
    wget https://archive.apache.org/dist/tomcat/tomcat-9/v9.0.97/bin/apache-tomcat-9.0.97.tar.gz && \
    echo "537dbbfc03b37312c2ec282c6906828298cb74e42aca6e3e6835d44bf6923fd8c5db77e98bf6ce9ef19e1922729de53b20546149176e07ac04087df786a62fd9 *apache-tomcat-9.0.97.tar.gz" > checksum.txt && \
    sha512sum -c checksum.txt && \
    tar xzf apache-tomcat-*tar.gz && \
    mv apache-tomcat-9.0.97 /usr/local/tomcat/ && \
    cd / && \
    rm -rf /download
CMD ["/usr/local/tomcat/bin/catalina.sh","run"]

FROM tomcat_base as api

COPY --from=builder /builddir/cwms-data-api/build/docker/cda/ /usr/local/tomcat
COPY --from=builder /builddir/cwms-data-api/build/docker/context.xml /usr/local/tomcat/conf
COPY --from=builder /builddir/cwms-data-api/build/docker/server.xml /usr/local/tomcat/conf
COPY --from=builder /builddir/cwms-data-api/build/docker/setenv.sh /usr/local/tomcat/bin
COPY --from=builder /builddir/cwms-data-api/build/docker/libs/ /usr/local/tomcat/lib

ENV CDA_JDBC_DRIVER "oracle.jdbc.driver.OracleDriver"
ENV CDA_JDBC_URL ""
ENV CDA_JDBC_USERNAME ""
ENV CDA_JDBC_PASSWORD ""
ENV CDA_POOL_INIT_SIZE "5"
ENV CDA_POOL_MAX_ACTIVE "30"
ENV CDA_POOL_MAX_IDLE "10"
ENV CDA_POOL_MIN_IDLE "5"
ENV cwms.dataapi.access.providers "KeyAccessManager,OpenID"
ENV cwms.dataapi.access.openid.wellKnownUrl "https://identity-test.cwbi.us/auth/realms/cwbi/.well-known/openid-configuration"
ENV cwms.dataapi.access.openid.issuer "https://identity-test.cwbi.us/auth/realms/cwbi"
ENV cwms.dataapi.access.openid.timeout "604800"
ENV cwms.dataapi.access.openid.altAuthUrl "https://identityc-test.cwbi.us/auth/realms/cwbi"

# used to simplify redeploy in certain contexts. Update to match -<marker> in image label
ENV IMAGE_MARKER="a"
EXPOSE 7000
