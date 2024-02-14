FROM gradle:8.5-jdk8 as builder
USER $USER
RUN --mount=type=cache,target=/home/gradle/.gradle
WORKDIR /builddir
COPY . /builddir/
RUN  gradle clean prepareDockerBuild --info --no-daemon

FROM alpine:3.19.0 as tomcat_base
RUN apk update && apk upgrade --no-cache
RUN apk add openjdk8-jre curl
RUN apk add --no-cache bash


RUN mkdir /download && \
    cd /download && \
    wget https://dlcdn.apache.org/tomcat/tomcat-9/v9.0.85/bin/apache-tomcat-9.0.85.tar.gz && \
    echo "06e239d15ff7b72017c1d0752ddb1be4651374f7c1391631ec5619f4981cb2911267bc6b044d6c71a2a74738f70d433b96418951439848121f1d874862ddd3de *apache-tomcat-9.0.85.tar.gz" > checksum.txt && \
    sha512sum -c checksum.txt && \
    tar xzf apache-tomcat-*tar.gz && \
    mv apache-tomcat-9.0.85 /usr/local/tomcat/ && \
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
ENV cwms.dataapi.access.openid.altAuthUrl ""
# used to simplify redeploy in certain contexts. Update to match -<marker> in image label
# reset to "a" on version changes.
ENV IMAGE_MARKER="h"
EXPOSE 7000
