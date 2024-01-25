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
    wget https://dlcdn.apache.org/tomcat/tomcat-9/v9.0.84/bin/apache-tomcat-9.0.84.tar.gz && \
    echo "85a42ab5e7e4cb1923888e96a78a0f277a870d06e76147a95457878c124001c9a317eade4ad69c249a460ffe2cbefe894022b84389cdf33038bc456e3699c8e3 *apache-tomcat-9.0.84.tar.gz" > checksum.txt && \
    sha512sum -c checksum.txt && \
    tar xzf apache-tomcat-*tar.gz && \
    mv apache-tomcat-9.0.84 /usr/local/tomcat/ && \
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
ENV cwms.dataapi.access.openid.altAuthUrl "identityc-test.cwbi.us"
# used to simplify redeploy in certain contexts. Update to match -<marker> in image label
# reset to "a" on version changes.
ENV IMAGE_MARKER="e"
EXPOSE 7000
