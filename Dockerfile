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
    wget https://archive.apache.org/dist/tomcat/tomcat-9/v9.0.91/bin/apache-tomcat-9.0.91.tar.gz && \
    echo "b22054c9141782232a693765d23d944f0f50774af17dd8968331e020b425e71459b5877a7ba8c2121246a5ce47e6b6a31c3f4215ef133e942da45b49cb534948 *apache-tomcat-9.0.91.tar.gz" > checksum.txt && \
    sha512sum -c checksum.txt && \
    tar xzf apache-tomcat-*tar.gz && \
    mv apache-tomcat-9.0.91 /usr/local/tomcat/ && \
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
ENV cwms.dataapi.access.providers ""
ENV cwms.dataapi.access.openid.wellKnownUrl ""
ENV cwms.dataapi.access.openid.issuer ""
ENV cwms.dataapi.access.openid.timeout "604800"
# used to simplify redeploy in certain contexts. Update to match -<marker> in image label
ENV IMAGE_MARKER="a"
EXPOSE 7000
