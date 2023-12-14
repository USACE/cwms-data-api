FROM gradle:6.4.1-jdk8 as cache
USER $USER
WORKDIR /builddir
ENV GRADLE_USER_HOME /cache
COPY build.gradle /builddir/
COPY cwms-data-api/build.gradle /builddir/cwms-data-api/
COPY annotations/build.gradle /builddir/annotations/
COPY access-manager-api/build.gradle /builddir/access-amanger-api/
COPY settings.gradle /builddir/
RUN  gradle build --info --no-daemon

FROM gradle:6.4.1-jdk8 as builder
USER $USER
COPY --from=cache /cache /home/gradle/.gradle
WORKDIR /builddir
COPY . /builddir/
RUN  gradle clean prepareDockerBuild --info --no-daemon

FROM tomcat:9.0.83-jdk8 as api
#RUN DEBIAN_FRONTEND="noninteractive" \ 
#    apt-get -y update && \
#    apt-get -y upgrade --fix-missing

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
# used to simplify redeploy in certain contexts. Update to match -<marker> in image label
ENV IMAGE_MARKER="d"
EXPOSE 7000
