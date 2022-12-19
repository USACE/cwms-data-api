FROM gradle:6.4.1-jdk8 as cache
USER $USER
WORKDIR /builddir
ENV GRADLE_USER_HOME /cache
COPY build.gradle /builddir/
COPY cwms_radar_api/build.gradle /builddir/cwms_radar_api/
COPY annotations/build.gradle /builddir/annotations/
COPY access-manager-api/build.gradle /builddir/access-amanger-api/
COPY settings.gradle /builddir/
RUN  gradle build --info --no-daemon

FROM gradle:6.4.1-jdk8 as builder
USER $USER
COPY --from=cache /cache /home/gradle/.gradle
WORKDIR /builddir
COPY . /builddir/
RUN  gradle prepareDockerBuild --info --no-daemon

FROM tomcat:9.0.64-jdk8 as api

COPY --from=builder /builddir/cwms_radar_api/build/docker/radar/ /usr/local/tomcat
COPY --from=builder /builddir/cwms_radar_api/build/docker/context.xml /usr/local/tomcat/conf
COPY --from=builder /builddir/cwms_radar_api/build/docker/server.xml /usr/local/tomcat/conf
COPY --from=builder /builddir/cwms_radar_api/build/docker/setenv.sh /usr/local/tomcat/bin
COPY --from=builder /builddir/cwms_radar_api/build/docker/libs/ /usr/local/tomcat/lib

ENV RADAR_JDBC_DRIVER "oracle.jdbc.driver.OracleDriver"
ENV RADAR_JDBC_URL "jdbc:oracle:thin:@localhost/CWMSDEV"
ENV RADAR_JDBC_USERNAME "username here"
ENV RADAR_JDBC_PASSWORD "password here"
ENV RADAR_POOL_INIT_SIZE "5"
ENV RADAR_POOL_MAX_ACTIVE "10"
ENV RADAR_POOL_MAX_IDLE "5"
ENV RADAR_POOL_MIN_IDLE "2"
EXPOSE 7000
