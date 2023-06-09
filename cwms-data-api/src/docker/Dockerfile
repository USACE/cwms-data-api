FROM adoptopenjdk:8 as builder

USER $USER

FROM tomcat:9.0.64-jdk8

COPY cda/ /usr/local/tomcat
COPY context.xml /usr/local/tomcat/conf
COPY server.xml /usr/local/tomcat/conf
COPY setenv.sh /usr/local/tomcat/bin
COPY libs/ /usr/local/tomcat/lib

ENV RADAR_JDBC_DRIVER "oracle.jdbc.driver.OracleDriver"
ENV RADAR_JDBC_URL "jdbc:oracle:thin:@localhost/CWMSDEV"
ENV RADAR_JDBC_USERNAME "username here"
ENV RADAR_JDBC_PASSWORD "password here"
ENV RADAR_POOL_INIT_SIZE "5"
ENV RADAR_POOL_MAX_ACTIVE "10"
ENV RADAR_POOL_MAX_IDLE "5"
ENV RADAR_POOL_MIN_IDLE "2"

# New names
ENV CDA_JDBC_DRIVER=
ENV CDA_JDBC_URL=
ENV CDA_JDBC_USER=
ENV CDA_JDBC_PASSWORD=
ENV CDA_POOL_INIT_SIZE=
ENV CDA_POOL_MAX_ACTIVE=
ENV CDA_POOL_MAX_IDLE=
ENV CDA_POOL_MIN_IDLE=

EXPOSE 7000
