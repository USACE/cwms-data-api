FROM adoptopenjdk:8 as builder

USER $USER

FROM tomcat:9.0.64-jdk8

COPY cda/ /usr/local/tomcat
COPY context.xml /usr/local/tomcat/conf
COPY server.xml /usr/local/tomcat/conf
COPY setenv.sh /usr/local/tomcat/bin
COPY libs/ /usr/local/tomcat/lib

EXPOSE 7000

ENV CDA_JDBC_DRIVER=oracle.jdbc.driver.OracleDriver
ENV CDA_JDBC_URL=jdbc:oracle:thin:@db/CWMS
ENV CDA_JDBC_USERNAME=q0webtest
ENV CDA_JDBC_PASSWORD=simplecwmspasswD1
ENV CDA_POOL_INIT_SIZE=5
ENV CDA_POOL_MAX_ACTIVE=10
ENV CDA_POOL_MAX_IDLE=5
ENV CDA_POOL_MIN_IDLE=2