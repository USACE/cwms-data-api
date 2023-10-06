FROM adoptopenjdk:8 as builder

USER $USER

FROM tomcat:9.0.64-jdk8

COPY cda/ /usr/local/tomcat
COPY context.xml /usr/local/tomcat/conf
COPY server.xml /usr/local/tomcat/conf
COPY setenv.sh /usr/local/tomcat/bin
COPY libs/ /usr/local/tomcat/lib

EXPOSE 7000

#  env variables  (set in docker-compose.yml for development)
# ENV CDA_JDBC_DRIVER=
# ENV CDA_JDBC_URL=
# ENV CDA_JDBC_USER=
# ENV CDA_JDBC_PASSWORD=
# ENV CDA_POOL_INIT_SIZE=
# ENV CDA_POOL_MAX_ACTIVE=
# ENV CDA_POOL_MAX_IDLE=
# ENV CDA_POOL_MIN_IDLE=