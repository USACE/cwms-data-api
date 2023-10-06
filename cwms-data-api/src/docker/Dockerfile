FROM adoptopenjdk:8 as builder

USER $USER

FROM tomcat:9.0.64-jdk8

COPY cda/ /usr/local/tomcat
COPY context.xml /usr/local/tomcat/conf
COPY server.xml /usr/local/tomcat/conf
COPY setenv.sh /usr/local/tomcat/bin
COPY libs/ /usr/local/tomcat/lib

EXPOSE 7000
