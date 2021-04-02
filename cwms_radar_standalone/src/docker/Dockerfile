FROM adoptopenjdk:11-jre-hotspot
RUN groupadd -r radar && useradd -r -s /bin/false -g radar radar
USER radar
COPY radar /radar/

ENV RADAR_JDBC_DRIVER "oracle.jdbc.driver.OracleDriver"
ENV RADAR_JDBC_URL "jdbc:oracle:thin:@localhost/CWMSDEV"
ENV RADAR_JDBC_USERNAME "username here"
ENV RADAR_JDBC_PASSWORD "password here"
ENV RADAR_POOL_INIT_SIZE "5"
ENV RADAR_POOL_MAX_ACTIVE "10"
ENV RADAR_POOL_MAX_IDLE "5"
ENV RADAR_POOL_MIN_IDLE "2"

CMD ["java","-jar","/radar/radar-standalone.jar"]