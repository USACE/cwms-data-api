export CATALINA_OPTS="$CATALINA_OPTS -Dorg.apache.tomcat.util.digester.PROPERTY_SOURCE=org.apache.tomcat.util.digester.EnvironmentPropertySource"


# support to handle transition in various places
if [ "$RADAR_JDBC_DRIVER" != "" && "$CDA_JDBC_DRIVER" == "" ]; then
    export CDA_JDBC_DRIVER=$RADAR_JDBC_DRIVER
fi

if [ "$RADAR_JDBC_URL" != "" && "$CDA_JDBC_URL" == "" ]; then
    export CDA_JDBC_URL=$RADAR_JDBC_URL
fi

if [ "$RADAR_JDBC_USERNAME" != "" && "$CDA_JDBC_USERNAME" == "" ]; then
    export CDA_JDBC_USERNAME=$RADAR_JDBC_USERNAME
fi

if [ "$RADAR_JDBC_PASSWORD" != "" && "$CDA_JDBC_PASSWORD" == "" ]; then
    export CDA_JDBC_PASSWORD=$RADAR_JDBC_PASSWORD
fi

if [ "$RADAR_POOL_INIT_SIZE" != "" && "$CDA_POOL_INIT_SIZE" == "" ]; then
    export CDA_POOL_INIT_SIZE=$RADAR_POOL_INIT_SIZE
fi

if [ "$RADAR_POOL_MAX_ACTIVE" != "" && "$CDA_POOL_MAX_ACTIVE" == "" ]; then
    export CDA_POOL_MAX_ACTIVE=$RADAR_POOL_MAX_ACTIVE
fi

if [ "$RADAR_POOL_MAX_IDLE" != "" && "$CDA_POOL_MAX_IDLE" == "" ]; then
    export CDA_POOL_MAX_IDLE=$RADAR_POOL_MAX_IDLE
fi

if [ "$RADAR_POOL_MIN_IDLE" != "" && "$CDA_POOL_MIN_IDLE" == "" ]; then
    export CDA_POOL_MIN_IDLE=$RADAR_POOL_MIN_IDLE
fi
