CATALINA_OPTS="$CATALINA_OPTS -Dorg.apache.tomcat.util.digester.PROPERTY_SOURCE=org.apache.tomcat.util.digester.EnvironmentPropertySource"
CATALINA_OPTS="$CATALINA_OPTS -Djuli-logback.configurationFile=/logback-juli.xml -Djuli-logback.ContextSelector=JNDI"
CATALINA_OPTS="$CATALINA_OPTS -Dlogback.configurationFile=/logback.xml -Dlogback.ContextSelector=JNDI"

export CATALINA_OPTS
