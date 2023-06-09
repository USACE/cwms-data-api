VERSION=`./gradlew --init-script init.gradle properties -q | grep "^version:" | awk '{ print $2}'`
WARFILE="cwms-data-api/build/libs/cwms-data-api-tomcat-$VERSION.war"
echo "::set-output name=VERSION::$VERSION"
echo "::set-output name=WAR_FILE_NAME::$WARFILE"
