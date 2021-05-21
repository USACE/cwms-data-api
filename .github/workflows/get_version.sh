VERSION=`./gradlew properties -q | grep "^version:" | awk '{ print $2}'`
WARFILE="cwms_radar_tomcat/build/libs/cwms_radar_tomcat-$VERSION.war"
echo "::set-output name=VERSION::$VERSION"
echo "::set-output name=WAR_FILE_NAME::$WARFILE"
