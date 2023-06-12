VERSION=`./gradlew --init-script init.gradle properties -q | grep "^version:" | awk '{ print $2}'`
WARFILE="cwms-data-api/build/libs/cwms-data-api-tomcat-$VERSION.war"
echo "VERSION=$VERSION" >> $GITHUB_ENV
echo "WAR_FILE_NAME=$WARFILE" >> $GITHUB_ENV
