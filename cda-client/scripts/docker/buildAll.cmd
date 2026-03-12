@REM runs all scripts start to finish, using docker where windows might be an issue
echo Fetching and Manipulating spec...
npm.cmd run buildSpec && npm.cmd run openapi && .\scripts\docker\runPkg.cmd && cd cwmsjs && npm.cmd install && npm.cmd run build && cd ..
echo Done.
