#!/bin/bash

mkfifo backpipe
#while true; do   nc -lk -p 7100  0<backpipe | nc auth 7100 1>backpipe; done
nc -lk -p ${APP_PORT:-8081} -e nc auth ${APP_PORT:-8081}