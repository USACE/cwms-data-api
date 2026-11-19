#!/bin/sh

nc -lk -p ${APP_PORT:-8081} -e nc auth ${APP_PORT:-8081}