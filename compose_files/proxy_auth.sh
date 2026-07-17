#!/bin/bash

mkfifo backpipe
nc -lk -p ${AUTH_PORT:-7100} -e nc auth ${AUTH_PORT:-7100}