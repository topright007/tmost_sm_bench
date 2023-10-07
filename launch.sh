#!/bin/bash

docker-compose down && \
 docker-compose rm -f && \
 docker-compose up -d && \
 sleep 5 && ./migration.sh