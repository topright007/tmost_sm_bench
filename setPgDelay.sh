#!/bin/bash

echo "setting PG delay to $1"
docker exec postgres tc qdisc del dev eth0 root netem delay 1ms
docker exec postgres tc qdisc add dev eth0 root netem delay "$1"
