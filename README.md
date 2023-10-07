# tmost_state_machine_bench

#emulate network delay
https://medium.com/@kazushi/simulate-high-latency-network-using-docker-containerand-tc-commands-a3e503ea4307
```bash
docker exec postgres tc qdisc add dev eth0 root netem delay 1ms
docker exec postgres tc qdisc del dev eth0 root netem delay 1ms

docker exec postgres tc qdisc add dev eth0 root netem delay 30ms
docker exec postgres tc qdisc del dev eth0 root netem delay 30ms
```
