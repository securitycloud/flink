#!/bin/bash


/root/flink-0.9.1/bin/start-cluster-streaming.sh
sleep 5
/root/flink-0.9.1/bin/flink run /root/flink-0.9.1/examples/flink-1.jar "empty" > /dev/null 2>&1 &
sleep 60 
bin/stop-cluster.sh
sleep 5

/root/flink-0.9.1/bin/start-cluster-streaming.sh
sleep 5
/root/flink-0.9.1/bin/flink run /root/flink-0.9.1/examples/flink-1.jar "filter" > /dev/null 2>&1 &
sleep 60
bin/stop-cluster.sh
sleep 5

/root/flink-0.9.1/bin/start-cluster-streaming.sh
sleep 5
/root/flink-0.9.1/bin/flink run /root/flink-0.9.1/examples/flink-1.jar "count" > /dev/null 2>&1 &
sleep 60
bin/stop-cluster.sh
sleep 5

/root/flink-0.9.1/bin/start-cluster-streaming.sh
sleep 5
/root/flink-0.9.1/bin/flink run /root/flink-0.9.1/examples/flink-1.jar "aggregate" > /dev/null 2>&1 &
sleep 60
bin/stop-cluster.sh
sleep 5

/root/flink-0.9.1/bin/start-cluster-streaming.sh
sleep 5
/root/flink-0.9.1/bin/flink run /root/flink-0.9.1/examples/flink-1.jar "topN" > /dev/null 2>&1 &
sleep 60
bin/stop-cluster.sh
sleep 5

/root/flink-0.9.1/bin/start-cluster-streaming.sh
sleep 5
/root/flink-0.9.1/bin/flink run /root/flink-0.9.1/examples/flink-1.jar "scan" > /dev/null 2>&1 &
sleep 60
bin/stop-cluster.sh
sleep 5

