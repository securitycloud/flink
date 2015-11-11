#!/bin/bash

scp /tmp/flink.properties sc-211:/tmp/flink.properties
scp /tmp/flink.properties sc-212:/tmp/flink.properties
scp /tmp/flink.properties sc-213:/tmp/flink.properties
scp /tmp/flink.properties sc-215:/tmp/flink.properties

scp /tmp/flink_count_window.properties sc-211:/tmp/flink_count_window.properties
scp /tmp/flink_count_window.properties sc-212:/tmp/flink_count_window.properties
scp /tmp/flink_count_window.properties sc-213:/tmp/flink_count_window.properties
scp /tmp/flink_count_window.properties sc-215:/tmp/flink_count_window.properties

scp /root/flink-0.9.1/conf/flink-conf.yaml sc-211:/root/flink-0.9.1/conf/flink-conf.yaml
scp /root/flink-0.9.1/conf/flink-conf.yaml sc-212:/root/flink-0.9.1/conf/flink-conf.yaml
scp /root/flink-0.9.1/conf/flink-conf.yaml sc-213:/root/flink-0.9.1/conf/flink-conf.yaml
scp /root/flink-0.9.1/conf/flink-conf.yaml sc-215:/root/flink-0.9.1/conf/flink-conf.yaml
