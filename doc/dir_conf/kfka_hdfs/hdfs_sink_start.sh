#!/bin/sh
nohup ../bin/flume-ng agent --conf conf --conf-file ../conf/flume_kafka_to_hdfs.conf  --name a1 -Dflume.root.logger=INFO,console > x1nohup.out 2>&1 &
