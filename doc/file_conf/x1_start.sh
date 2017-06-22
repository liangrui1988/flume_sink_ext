#!/bin/sh
nohup ../bin/flume-ng agent --conf conf --conf-file ../conf/db_flumex1.conf  --name a1 -Dflume.root.logger=INFO,console > x1nohup.out 2>&1 &
