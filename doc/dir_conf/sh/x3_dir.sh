#!/bin/sh
nohup ../bin/flume-ng agent --conf ../conf --conf-file ../conf/x3_dir_to_db_flume.conf  --name a1 -Dflume.root.logger=INFO,console > x3nohup.out 2>&1 &
