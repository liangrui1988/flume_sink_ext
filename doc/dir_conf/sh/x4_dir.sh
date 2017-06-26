#!/bin/sh
nohup ../bin/flume-ng agent --conf ../conf --conf-file ../conf/x4_dir_to_db_flume.conf  --name a1 -Dflume.root.logger=INFO,console > x4nohup.out 2>&1 &
