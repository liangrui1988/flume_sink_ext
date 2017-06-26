#!/bin/sh
nohup ../bin/flume-ng agent --conf ../conf --conf-file ../conf/x8_dir_to_db_flume.conf  --name a1 -Dflume.root.logger=INFO,console > x8nohup.out 2>&1 &
