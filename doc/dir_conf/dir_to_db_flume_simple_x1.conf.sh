#!/bin/sh
nohup ../bin/flume-ng agent --conf conf --conf-file ../conf/dir_to_db_flume_simple_x1.conf  --name a1 -Dflume.root.logger=INFO,console > x1nohup.out 2>&1 &
