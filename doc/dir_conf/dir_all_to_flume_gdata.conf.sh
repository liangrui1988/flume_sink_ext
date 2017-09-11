#!/bin/sh
nohup bin/flume-ng agent --conf conf --conf-file conf/dir_all_to_flume_gdata.conf  --name a1 -Dflume.root.logger=INFO,console &
