#!/bin/bash

set -x
ulimit -Sn 4096
ulimit -n 4096

cd /phxqueue/
bin/store_main -d -c etc/store_server.0.conf
bin/store_main -d -c etc/store_server.1.conf
bin/store_main -d -c etc/store_server.2.conf
bin/consumer_main -d -c etc/consumer_server.0.conf
bin/consumer_main -d -c etc/consumer_server.1.conf
bin/consumer_main -d -c etc/consumer_server.2.conf
bin/lock_main -d -c etc/lock_server.0.conf
bin/lock_main -d -c etc/lock_server.1.conf
bin/lock_main -d -c etc/lock_server.2.conf
bin/scheduler_main -d -c etc/scheduler_server.0.conf
bin/scheduler_main -d -c etc/scheduler_server.1.conf
bin/scheduler_main -d -c etc/scheduler_server.2.conf

#ps -ef | grep store_main
#ps -ef | grep lock_main
#ps -ef | grep scheduler_main
#ps -ef | grep consumer_main

exec tail -f /dev/null

