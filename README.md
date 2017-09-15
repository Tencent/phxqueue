# PhxQueue

[简体中文README](README.zh_cn.md)

PhxQueue is a high-availability, high-throughput and highly reliable distributed queue based on the Paxos protocol. It guarantees At-Least-Once Delivery. It is widely used in WeChat for WeChat Pay, WeChat Media Platform, and many other important businesses.

Authors: Junjie Liang, Tao He, Haochuan Cui, Qing Huang and Jiatao Xu

Contact us: phxteam@tencent.com

[![Build Status](https://travis-ci.org/Tencent/phxqueue.svg?branch=master)](https://travis-ci.org/Tencent/phxqueue)

## Features

*	Guaranteed delivery with strict real-time reconciliation

*	Batch enqueue

*	Strictly ordered dequeue

*	Multiple subscribers

*	Dequeue speed limits

*	Dequeue replays

*	Consumer load balancing

*	All modules are scalable

*	Multi-region deployment for Store or Lock nodes

## Building automatically

```sh
git clone https://github.com/Tencent/phxqueue
cd phxqueue/
bash build.sh
```

Now that all modules are built, you can continue to [PhxQueue Quickstart](#start-a-simple-phxqueue).

## Building manually

### Download PhxQueue source

Download the [phxqueue.tar.gz](https://github.com/Tencent/phxqueue/tarball/master) and un-tar it to `$PHXQUEUE_DIR`.

### Install dependencies

*	Prepare the `$DEP_PREFIX` diectory for dependency installation:

	```sh
	export $DEP_PREFIX='/usr/local'
	```

*	Protocol Buffers and glog

	Build [Protocol Buffers](https://github.com/google/protobuf/releases) and [glog](https://github.com/google/glog/releases) with `./configure CXXFLAGS=-fPIC --prefix=$DEP_PREFIX`. Then create symlinks:

	```sh
	rm -r $PHXQUEUE_DIR/third_party/protobuf/
	rm -r $PHXQUEUE_DIR/third_party/glog/
	ln -s $DEP_PREFIX $PHXQUEUE_DIR/third_party/protobuf
	ln -s $DEP_PREFIX $PHXQUEUE_DIR/third_party/glog
	```

*	LevelDB

	Build [LevelDB](https://github.com/google/leveldb/releases) in `$PHXQUEUE_DIR/third_party/leveldb/` and `ln -s out-static lib`.

*	PhxPaxos and PhxRPC

	Build [PhxPaxos](https://github.com/Tencent/phxpaxos/releases) in `$PHXQUEUE_DIR/third_party/phxpaxos/`. Build [PhxRPC](https://github.com/Tencent/phxrpc/releases) in `$PHXQUEUE_DIR/third_party/phxrpc/`.

*	libco

	Git clone [libco](https://github.com/Tencent/libco) to `$PHXQUEUE_DIR/third_party/colib/`.

### Compile PhxQueue

```sh
cd $PHXQUEUE_DIR/
make
```

## PhxQueue distribution

PhxQueue is structured like this:

```
phxqueue/ ................. The PhxQueue root directory
├── bin/ .................. Generated binary files
├── etc/ .................. Example configuration files
├── lib/ .................. Generated library files
├── phxqueue/ ............. PhxQueue source files
├── phxqueue_phxrpc/ ...... PhxQueue with PhxRPC implementation
└── ...
```

the output files are located in `bin/` and `lib/`, while the sample configure files are located in `etc/`.

## PhxQueue quickstart

The built PhxQueue is ready to run simple demos.

### Preparing the setup

PhxQueue can be run using multiple configuration files at the same time. 
Make sure to set high enough (> 4000) open file limit with `ulimit -Sn` or `ulimit -n`.

### Starting the Store nodes

Start 3 Store nodes (add `-d` if run as daemon) as shown bellow:

```sh
bin/store_main -c etc/store_server.0.conf
bin/store_main -c etc/store_server.1.conf
bin/store_main -c etc/store_server.2.conf
```

You can follow the status of the nodes and check for any errors in these log files as shown bellow:

```sh
ps -ef | grep store_main
tail -f log/store.0/store_main.INFO
tail -f log/store.1/store_main.INFO
tail -f log/store.2/store_main.INFO
```

### Starting the Consumer nodes

Start 3 Consumer nodes:

```sh
bin/consumer_main -c etc/consumer_server.0.conf
bin/consumer_main -c etc/consumer_server.1.conf
bin/consumer_main -c etc/consumer_server.2.conf
```

You can follow the status of the nodes and check for any errors in these log files as shown bellow:

```sh
ps -ef | grep consumer_main
tail -f log/consumer.0/consumer_main.INFO
tail -f log/consumer.1/consumer_main.INFO
tail -f log/consumer.2/consumer_main.INFO
```

### Sending test requests

Now that both Store and Consumer nodes have been deployed, you can use the benchmark tool 
to send some test requests:

```sh
bin/test_producer_echo_main
```

You will get the output from test Producer:

```sh
produce echo succeeded!
```

Now let's see the output of Consumer (only 1 of 3 consumers):

```sh
consume echo succeeed! ...
```

### Running benchmarks

```sh
bin/producer_benchmark_main 10 5 5 10
```

Watch the Consumer log files:

```sh
tail -f log/consumer.0/consumer_main.INFO
tail -f log/consumer.1/consumer_main.INFO
tail -f log/consumer.2/consumer_main.INFO
```

This is an example of the output you can expect from the Consumer log:

```
INFO: Dequeue ret 0 topic 1000 sub_id 1 store_id 1 queue_id 44 size 1 prev_cursor_id 9106 next_cursor_id 9109
```

### Clearing data and logs created during testing

While testing PhxQueue, a lot of logs and data is generated. Run `log/clear_log.sh` to clear logs and `data/clear_data.sh` to delete data.
Make sure that you are running these commands against stores that does not hold any important data. 
Comands listed here will result in permanent data loss.

## Deploy distributed PhxQueue

Normally, each node should be deployed on separate machine. You need to configure `etc/*.conf`
configuration files for each node.

Files located in directory `etc/`:

```
globalconfig.conf .................Global config
topicconfig.conf ................. Topic config
storeconfig.conf ................. Store config
consumerconfig.conf ...............Consumer config
schedulerconfig.conf ..............Scheduler config
lockconfig.conf ...................Lock config
```

Deloy and modify these files on all target machines.

### Deploying Store nodes

Store is the storage module for queues, using the Paxos protocol for replica synchronization.

Deploy these configs to 3 Store nodes and start each node:

```sh
bin/store_main -c etc/store_server.0.conf -d
bin/store_main -c etc/store_server.1.conf -d
bin/store_main -c etc/store_server.2.conf -d
```

### Deploying Consumer nodes

Consumer pulls and consumes data from Store.

Deploy these configs to 3 Consumer nodes and start each node:

```sh
bin/consumer_main -c etc/consumer_server.0.conf -d
bin/consumer_main -c etc/consumer_server.1.conf -d
bin/consumer_main -c etc/consumer_server.2.conf -d
```

### Deploying Lock nodes (Optional)

Lock is a distributed lock module. You can deploy Lock independently, providing a common distributed lock service.

Set `skip_lock = 1` in `topicconfig.conf` to disable distributed Lock.

Deploy these configs to 3 Lock nodes and start each node:

```sh
bin/lock_main -c etc/lock_server.0.conf -d
bin/lock_main -c etc/lock_server.1.conf -d
bin/lock_main -c etc/lock_server.2.conf -d
```

### Deploying Scheduler nodes (Optional)

Scheduler gathers global load information from Consumer for disaster recovery and load balancing. If no Scheduler is deployed, Consumer will be assigned according to weight configured.

If you want to deploy Scheduler, you will need to deploy Lock first.

Set `use_dynamic_scale = 0` in `topicconfig.conf` to disable Scheduler.

Deploy these configs to 3 Scheduler nodes and start each node:

```sh
bin/scheduler_main -c etc/scheduler_server.0.conf -d
bin/scheduler_main -c etc/scheduler_server.1.conf -d
bin/scheduler_main -c etc/scheduler_server.2.conf -d
```

### Viewing logs

For each node, there is a log file where you can trace current node status
and errors. For example, you can access log file for Store node with ID 0 like shown bellow:

```sh
tail -f log/store.0/store_main.INFO
```

## Contribution

Please follow [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html) in PRs.

