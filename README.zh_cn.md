# PhxQueue

PhxQueue是微信开源的一款基于Paxos协议实现的高可用、高吞吐和高可靠的分布式队列，保证At-Least-Once Delivery。在微信内部广泛支持微信支付、公众平台等多个重要业务。

作者：Junjie Liang, Tao He, Haochuan Cui, Qing Huang and Jiatao Xu

联系我们：phxteam@tencent.com

[![Build Status](https://travis-ci.org/Tencent/phxqueue.svg?branch=master)](https://travis-ci.org/Tencent/phxqueue)

## 主要特性

* 同步刷盘，入队数据绝对不丢，自带内部实时对账

* 出入队严格有序

* 多订阅

* 出队限速

* 出队重放

* 所有模块均可平行扩展

* 存储层批量刷盘、同步，保证高吞吐

* 存储层支持同城多中心部署

* 存储层自动容灾/接入均衡

* 消费者自动容灾/负载均衡

## 自动构建

```sh
git clone https://github.com/Tencent/phxqueue
cd phxqueue/
bash build.sh
```

## 手动编译

### 下载源代码

下载[phxqueue.tar.gz](https://github.com/Tencent/phxqueue/tarball/master)并解压到`$PHXQUEUE_DIR`.

### 安装第三方依赖

*	选择一个目录`$DEP_PREFIX`作为安装依赖的路径，例如：

	```sh
	export $DEP_PREFIX='/usr/local'
	```

*	Protocol Buffers和glog

	编译[Protocol Buffers](https://github.com/google/protobuf/releases)和[glog](https://github.com/google/glog/releases)，注意必须使用编译参数`./configure CXXFLAGS=-fPIC --prefix=$DEP_PREFIX`。然后创建软链接：

	```sh
	rm -r $PHXQUEUE_DIR/third_party/protobuf/
	rm -r $PHXQUEUE_DIR/third_party/glog/
	ln -s $DEP_PREFIX $PHXQUEUE_DIR/third_party/protobuf
	ln -s $DEP_PREFIX $PHXQUEUE_DIR/third_party/glog
	```

*	LevelDB

	编译[LevelDB](https://github.com/google/leveldb/releases)到`$PHXQUEUE_DIR/third_party/leveldb/`，然后`ln -s out-static lib`。

*	PhxPaxos和PhxRPC

	编译[PhxPaxos](https://github.com/Tencent/phxpaxos/releases)到`$PHXQUEUE_DIR/third_party/phxpaxos/`；编译 [PhxRPC](https://github.com/Tencent/phxrpc/releases)到`$PHXQUEUE_DIR/third_party/phxrpc/`。

*	libco

	使用Git clone [libco](https://github.com/Tencent/libco)到`$PHXQUEUE_DIR/third_party/colib/`。

### 编译PhxQueue

```sh
cd $PHXQUEUE_DIR/
make
```

## PhxQueue项目目录结构

PhxQueue的目录结构如下：

```
phxqueue/ .................... PhxQueue根目录
├── bin/ ..................... 生成的二进制文件
├── etc/ ..................... 配置文件模板
├── lib/ ..................... 生成的库文件
├── phxqueue/ ................ PhxQueue源文件
├── phxqueue_phxrpc/ ......... PhxQueue的PhxRPC实现
└── ...
```

输出文件在`bin/`和`lib/`，示例配置文件在`etc/`。

## 启动一个简单的PhxQueue

编译好的PhxQueue源代码目录可以直接运行简单的演示。

### 准备

PhxQueue需要同时使用多个文件描述符。请保证设置`ulimit -Sn`和`ulimit -n`到足够大（4000以上）。

### 启动Store

启动3个Store节点（加`-d`参数可以在后台运行）：

```sh
bin/store_main -c etc/store_server.0.conf
bin/store_main -c etc/store_server.1.conf
bin/store_main -c etc/store_server.2.conf
```

可以查看Store的日志：

```sh
ps -ef | grep store_main
tail -f log/store.0/store_main.INFO
tail -f log/store.1/store_main.INFO
tail -f log/store.2/store_main.INFO
```

### 启动Consumer

启动3个Consumer节点：

```sh
bin/consumer_main -c etc/consumer_server.0.conf
bin/consumer_main -c etc/consumer_server.1.conf
bin/consumer_main -c etc/consumer_server.2.conf
```

可以查看Consumer的日志：

```sh
ps -ef | grep consumer_main
tail -f log/consumer.0/consumer_main.INFO
tail -f log/consumer.1/consumer_main.INFO
tail -f log/consumer.2/consumer_main.INFO
```

### 发送测试请求

现在简单的PhxQueue已经部署完成！使用工具就可以发送测试请求了：

```sh
bin/test_producer_echo_main
```

你会看到Producer的输出：

```sh
produce echo succeeded!
```

我们回到Consumer看输出（3个中只有1个会处理请求并输出）

```sh
consume echo succeeed! ...
```

### 运行压测

```sh
bin/producer_benchmark_main 10 5 5 10
```

再次观看Consumer的日志：

```sh
tail -f log/consumer.0/consumer_main.INFO
tail -f log/consumer.1/consumer_main.INFO
tail -f log/consumer.2/consumer_main.INFO
```

你会发现有类似这样的Consumer消费出队请求的日志：

```
INFO: Dequeue ret 0 topic 1000 sub_id 1 store_id 1 queue_id 44 size 1 prev_cursor_id 9106 next_cursor_id 9109
```

### 清理测试日志和数据

在运行PhxQueue的过程中，会产生大量的日志和数据。运行`log/clear_log.sh`以清理日志，运行`data/clear_data.sh`以删除数据。删除前务必确认数据已经没有用。

## 部署分布式的PhxQueue

通常，每个节点应该部署在一台机器上，针对每个节点修改`etc/*.conf`中的内容。

### 部署全局配置

在`etc/`目录下，有以下这些文件

```
globalconfig.conf .................全局配置
topicconfig.conf ................. 主题配置
storeconfig.conf ................. Store配置
consumerconfig.conf ...............Consumer配置
schedulerconfig.conf ..............Scheduler配置
lockconfig.conf ...................Lock配置
```

将这些文件部署在所有目标机器上，并做相应的修改。

### 部署Store

Store是队列的存储，使用Paxos协议作副本同步。

将以下3个配置文件分别部署到3个Store节点并启动：

```sh
bin/store_main -c etc/store_server.0.conf -d
bin/store_main -c etc/store_server.1.conf -d
bin/store_main -c etc/store_server.2.conf -d
```

### 部署Consumer

Consumer是队列的消费者，以批量拉取的方式从Store拉数据。

将以下3个配置文件分别部署到3个Consumer节点并启动：

```sh
bin/consumer_main -c etc/consumer_server.0.conf -d
bin/consumer_main -c etc/consumer_server.1.conf -d
bin/consumer_main -c etc/consumer_server.2.conf -d
```

### 部署Lock（可选）

Lock是一个分布式锁，其接口设计非常通用化，使用者可以选择将Lock独立部署，提供通用分布式锁服务。部署Lock可以避免队列的重复消费。

如果不使用Lock，`topicconfig.conf`中需要设置`skip_lock = 1`。

将以下3个配置文件分别部署到3个Lock节点并启动：

```sh
bin/lock_main -c etc/lock_server.0.conf -d
bin/lock_main -c etc/lock_server.1.conf -d
bin/lock_main -c etc/lock_server.2.conf -d
```

### 部署Scheduler（可选）

Scheduler收集Consumer全局负载信息, 对Consumer做容灾和负载均衡。当使用者没有这方面的需求时，可以省略部署Scheduler，此时各Consumer根据配置权重决定与队列的处理关系。

Scheduler依赖Lock，如果需要部署Scheduler，请先部署Lock。

如果不使用Scheduler，`topicconfig.conf`中需要设置`use_dynamic_scale = 0`。

将以下3个配置文件分别部署到3个Scheduler节点并启动：

```sh
bin/scheduler_main -c etc/scheduler_server.0.conf -d
bin/scheduler_main -c etc/scheduler_server.1.conf -d
bin/scheduler_main -c etc/scheduler_server.2.conf -d
```

### 查看日志

各个模块的日志位于'log/'下模块名子目录中，例如store的0号节点日志：

```sh
tail -f log/store.0/store_main.INFO
```

## 贡献

PR代码请遵守[Google C++ 风格指南](https://google.github.io/styleguide/cppguide.html)。

