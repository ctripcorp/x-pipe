x-pipe
================

### [master]
[![Build Status](https://travis-ci.org/ctripcorp/x-pipe.svg?branch=master)](https://travis-ci.org/ctripcorp/x-pipe)
[![Coverage Status](https://coveralls.io/repos/github/ctripcorp/x-pipe/badge.svg?branch=master)](https://coveralls.io/github/ctripcorp/x-pipe?branch=master)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/8884/badge.svg)](https://scan.coverity.com/projects/ctripcorp-x-pipe)

### [dev]
[![Build Status](https://travis-ci.org/ctripcorp/x-pipe.svg?branch=dev)](https://travis-ci.org/ctripcorp/x-pipe)
[![Coverage Status](https://coveralls.io/repos/github/ctripcorp/x-pipe/badge.svg?branch=dev)](https://coveralls.io/github/ctripcorp/x-pipe?branch=dev)

<!-- MarkdownTOC -->

- [XPipe解决什么问题](#xpipe解决什么问题)
- [系统详述](#系统详述)
    - [整体架构](#整体架构)
    - [Redis数据复制问题](#redis数据复制问题)
    - [机房切换](#机房切换)
        - [切换流程](#切换流程)
    - [高可用](#高可用)
        - [XPipe系统高可用](#xpipe系统高可用)
        - [Redis自身高可用](#redis自身高可用)
    - [测试数据](#测试数据)
        - [延时测试](#延时测试)

<!-- /MarkdownTOC -->


<a name="xpipe解决什么问题"></a>
# XPipe解决什么问题
Redis在携程内部得到了广泛的使用，根据客户端数据统计，整个携程全部Redis的读写请求在200W QPS/s，其中写请求约10W QPS/S，很多业务甚至会将Redis当成内存数据库使用。这样，就对Redis多数据中心提出了很大的需求，一是为了提升可用性，解决数据中心DR(Disaster Recovery)问题，二是提升访问性能，每个数据中心可以读取当前数据中心的数据，无需跨机房读数据，在这样的需求下，XPipe应运而生 。  

为了方便描述，后面用DC代表数据中心(Data Center)。

<a name="系统详述"></a>
# 系统详述
<a name="整体架构"></a>
## 整体架构
整体架构图如下所示：  
![design](https://raw.github.com/ctripcorp/x-pipe/master/doc/image/total.jpg)  

- Console用来管理多机房的元信息数据，同时提供用户界面，供用户进行配置和DR切换等操作。
- Keeper负责缓存Redis操作日志，并对跨机房传输进行压缩、加密等处理。
- Meta Server管理单机房内的所有keeper状态，并对异常状态进行纠正。

<a name="redis数据复制问题"></a>
## Redis数据复制问题
多数据中心首先要解决的是数据复制问题，即数据如何从一个DC传输到另外一个DC。我们决定采用伪slave的方案，即实现Redis协议，伪装成为Redis slave，让Redis master推送数据至伪slave。这个伪slave，我们把它称为keeper，如下图所示：  
![keepers](https://raw.github.com/ctripcorp/x-pipe/master/doc/image/keepers.jpg)  

有了keeper之后，多数据中心之间的数据传输，可以通过keeper进行。keeper将Redis日志数据缓存到磁盘，这样，可以缓存大量的日志数据(Redis将数据缓存到内存ring buffer，容量有限)，当数据中心之间的网络出现较长时间异常时仍然可以续传日志数据。  

Redis协议不可更改，而keeper之间的数据传输协议却可以自定义。这样就可以进行压缩，以提升系统性能，节约传输成本；多个机房之间的数据传输往往需要通过公网进行，这样数据的安全性变得极为重要，keeper之间的数据传输也可以加密，提升安全性。
<a name="机房切换"></a>
## 机房切换
<a name="切换流程"></a>
### 切换流程
-   检查是否可以进行DR切换  
    类似于2PC协议，首先进行prepare，保证流程能顺利进行。
-   原主机房master禁止写入  
此步骤，保证在迁移的过程中，只有一个master，解决在迁移过程中可能存在的数据丢失情况。
-   提升新主机房master
-   其它机房向新主机房同步

同时提供回滚和重试功能。回滚功能可以回滚到初始的状态，重试功能可以在DBA人工介入的前提下，修复异常条件，继续进行切换。
<a name="高可用"></a>
## 高可用
<a name="xpipe系统高可用"></a>
### XPipe系统高可用
如果keeper挂掉，多数据中心之间的数据传输可能会中断，为了解决这个问题，keeper有主备两个节点，备节点实时从主节点复制数据，当主节点挂掉后，备节点会被提升为主节点，代替主节点进行服务。

提升的操作需要通过第三方节点进行，我们把它称之为MetaServer，主要负责keeper状态的转化以及机房内部元信息的存储。同时MetaServer也要做到高可用：每个MetaServer负责特定的Redis集群，当有MetaServer节点挂掉时，其负责的Redis集群将由其它节点接替；如果整个集群中有新的节点接入，则会自动进行一次负载均衡，将部分集群移交到此新节点。
<a name="redis自身高可用"></a>
### Redis自身高可用
Redis也可能会挂，Redis本身提供哨兵(Sentinel)机制保证集群的高可用。但是在Redis4.0版本之前，提升新的master后，其它节点连到此节点后都会进行全量同步，全量同步时，slave会处于不可用状态；master将会导出rdb，降低master的可用性；同时由于集群中有大量数据(RDB)传输，将会导致整体系统的不稳定。  

截止当前文章书写之时，4.0仍然没有发布release版本，而且携程内部使用的Redis版本为2.8.19，如果升到4.0，版本跨度太大，基于此，我们在Redis3.0.7的版本基础上进行优化，实现了psync2.0协议，实现了增量同步。下面是Redis作者对协议的介绍：[psync2.0](https://gist.github.com/antirez/ae068f95c0d084891305)。

[携程内部Redis地址链接](https://github.com/ctripcorp/redis)

<a name="测试数据"></a>
## 测试数据
<a name="延时测试"></a>
### 延时测试
#### 测试方案
测试方式如下图所示。从client发送数据至master，并且slave通过keyspace notification的方式通知到client，整个测试延时时间为t1+t2+t3。  
![test](https://raw.github.com/ctripcorp/x-pipe/master/doc/image/delay.jpg)  
#### 测试数据
首先我们测试Redis master直接复制到slave的延时，为0.2ms。然后在master和slave之间增加一层keeper，整体延时增加0.1ms，到0.3ms。

在携程生产环境进行了测试，生产环境两个机房之间的ping RTT约为0.61ms，经过跨数据中心的两层keeper后，测试得到的平均延时约为0.8ms，延时99.9线为2ms。
