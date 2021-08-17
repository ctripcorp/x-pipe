x-pipe
================

[![Build Status](https://travis-ci.com/ctripcorp/x-pipe.svg?branch=master)](https://travis-ci.com/ctripcorp/x-pipe)
[![Coverage Status](https://coveralls.io/repos/github/ctripcorp/x-pipe/badge.svg?branch=master)](https://coveralls.io/github/ctripcorp/x-pipe?branch=master)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/8884/badge.svg)](https://scan.coverity.com/projects/ctripcorp-x-pipe)
[![QualityGate](https://cloud.quality-gate.com/dashboard/api/badge?projectName=ctripcorp_x-pipe&branchName=master)](https://cloud.quality-gate.com/dashboard/branches/3169#overview)


<!-- MarkdownTOC -->

- [XPipe 解决什么问题](#xpipe-解决什么问题)
- [系统详述](#系统详述)
    - [整体架构](#整体架构)
    - [Redis 数据复制问题](#redis-数据复制问题)
    - [机房切换](#机房切换)
        - [切换流程](#切换流程)
    - [高可用](#高可用)
        - [XPipe 系统高可用](#xpipe-系统高可用)
        - [Redis 自身高可用](#redis-自身高可用)
    - [测试数据](#测试数据)
        - [延时测试](#延时测试)
    - [跨公网部署及架构](#跨公网部署及架构)
- [深入了解](#深入了解)
- [技术交流](#技术交流)
- [License](#license)

<!-- /MarkdownTOC -->


<a name="xpipe-解决什么问题"></a>
# XPipe 解决什么问题
Redis 在携程内部得到了广泛的使用，根据客户端数据统计，整个携程全部 Redis 的读写请求在每秒 2000W，其中写请求约 100W，很多业务甚至会将 Redis 当成内存数据库使用。这样，就对 Redis 多数据中心提出了很大的需求，一是为了提升可用性，解决数据中心 DR(Disaster Recovery) 问题，二是提升访问性能，每个数据中心可以读取当前数据中心的数据，无需跨机房读数据，在这样的需求下，XPipe 应运而生 。  

为了方便描述，后面用 DC 代表数据中心 (Data Center)。

<a name="系统详述"></a>
# 系统详述
<a name="整体架构"></a>
## 整体架构
整体架构图如下所示：  
![design](https://raw.github.com/ctripcorp/x-pipe/master/doc/image/total.jpg)  

- Console 用来管理多机房的元信息数据，同时提供用户界面，供用户进行配置和 DR 切换等操作。
- Keeper 负责缓存 Redis 操作日志，并对跨机房传输进行压缩、加密等处理。
- Meta Server 管理单机房内的所有 keeper 状态，并对异常状态进行纠正。

<a name="redis-数据复制问题"></a>
## Redis 数据复制问题
多数据中心首先要解决的是数据复制问题，即数据如何从一个 DC 传输到另外一个 DC。我们决定采用伪 slave 的方案，即实现 Redis 协议，伪装成为 Redis slave，让 Redis master 推送数据至伪 slave。这个伪 slave，我们把它称为 keeper，如下图所示：  
![keepers](https://raw.github.com/ctripcorp/x-pipe/master/doc/image/keepers.jpg)  

使用 keeper 带来的优势  

- 减少 master 全量同步  
如果异地机房 slave 直接连向 master，多个 slave 会导致 master 多次全量同步，而 keeper 可以缓存 rdb 和 replication log，异地机房的 slave 直接从 keeper 获取数据，增强 master 的稳定性。
- 减少多数据中心网络流量  
在两个数据中心之间，数据只需要通过 keeper 传输一次，且 keeper 之间传输协议可以自定义，方便支持压缩 (目前暂未支持)。
- 网络异常时减少全量同步  
keeper 将 Redis 日志数据缓存到磁盘，这样，可以缓存大量的日志数据 (Redis 将数据缓存到内存 ring buffer，容量有限)，当数据中心之间的网络出现较长时间异常时仍然可以续传日志数据。  
- 安全性提升  
多个机房之间的数据传输往往需要通过公网进行，这样数据的安全性变得极为重要，keeper 之间的数据传输也可以加密 (暂未实现)，提升安全性。

<a name="机房切换"></a>
## 机房切换
<a name="切换流程"></a>
### 切换流程
-   检查是否可以进行 DR 切换  
    类似于 2PC 协议，首先进行 prepare，保证流程能顺利进行。
-   原主机房 master 禁止写入  
此步骤，保证在迁移的过程中，只有一个 master，解决在迁移过程中可能存在的数据丢失情况。
-   提升新主机房 master
-   其它机房向新主机房同步

同时提供回滚和重试功能。回滚功能可以回滚到初始的状态，重试功能可以在 DBA 人工介入的前提下，修复异常条件，继续进行切换。
<a name="高可用"></a>
## 高可用
<a name="xpipe-系统高可用"></a>
### XPipe 系统高可用
如果 keeper 挂掉，多数据中心之间的数据传输可能会中断，为了解决这个问题，keeper 有主备两个节点，备节点实时从主节点复制数据，当主节点挂掉后，备节点会被提升为主节点，代替主节点进行服务。

提升的操作需要通过第三方节点进行，我们把它称之为 MetaServer，主要负责 keeper 状态的转化以及机房内部元信息的存储。同时 MetaServer 也要做到高可用：每个 MetaServer 负责特定的 Redis 集群，当有 MetaServer 节点挂掉时，其负责的 Redis 集群将由其它节点接替；如果整个集群中有新的节点接入，则会自动进行一次负载均衡，将部分集群移交到此新节点。
<a name="redis-自身高可用"></a>
### Redis 自身高可用
Redis 也可能会挂，Redis 本身提供哨兵 (Sentinel) 机制保证集群的高可用。但是在 Redis4.0 版本之前，提升新的 master 后，其它节点连到此节点后都会进行全量同步，全量同步时，slave 会处于不可用状态；master 将会导出 rdb，降低 master 的可用性；同时由于集群中有大量数据 (RDB) 传输，将会导致整体系统的不稳定。  

截止当前文章书写之时，4.0 仍然没有发布 release 版本，而且携程内部使用的 Redis 版本为 2.8.19，如果升到 4.0，版本跨度太大，基于此，我们在 Redis3.0.7 的版本基础上进行优化，实现了 psync2.0 协议，实现了增量同步。下面是 Redis 作者对协议的介绍：[psync2.0](https://gist.github.com/antirez/ae068f95c0d084891305)。

[携程内部 Redis 地址链接](https://github.com/ctripcorp/redis)

<a name="测试数据"></a>
## 测试数据
<a name="延时测试"></a>
### 延时测试
#### 测试方案
测试方式如下图所示。从 client 发送数据至 master，并且 slave 通过 keyspace notification 的方式通知到 client，整个测试延时时间为 t1+t2+t3。  
![test](https://raw.github.com/ctripcorp/x-pipe/master/doc/image/delay.jpg)  
#### 测试数据
首先我们测试 Redis master 直接复制到 slave 的延时，为 0.2ms。然后在 master 和 slave 之间增加一层 keeper，整体延时增加 0.1ms，到 0.3ms。

在携程生产环境进行了测试，生产环境两个机房之间的 ping RTT 约为 0.61ms，经过跨数据中心的两层 keeper 后，测试得到的平均延时约为 0.8ms，延时 99.9 线为 2ms。

<a name="跨公网部署及架构"></a>
## 跨公网部署及架构
[详情参考 -- 跨公网部署及架构](https://github.com/ctripcorp/x-pipe/blob/master/doc/proxy.md)

<a name="深入了解"></a>
# 深入了解
- 【有任何疑问，请阅读】[XPipe Wiki](https://github.com/ctripcorp/x-pipe/wiki) 
- 【目前用户的问题整理】[XPipe Q&A](https://github.com/ctripcorp/x-pipe/wiki/XPipe-Q&A)
- 【文章】[携程Redis多数据中心解决方案-XPipe](https://mp.weixin.qq.com/s/Q3bt0-5nv8uNMdHuls-Exw?)
- 【文章】[携程Redis海外机房数据同步实践](https://mp.weixin.qq.com/s/LeSSdT6bOEFzZyN26PRVzg)

<a name="技术交流"></a>
# 技术交流
![tech-support-qq](https://raw.github.com/ctripcorp/x-pipe/master/doc/xpipe_qq.png)


<a name="license"></a>
# License
The project is licensed under the [Apache 2 license](https://github.com/ctripcorp/x-pipe/blob/master/LICENSE).
