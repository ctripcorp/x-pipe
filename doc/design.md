
<!-- MarkdownTOC -->

1. [总体设计](#总体设计)
    1. [设计原则](#设计原则)
    1. [总体设计图](#总体设计图)
    1. [机房之间交互](#机房之间交互)
    1. [机房内交互](#机房内交互)
1. [console](#console)
    1. [db存储](#db存储)
1. [zk](#zk)
    1. [zk提供功能](#zk提供功能)
1. [meta server](#meta-server)
    1. [meta server互相通知](#meta-server互相通知)
    1. [功能](#功能)
    1. [角色](#角色)
    1. [任务划分](#任务划分)
    1. [master/slave](#masterslave)
    1. [任务执行](#任务执行)
    1. [API](#api)
    1. [状态查询](#状态查询)
1. [keeper](#keeper)
    1. [keeper状态变化](#keeper状态变化)
    1. [命令](#命令)

<!-- /MarkdownTOC -->

<a name="总体设计"></a>
# 总体设计
<a name="设计原则"></a>
## 设计原则
1. 机房通信异常，单机房内可以正常工作
1. 
<a name="总体设计图"></a>
## 总体设计图
<a name="机房之间交互"></a>
## 机房之间交互
1. 用户配置下发执行  
    * keeper增加、删除、启动、停止
    * event写到zookeeper
    * meta server监听event，执行
1. 手工变化active dc
    * 变化特定cluster的active dc
    * 变化所有的active dc
1. redis 挂掉
    * setinel通知meta server
    * meta server转发任务至meta master
    * meta server master调用keeper，执行slaveof no one destip destport
    * 通知console新的变化，console通知第三方系统（比如redis配置中心）
1. keeper active变化
    * meta server发现keeper active变化
    * meta server执行keeper active变化的操作
        - 通知keeper 由backup转active(keeper setstate active)/ checkstate(keeper getstate)
        - 通知redis slave: kslaveof ip port
    * 通知console，
    * console通知关联meta server
        - /api/{cluster}/{shard}/upstreamkeeper/ip/port
1. 主动切换机房
    * console发出切换机房指令至active的meta server : /{cluster}/backup
    * meta server终止setinel
    * 当前active机房的master变成slave:  kslaveof keeperip keeperport
    * keeper等待数据传输至其他机房完毕
    * console发出指令至被提升的meta server:  /{cluster}/active
    *   被提升机房增加setinel监控
    *   setinel选举master，通知meta提升master
    * console发出指令至其它backup机房，/{cluster}/upstreamchanged/ip:port
      
1. 机房挂掉，切换机房
    * 
<a name="机房内交互"></a>
## 机房内交互
<a name="console"></a>
# console
<a name="db存储"></a>
## db存储
<a name="zk"></a>
# zk
<a name="zk提供功能"></a>
## zk提供功能
1. leader选举
    1. 选举出keeper的master  
    1. 选举出meta server的master  
1. 事件通知(X)
    直接调用api通知
1. meta server sharding?
    初始化
    resharding
        meta server挂
        主动触发
    api/shard/add
    api/shard/delete
    api/shard/export
    api/shard/emport
1. _meta信息存储_

event:
    dc1  
        cluster1  keeperAdd keeperDelte  promoteMaster  redisMasterChanged keeperActiveChanged
        cluster2 
    dc2

meta
    dc1(只存储此dc内的信息)
        metaserver1
        metaserver2
        cluster : {config} {version}
            shard  {upstreamKeeper: ""} 
                keeper 
                keeper 
                redis 
                redis

elector  
    dc1 
        metaserver  
        cluster  
            shard  
                keeper  

<a name="meta-server"></a>
# meta server

<a name="meta-server互相通知"></a>
## meta server互相通知
1. 通过console
1. 通过zk, ureka
1. 通过apollo

<a name="功能"></a>
## 功能
1. 执行任务
1. 执行监控监测，监测节点健康状态

<a name="角色"></a>
## 角色
* master
    - cluster resharding(meta server挂)
    - masterChanged API调用
* slave 

<a name="任务划分"></a>
## 任务划分
根据cluster分meta server
resharding

<a name="masterslave"></a>
## master/slave
* master负责任务执行、更新API
* slave查询
* 如果更新API调用到slave，转发至master
<a name="任务执行"></a>
## 任务执行
* 消费events目录下事件
* API提供接口供调用
    * redis master changed 
<a name="api"></a>
## API
* 查询
    - GET /api/{cluster}/{shard}
    - GET /api/{clusterId}/{shard}/redis/master
    - GET /api/{clusterId}/{shard}/keeper/active
    - GET /api/{cluster}/{shard}/arch(info replication view)
* 更改
    - POST /api/{cluster}/{shard}/redis/master/change
    slave转发到master执行
    - 
<a name="状态查询"></a>
## 状态查询
<a name="keeper"></a>
# keeper
<a name="keeper状态变化"></a>
## keeper状态变化
1. keeper启动
    * 如果为active，则连接对应的redismaster或者upstream
    * 如果为backup，则连接对应的active
    * 如果为null，状态置为unknown
1. meta server发现注册的keeper变化
    * 选举keeper active
        - 获取所有keeper状态，如果有active，则active设置为active的keeper
            + 通知所有keeper状态变化
        - 如果没有，选举出active
            + 通知所有keeper状态变化




<a name="命令"></a>
## 命令
1. keeper getstate
1. keeper setstate  



