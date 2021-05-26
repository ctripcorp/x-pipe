## AWS 机房故障操作方案

AWS 机房部分 redis 故障, 需要重新上线 redis. 可以先把新上线 redis 作为 slave, 接入 AWS 端的同一 group 中 redis 进行同步.

同步完成后, 再在 xpipe 上添加 redis, 由 xpipe 进行管理

## 接入流程

- 接入之前, 请先确认 Redis 实例健康状态良好, 并且 CRedis 端已经部署好

  - 接入 XPipe 前, 先将 CMS 上相关 Redis Cluster 设置为 xpipe DR

  - 调整 CMS 上 Redis Cluster 的路由策略 (写 Master 读主机房 Slave)

  - 添加 AWS 的 Redis 实例, 将 Redis 的 IDC 设置为相应站点 (目前法兰克福为 FRA-AWS)

  - Redis 实例的 Master 至为 unkown

    

- 针对存量集群, 绑定数据中心 (新接入集群略过)

![](/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.4_oversea_how_to_use_for_dba/1.png)

![](/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.4_oversea_how_to_use_for_dba/2.png)

- 参考 [XPipe使用文档](http://conf.ctripcorp.com/pages/viewpage.action?pageId=113945769) 添加相关 Redis 和 Keeper

![](/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.4_oversea_how_to_use_for_dba/3.png)