## 接入原则：

1. **核心**且**流量大**的Redis集群请放在outage窗口期进行接入

   1. “**核心Redis集群**”请和业务同事确认

   2. “**流量大的Redis集群**”标准定义（满足以下任何一个条件）

      1. 所有Redis读流量之和>=20MB

      2. 单一Redis连接数>=2000
            可以通过 info clients命令查看客户端连接，比如

         `10.15``.``93.70``:``6379``> info clients``# Clients``connected_clients:``2809``client_longest_output_list:``0``client_biggest_input_buf:``0``blocked_clients:``0`

2. **流量大**的Redis集群保证客户端升级到最新版本(CRedis客户端bug导致Redis Server连接数被打爆)

   1. java客户端版本 3.0.4 以上
   2. .net客户端版本 1.2.5.6 以上

## 接入前配置：

此部分非常重要，否则可能无法接入成功。

配置主要涉及Redis配置，以及服务器配置，详情参见：《[XPipe使用文档](http://conf.ctripcorp.com/pages/viewpage.action?pageId=113945769#XPipe使用文档-环境准备)》  章节：环境准备

## 接入流程：

### Redis接入

- 安装备份机房Redis
- 在xpipe上建立复制关联，并且确认从机房Redis同步成功
  如下图：登录XPipe管理界面，确认**所有节点**健康状态为绿色；如果为红色，代表**复制失败**；
  ![](/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.5_upgrade_to_xpipe/1.png)
- 在CRedis上修改**原有Redis服务器**信息，指定机房位置
- 在CRedis上增加备机房Redis地址，Redis状态为“无效”，机房为所在机房
  <img src="/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.5_upgrade_to_xpipe/2.png" style="zoom:50%;" />
- 修改cluster信息
  - 多IDC：“启用”
  - 主IDC位置请选择：**Redis master所在的机房**
  - 路由规则：“写master，读主机房slave”

<img src="/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.5_upgrade_to_xpipe/3.png" style="zoom:33%;" />

- 修改从机房Redis，状态为“有效”，“可读”
  <img src="/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.5_upgrade_to_xpipe/4.png" style="zoom:50%;" />
- 修改主机房master示例，状态可读
  如果路由规则为读主机房slave，即使配置为可读，默认也不会读取master
  修改原因：
  保证在切换机房过程中，有slave可以进行读取

### sentinel相关

- 在XPipe的Console上面新增shard时，需要选择每个机房对应的哨兵地址，如下所示：
  <img src="/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.5_upgrade_to_xpipe/5.png" style="zoom:40%;" />
- 选择哨兵后，XPipe会自动进行哨兵的增删操作

## DR演练流程

假设集群cluster1，主机房在金桥

- 备份金桥机房数据【重要，切换后异常情况可以回滚】

- 在XPipe上操作，将集群从金桥切换到欧阳

- 观察业务OK

- 如果金桥Redis-Server版本非XRedis版本（是的话

  跳过

  此步骤）

  - 确认路由策略为“写master，读主机房slave”或者“读写master”
  - 关闭金桥Redis
  - 在XPipe上替换金桥Redis地址为XRedis地址
  - 在CRedis上修改金桥Redis地址为XRedis地址（原来的Redis**地址必须删除**，不能以“无效”的状态存在）
  - 观察XPipe同步状态OK
    <img src="/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.5_upgrade_to_xpipe/6.png" style="zoom:33%;" />

- 回切，将集群从欧阳切换到金桥

- 观察业务OK

- DR演练结束

## DR切换

- 访问

   http://xpipe.ctripcorp.com/

  - 如果XPipe Console所在机房挂掉，可以通过修改**本机hosts文件绑定**特定IP的方式访问另外一个机房的服务器
  - 金桥机房服务器：
    - 10.8.151.22 [xpipe.ctripcorp.com](http://xpipe.ctripcorp.com/)
  - 欧阳机房服务器：
    - 10.15.206.22 [xpipe.ctripcorp.com](http://xpipe.ctripcorp.com/)