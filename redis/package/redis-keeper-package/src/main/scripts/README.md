# Redis Sentinel部署使用手册

### 简介

批量部署redis和sentinel



### 环境准备

0.修改系统配置(可选，操作需谨慎)

根据启动的redis和sentinel数量，以及机器配置情况，决定是否需要进行此操作

sudo权限执行 change_system_config.sh

此脚本会修改系统级别以及systemd级别的句柄、进程数的最大值

脚本执行后需重启生效，可执行 sudo reboot命令重启



1.redis可执行文件

(1)在系统环境变量中存在redis-server, redis-cli, redis-sentinel 命令

(2)或在当前用户的home目录下，新建redis文件夹，文件夹内放入redis-server, redis-cli, redis-sentinel可执行文件

当(1)(2)同时存在时，优先使用~/redis/文件夹下的可执行文件



2.参数配置

新建 ~/redis/config文件，内容如下：

```
redis_num=4
redis_start_port=20000
sentinel_num=2
sentinel_start_port=5000
```

依次表示redis实例数量、redis开始的端口、sentinel数量、sentinel开始的端口，可自定义

redis和sentinel实例启动的端口连续。当如示例中配置时，redis启动在20000, 20001, 20002, 20003端口，sentinel启动在5000, 50001端口



3.如果通过公司内发布系统发布，需要在 /opt/settings/server.properties 中添加配置 role=REDIS

### 启动

启动分为两种模式：主机房和备机房

​	主机房模式：

​		启动命令： ./start_all active

​		具体解释：会以主从模式启动，复制关系默认为 端口n+1 slaveof 端口n ， 其中n为偶数。因此建议config中的redis_start_port参数配置为偶数；sentinel不会monitor实例

​	备机房模式：

​		启动命令:   ./start_all backup

​		具体解释：全部redis均为master，sentinel不会monitor实例



### 日志

日志路径：

/opt/data/redis/$((port/1000))/$port.log

/opt/data/sentinel/$port.log



### 关闭

关闭所有的redis和sentinel

关闭命令：./stop_all