## 接入注意事项

### XPipe

-  XPipe使用Redis版本为[XRedis](http://conf.ctripcorp.com/pages/viewpage.action?pageId=113945769#XPipe使用文档-XRedis)，最新版本基于Redis 4.0.8更改，支持4.0.8所有功能（目前生产环境版本2.8.19）
- 优势
  - 增量同步
    老的2.8.19的Redis在切换过程中全量同步，XRedis可以在切换过程做到增量，提升切换性能和系统可用性。
- XRedis版本下载：[XPipe使用文档#XRedis](http://conf.ctripcorp.com/pages/viewpage.action?pageId=113945769#XPipe使用文档-XRedis)

### CRedis(强依赖)

-  CRedis支持三种路由模式，可以在CRedis配置自动更新生效

- -  读写master
  -  写主机房，读取主机房slave
    - 如果主机房slave挂掉，自动切换到读取master
  -  写主机房，就近读取
    - 主机房应用：如果主机房slave挂掉，自动切换到读取master
    - 从机房应用：如果从机房slave挂掉，自动切换到读取主机房slave；主机房slave挂掉，业务报错，不会切到master(防止对master过大压力)

- CRedis路由生效时间

  - 老版本CRedis平均路由生效时间60S
    - .net 版本 < 1.2.5.0
    - java 版本 < 2.5.2
  - 支持路由立即生效（**强烈推荐使用**，可以将切换机房时间降低到5秒内, 延时和打点对用户更加友好）
    - .net 版本>=1.3.1.5
    - java 版本>=4.3.6

### 接入流程

- 升级代码CRedis版本(**可选**，不升级也可进行DR切换，升级后可以减少DR切换时间)

  - .net客户端 >= 1.2.5.0
  - java客户端 >= 2.5.2

- 测试环境升级Redis-Server版本【自助操作，有问题请联系测试环境Redis负责人： [Brad Lee （李剑）](http://conf.ctripcorp.com/display/~jian.li) [Kun Chen （陈昆）](http://conf.ctripcorp.com/display/~chenkun) 】
  测试环境需要升级Redis版本至XRedis最新版本，验证业务功能在此版本上功能正常。

  以下是升级流程链接

  http://qate.qa.nt.ctripcorp.com/#/create_redis
  如下，请选择“清空credis原配置”
  ![](/Users/fints/p/java/x-pipe/doc/gitbook/reference/1.2_how_to_use/1.png)

- 生产环境接入（不影响业务）
  生产环境在异地机房建立Redis实时复制关系
  生产环境Redis负责人：江浩([jiang_h@ctrip.com](mailto:jiang_h@ctrip.com)) 寿向晨 [Sunny Shou （寿向晨）](http://conf.ctripcorp.com/display/~xcshou)

- 生产环境演练**(可选)**
  模拟机房挂掉，DR切换的场景
  演练请联系DBA：江浩([jiang_h@ctrip.com](mailto:jiang_h@ctrip.com)), 寿向晨 [Sunny Shou （寿向晨）](http://conf.ctripcorp.com/display/~xcshou)

## 接入接口

**邮件模板：**

收件人: redisdba@ctrip.com, [Hao Jiang （江浩）（FU-IT）（网站运营中心）](http://conf.ctripcorp.com/display/~jiang_h)

抄送: xpipe@ctrip.com

接入XPipe的Redis集群信息见附件
@江浩 请在生产环境接入XPipe，支持DR（或者其它业需求）

[XPipe 接入请求.oft](http://conf.ctripcorp.com/download/attachments/133504599/XPipe 接入请求.oft?version=2&modificationDate=1590569931000&api=v2)



**附件内容：**

| BU/SBU | Redis 实例名            | UAT环境 | 生产环境 | 应用           | 影响范围             |
| ------ | ----------------------- | ------- | -------- | -------------- | -------------------- |
| 火车票 | train_data_phenix_redis | √       | √        | 火车票数据系统 | 携程火车车次数据查询 |

## 性能稳定性测试报告

## [机票部门测试报告](http://conf.ctripcorp.com/pages/viewpage.action?pageId=133500894)

测试叙要：

- 写入主机房，sleep特定时间，从从机房读取数据
- 2ms成功率99.9%
- 1s内消息读取成功率100%
- DR切换时间5秒左右

## [XPipe性能稳定性测试报告](http://conf.ctripcorp.com/pages/viewpage.action?pageId=113945795)

测试叙要：

- 生产环境24小时，10000QPS，每条消息100字节，系统表现稳定