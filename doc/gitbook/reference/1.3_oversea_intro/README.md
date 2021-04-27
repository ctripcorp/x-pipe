## 原理

参考：https://mp.weixin.qq.com/s/LeSSdT6bOEFzZyN26PRVzg

## 数据同步延迟监控数据

### Dashboard

- Metric名字： fx.xpipe.delay
  - Tag
    - cluster           集群名
    - shard             CRedis集群分片名
    - ip                    Redis的IP地址
    - port                Redis 端口
    - dc                   Redis所在机房
    - console-dc    监控节点所在机房
- 例如：
  - http://dashboard.fx.ctripcorp.com/#report/4416795

## 业务同事接入注意事项

- 上海->海外单向数据同步，海外机房Redis可读，**不可写**
- 异地机房数据同步, 须接受极端情况下**20分钟**的数据延迟 
- 集群中单个group的平均**写入**流量不能超过5MB/S
- 支持CRedis Java客户端
  - 版本>=3.1.0 (推荐使用 4.3.6 以上版本)

### 详细信息

#### 数据延迟

**由于数据同步走的是公网 TCP**, 根据框架一年多的线上经验, 极端情况下, 20 分钟的数据延迟

详细打点信息可见 http://hickwall.ctripcorp.com/grafanav2/d/VuuzwxvZk/xpipe?orgId=6

| IDC  | AVG   | 99 线     | 999 线    |
| :--- | :---- | :-------- | :-------- |
| FRA  | 250ms | 500~600ms | 600~800ms |
| SIN  | 90ms  | 170~230ms | 230~280ms |
| YMQ  | 250ms | 320~380ms | 370~500ms |

## 接入流程

### 邮件到:

收件人: [redisdba@ctrip.com](mailto:redisdba@ctrip.com)

抄送: [xpipe@ctrip.com](mailto:xpipe@ctrip.com)


邮件说清楚需要接入集群，海外同步的数据中心