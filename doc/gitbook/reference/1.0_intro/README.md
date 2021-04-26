XPipe主要解决Redis DR以及多机房访问问题

具体可参见《[XPipe使用文档](http://conf.ctripcorp.com/pages/viewpage.action?pageId=113945769#XPipe使用文档-系统简介)》章节：系统简介

## 关联方

- XPipe (负责人: Slight Wu （吴骋成）Wenchao Meng （孟文超））
  - 负责Redis服务器管理，DR切换等
- CRedis (负责人: Slight Wu（吴骋成））
  - 负责客户端路由
- DBA （邮件组: redisdba@ctrip.com ）
  - 负责具体的集群接入和DR切换操作

## DR切换执行方

- 目前DR切换由DBA统一操作
- 用户有切换需求也可直接联系DBA redisdba@ctrip.com

## 目前的DR切换为什么手工进行？

DR切换是机房级别的故障处理方案，自动切换的难点在于判定机房是否挂：

1. 观测点放在机房内部，自身挂了无法判断
2. 观测点放在机房外部，很可能是由于机房之间网络不通
3. 目前携程主要有金桥、欧阳两个机房，无法多数点决策