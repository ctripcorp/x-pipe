# KeeperFix

## 概述

KeeperFix是一个通过替换keeper一键修复XPipe问题集群的脚本

使用前需要替换脚本里的Cookie、XPIPE_HOST，并安装有python2.7

## 功能&使用

**1、初始化**

命令行输入：python keeperFix.py

输出：

```bash
unhealthy clusters: 2
unhealthy shards: 7
attach fail dcs: ["SHAJQ", "SHAOY", "SFO-AWS", "YMQ-AWS", "CFTJQ"]
choose action:
    [1] refresh unhealthy info
    [2] show all unhealthy clusters
    [3] show cluster unhealthy shards
    [11] fix single cluster
    [12] fix n shards
    [13] fix all shards
    [21] config strict replace
    [22] config keeper port init
    [0] exit
```

内部机制：

1. 从xpipe拉取问题分片信息缓存到内存
2. 输出功能菜单，用户可输入功能选项，直到输入0退出程序

**2、refresh unhealthy info**

命令行输入：1

输出：

```bash
unhealthy clusters: 1
unhealthy shards: 6
attach fail dcs: ["SHAJQ", "SHAOY", "SFO-AWS", "YMQ-AWS", "CFTJQ"]
```

内部机制：

重新刷新问题分片信息，否则内存中的问题分片信息不回改变，即后续功能都是基于这份问题分片信息

**3、show all unhealthy clusters | show cluster unhealthy shards**

**show all unhealthy clusters**

命令行输入：2

输出（问题集群集群名）

```bash
test_one_way_migrate_v202207041902
```

**show cluster unhealthy shards**

命令行输入：3

输出：input cluster name

命令行输入：test_one_way_migrate_v202207041902

输出（输入集群的问题分片名）

```bash
SHAXY test_one_way_migrate_6
SHAXY test_one_way_migrate_5
SHAXY test_one_way_migrate_4
SHAXY test_one_way_migrate_3
SHAXY test_one_way_migrate_2
SHAXY test_one_way_migrate_1
```

**4、fix single cluster | fix n shards | fix all shards**

**fix single cluster**

命令行输入：11

输出：input cluster name

命令行输入：test_one_way_migrate_v202207041902

输出：fix cluster test_one_way_migrate_v202207041902

内部机制：

1. 查找集群下的问题分片信息，没有则退出
2. 查找集群信息，若没有或集群类型不是单向同步汲取则退出
3. 检查主机房是否也存在问题实例，跳过主机房异常的分片
4. 根据配置选择是严格替换还是不严格替换，替换集群主机房和所有问题机房的keeper

严格替换 | 非严格替换见下文

**fix n shards** 

命令行输入：12

输出：input fix limit

输入：10

内部机制：对问题集群挨个修复，修复过程同 fix single cluster，会限制总共修复的分片数量，数量到了自动停止

**fix all shards**

命令行输入：13

内部机制：对问题集群挨个修复，修复过程同 fix single cluster，直到全部分片修复完成为止

**注意：已经修复过的集群分片不会修改内存中的问题分片信息，如果重复调用修复功能，会重复修复同一集群**

**5、config strict replace | config keeper port init**

**config strict replace**

命令行输入：21

输出：1 - use strict replace; 0 - use normal strict

命令行输入：1

**严格替换模式（默认）**

从可用keeper中选出两个不同的keeper以做替换

1. 先从xpipe选中的keeper中选（最佳keeper，xpipe分配port），排除重复keepercontainer
2. 若选中keeper少于2个，再从全部可选keeper中选，直到选够两个不重复keeper，这一步选中的keeper脚本自动分配port（从5000开始可使用config keeper port init做调整）
3. 用选中keeper替换原keeper

**非严格替换模式**

无脑先调用删除keeper，再调用添加keeper，具体使用哪个keeper由xpipe自动选择，可能出现替换的keeper和原keeper相同的场景，导致替换无用

**config keeper port init**

命令行输入：22

输出：input init port

输入：5000

替换脚本分配port的起始端口，见"严格替换模式"