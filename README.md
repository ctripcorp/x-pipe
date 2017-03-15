x-pipe
================
### [master]
[![Build Status](https://travis-ci.org/ctripcorp/x-pipe.svg?branch=master)](https://travis-ci.org/ctripcorp/x-pipe)
[![Coverage Status](https://coveralls.io/repos/github/ctripcorp/x-pipe/badge.svg?branch=master)](https://coveralls.io/github/ctripcorp/x-pipe?branch=master)
[![Coverity Scan Build Status](https://scan.coverity.com/projects/8884/badge.svg)](https://scan.coverity.com/projects/ctripcorp-x-pipe)

### [dev]
[![Build Status](https://travis-ci.org/ctripcorp/x-pipe.svg?branch=dev)](https://travis-ci.org/ctripcorp/x-pipe)
[![Coverage Status](https://coveralls.io/repos/github/ctripcorp/x-pipe/badge.svg?branch=dev)](https://coveralls.io/github/ctripcorp/x-pipe?branch=dev)

Cross Data Center Pipeline
# xpipe解决什么问题
redis在携程内部得到广泛的使用，主要作为cache缓存数据，但是也有部分业务直接将其作为内存数据库使用。作为内存数据库，就对系统的可用性提出了极高的挑战，XPipe主要解决redis多数据中心遇到的问题。
## redis数据复制问题
1. replication log大小有限  
redis使用内存ring buffer存储数据日志，当数据大于特定大小时，将覆盖原先的日志。由于内存有限，复制日志大小必然受到限制。在多机房的情况下，容易导致复制失败，引发全量同步。
2. 无全局offset(新版4.0解决了此问题)  
redis4之前的版本，每个redis之间的offset彼此独立，导致当redis进行fail over或者切换时，必须全量同步。  
截止到目前，4.0版本release版本还未发布。

## 机房一键切换
# 系统特性
## 低延时
## 高可用
