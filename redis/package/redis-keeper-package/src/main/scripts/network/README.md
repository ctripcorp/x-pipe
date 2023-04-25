# 模拟机房间断网脚本
### 环境：
本机需安装sshpass

### 步骤：
1. 机器xpipe账号准备
2. 数据准备 ./data
3. 添加断网定时任务 ./start.sh $dc $hour $minute
4. 断网恢复 ./stop.sh $hour $minute

### 具体解释：
1.数据准备

- 将ip填入 ./data 的文件中
- iplist_ptjq 包含ptjq的xpipe console, xpipe metaserver, keeper, redis&sentinel, checker, beacon
- consoleiplist_ptjq 包含ptjq的console ip
- a10ip_ptjq 为ptjq group的a10 (ptjq的xpipe console, xpipe metaserver, credis console为同一个group)
- ptoy的这些iplist含义与ptjq的相同，只是所属机房不同
  - iplist_ptoy
  - consoleiplist_ptoy
  - a10ip_ptoy
- iplist_ptfq 包含ptfq的beacon，mysql，credis client的ip
- 假设ptjq是主机房, ptoy是备机房。断网时, ptjq的所有ip会使用iptables drop掉ptoy和ptfq的所有ip，ptjq的console会多drop掉a10ptoy的ip; ptoy的console会drop掉a10ptjq的ip

2.执行断网

- ./start.sh $dc $hour $minute $beacon $upload_sh
- 参数含义：
  - $dc: ptjq或ptoy，要隔离的主机房
  - $hour: 断网的小时
  - $minute: 断网的分钟
  - $beacon: 值为beacon时，表示只断beacon，空或其他值表示断整个机房
  - $upload_sh: 表示是否需要上传脚本到机器，非必填，默认值会上传脚本到机器。第一次执行或脚本有变化时必须上传，耗时较长；n 表示不需上传脚本
        断网触发时间为指定时间的01s~02s20ms之间

3.断网恢复

- ./stop.sh $hour $minute
  - iptables恢复

4.查看断网实际触发时间差

- ./analyze_isolate_time.sh $recovery
  - $recovery值为recovery时，表示查看断网恢复时间差; 为空或其他值时，表示查看断网时间差

5.常见问题排查

- 一直等待loading的状态：
  - ps -ef | grep sshpass 
  - 可查看当前未完成的进程所对应的机器ip
  - 通常因为机器压力过大，ssh连接较慢，等待即可；或因为机器挂掉，需要手动kill掉sshpass对应的线程
- retrylist始终不为空，导致进程无法结束
  - sshpass执行失败，且retry无法成功，具体失败原因会打印到屏幕上
  - 通常账号未申请会一直失败

6.执行失败的恢复流程

- 终止脚本
- kill掉所有的sshpass进程
- 执行./stop.sh重置所有机器的状态
- 重新执行断网脚本
