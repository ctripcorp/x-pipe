package com.ctrip.xpipe.redis.checker.alert;

import static com.ctrip.xpipe.redis.checker.alert.policy.receiver.EmailReceiver.EMAIL_DBA;
import static com.ctrip.xpipe.redis.checker.alert.policy.receiver.EmailReceiver.EMAIL_XPIPE_ADMIN;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 16, 2017
 */
public enum ALERT_TYPE {

    CLIENT_INSTANCE_NOT_OK("client_status", EMAIL_DBA | EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public long delayedSendingTime() {
            return 5 * 60 * 1000;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("CRedis中实例故障", "说明：CRedis中实例不可读或不可用");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    QUORUM_DOWN_FAIL("quorum_fail", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public boolean sendToCheckerLeader() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Quorum Down Fail 错误", "说明：Console会从多个点判断一个redis节点是否挂掉，如果没有达到大多数一致(一部分监测点认为Redis节点挂，另一部分认为OK，可能是网络抖动引起)，则报此错误");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    SENTINEL_RESET("stl_rst", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("", "");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    REDIS_CONF_REWRITE_FAILURE("redis_conf_rewrite_failure", EMAIL_DBA|EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Redis CONF REWRITE", "Redis CONF REWRITE不可用将导致 redis master切换/DR切换失败");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    CLIENT_INCONSIS("client_inconsis", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public long delayedSendingTime() {
            return 5 * 60 * 1000;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("CRedis | XPipe 数据不一致", "说明：发现CRedis和XPipe信息不一致");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    MIGRATION_MANY_UNFINISHED("migra_unfinish", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("存在迁移失败Case", "说明：请查看迁移历史");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    XREDIS_VERSION_NOT_VALID("xredis_version_not_valid", EMAIL_DBA) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("从机房Redis版本错误", "说明：XPipe从机房Redis应该为XRedis，且版本号大于等于0.0.3");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.LOW;
        }
    },
    REDIS_CONIFIG_CHECK_FAIL("redis_config_check_fail", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("redis config check fail", "redis has wrong config, please check it");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.LOW;
        }
    },
    REDIS_REPL_DISKLESS_SYNC_ERROR("redis_repl_diskless_sync_error", EMAIL_DBA) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Redis配置错误", "说明：Redis配置项repl-diskless-sync在Redis版本2.8.22以下，应该为NO");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.LOW;
        }
    },
    MARK_INSTANCE_UP("mark instance up", EMAIL_DBA) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public boolean sendToCheckerLeader() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Instance Mark UP", "说明：从机房Redis实例恢复之后，会将Redis拉入集群");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    MARK_INSTANCE_DOWN("mark instance down", EMAIL_DBA) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public boolean sendToCheckerLeader() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Instance Mark Down", "说明：从机房Redis健康检测出问题，会将Redis拉出集群");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    COMPENSATE_MARK_INSTANCE_UP("compensate mark instance up", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Instance Mark Up", "XPipe实例检查结果与CRedis可读状态不一致，补偿机制拉入实例");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    COMPENSATE_MARK_INSTANCE_DOWN("compensate mark instance down", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Instance Mark Down", "XPipe实例检查结果与CRedis可读状态不一致，补偿机制拉出实例");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    CRDT_INSTANCE_UP("crdt instance up", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("crdt instance up", "crdt instance in local dc is healthy");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.MEDIUM;
        }
    },
    CRDT_INSTANCE_DOWN("crdt instance down", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public boolean sendToCheckerLeader() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("crdt instance down", "crdt instance in local dc is unhealthy");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.MEDIUM;
        }
    },
    CRDT_CROSS_DC_REPLICATION_UP("crdt cross dc replication up", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public boolean sendToCheckerLeader() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("crdt cross dc replication up", "crdt replication between cross dc master is healthy");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.MEDIUM;
        }
    },
    CRDT_CROSS_DC_REPLICATION_DOWN("crdt cross dc replication down", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public boolean sendToCheckerLeader() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("crdt cross dc replication down", "crdt replication between cross dc master is unhealthy");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.MEDIUM;
        }
    },
    CRDT_BACKSTREAMING("crdt instance is on back streaming", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("crdt instance is on back streaming", "instance on back streaming can't read/write");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    ALERT_SYSTEM_OFF("alert system is turning off", EMAIL_DBA | EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return true;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("报警系统关闭", "");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    SENTINEL_AUTO_PROCESS_OFF("sentinel auto process is turning off", EMAIL_DBA | EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return true;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("哨兵检测关闭", "");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    KEEPER_BALANCE_INFO_COLLECT_ON("keeper balance info collect is turning on", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return true;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("自动匀keeper信息收集打开", "");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    AUTO_MIGRATION_NOT_ALLOW("auto migration not allow",  EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return true;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("auto migration not allow", "");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    REPL_BACKLOG_NOT_ACTIVE("repl_backlog_not_active", EMAIL_DBA | EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Redis Backlog 为空", "Redis依赖Backlog对即将发出的同步数据进行缓存, 没有将会导致全量同步的风险");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    SENTINEL_MONITOR_REDUNDANT_REDIS("sentinel_monitors_redundant_redis", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Redis哨兵监控到了多余的Redis", "Redis可能不存在于xpipe中");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    SENTINEL_MONITOR_INCONSIS("sentinel_monitor_incosis", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("哨兵监控与配置不一致", "");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    MAJORITY_SENTINELS_NETWORK_ERROR("majority_sentinels_network_error", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("超过一半哨兵失联", "");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    INSTANCE_SICK_BUT_DELAY_MARK_DOWN("instance_lag_delay_mark_down", EMAIL_XPIPE_ADMIN | EMAIL_DBA) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("Redis延迟Mark Down(一般用于跨国站点)", "配置此项的Redis不会立刻拉出, 而是经过一定时间(通常是半小时到1小时)还有问题, 再将其拉出集群");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    MIGRATION_SYSTEM_CHECK_OVER_DUE("migration_system_not_checked", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("DR迁移检测系统停止工作", "针对DR迁移系统的检测工作长时间停止, 则会报出此信息");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    META_CACHE_BLOCKED("MetaCache_Not_Updated", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("MetaCache not working", "Not working for a long time");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    SENTINEL_CONFIG_MISSING("sentinel config missing", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("sentinel config missing",
                    "Sentinels config is not found in db");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    DB_VARIABLES_INVALIDATE("DB variables not as expected", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("DB variables not as expected", "XPipe need some special DB variables");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    TOO_MANY_CLUSTERS_EXCLUDE_FROM_BEACON("beacon meta change too much", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("unregister cluster monitor too many", "too many clusters need to be excluded one round, skip");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    CRDT_CLUSTER_IDC_EXCLUDED("crdt cluster exclude idcs", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public boolean sendToCheckerLeader() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("CRDT cluster idc excluded", "双向同步有拉出机房，应尽快拉入");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    MIGRATION_DATA_CONFLICT("migration info conflict", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("unexpected migration data", "migration info conflict, maybe block migration");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    REPL_WRONG_SLAVE("slave repl from wrong master", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("wrong slave", "slave repl from wrong master");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.MEDIUM;
        }
    },
    MASTER_OVER_ONE("more than one master found", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("master over one", "more than one master found");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.MEDIUM;
        }
    },
    KEEPER_IN_SAME_AVAILABLE_ZONE("keepers in the same available zone found",  EMAIL_DBA|EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return true;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("keepers should in different available zones", "keepers in the same available zone found");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.MEDIUM;
        }
    },
    KEEPER_MIGRATION_SUCCESS("keeper migration success", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("keeper migration success", "keeper migration success");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    },
    KEEPER_MIGRATION_FAIL("keeper migration fail", EMAIL_XPIPE_ADMIN) {
        @Override
        public boolean urgent() {
            return false;
        }

        @Override
        public boolean reportRecovery() {
            return false;
        }

        @Override
        public DetailDesc detailDesc() {
            return new DetailDesc("keeper migration fail", "keeper migration fail");
        }

        @Override
        public ALERT_LEVEL getAlertLevel() {
            return ALERT_LEVEL.HIGH;
        }
    };


    private String simpleDesc;

    private int alertMethod;

    ALERT_TYPE(String simpleDesc, int alertMethod){
        this.simpleDesc = simpleDesc;
        this.alertMethod = alertMethod;
    }

    public String simpleDesc() {
        return simpleDesc;
    }

    public int getAlertMethod() {
        return alertMethod;
    }

    public abstract boolean urgent();

    public abstract boolean reportRecovery();

    public abstract DetailDesc detailDesc();

    public class DetailDesc {

        private String title;

        private String desc;

        public DetailDesc(String title, String desc) {
            this.title = title;
            this.desc = desc;
        }

        public String getTitle() {
            return title;
        }

        public String getDesc() {
            return desc;
        }
    }

    public enum ALERT_LEVEL {
        HIGH,
        MEDIUM,
        LOW
    }

    public boolean onlyTrack() {
        ALERT_LEVEL level = getAlertLevel();
        return level == ALERT_LEVEL.MEDIUM || level == ALERT_LEVEL.LOW;
    }

    public abstract ALERT_LEVEL getAlertLevel();

    public boolean sendToCheckerLeader() {
        return false;
    }

    public long delayedSendingTime() {
        // Maybe it will recover soon. we can wait a little longer
        return 0L;
    }
}
