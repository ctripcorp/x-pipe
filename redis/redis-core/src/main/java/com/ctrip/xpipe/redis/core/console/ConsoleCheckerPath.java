package com.ctrip.xpipe.redis.core.console;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public class ConsoleCheckerPath {

    private ConsoleCheckerPath() {}

    public static final String PATH_BIND_SHARD_SENTINEL = "/api/bind/shard/sentinels";
    
    public static final String PATH_GET_ALL_META = "/api/meta/divide";

    public static final String PATH_GET_ALL_META_LONG_PULL = "/api/meta/long_pull";

    public static final String PATH_GET_DC_ALL_META = "/api/meta/{dcName}/all";

    public static final String PATH_GET_META = "/api/meta/divide/{index}";

    public static final String PATH_GET_PROXY_CHAINS = "/api/proxy/chains";

    public static final String PATH_PUT_CHECKER_STATUS = "/api/health/checker/status";

    public static final String PATH_PUT_HEALTH_CHECK_RESULT = "/api/health/check/result";

    public static final String PATH_POST_KEEPER_CONTAINER_INFO_RESULT = "/api/keeperContainer/info/result/{index}";
    
    public static final String PATH_PERSISTENCE = "/api/persistence/";

    public static final String PATH_GET_IS_CLUSTER_ON_MIGRATION = PATH_PERSISTENCE + "isClusterOnMigration/{clusterName}";
    
    public static final String PATH_PUT_UPDATE_REDIS_ROLE = PATH_PERSISTENCE + "updateRedisRole/{role}";
    
    public static final String PATH_GET_SENTINEL_CHECKER_WHITE_LIST = PATH_PERSISTENCE + "sentinelCheckerWhiteList";
    
    public static final String PATH_GET_CLUSTER_ALERT_WHITE_LIST = PATH_PERSISTENCE + "clusterAlertWhiteList";

    public static final String PATH_GET_MIGRATING_CLUSTER_LIST = PATH_PERSISTENCE + "migratingClusterList";

    public static final String PATH_GET_IS_SENTINEL_AUTO_PROCESS = PATH_PERSISTENCE + "isSentinelAutoProcess";

    public static final String PATH_GET_IS_ALERT_SYSTEM_ON = PATH_PERSISTENCE + "isAlertSystemOn";

    public static final String PATH_GET_CLUSTER_CREATE_TIME = PATH_PERSISTENCE + "clusterCreateTime/{clusterId}";

    public static final String PATH_GET_LOAD_ALL_CLUSTER_CREATE_TIME = PATH_PERSISTENCE + "loadAllClusterCreateTime";

    public static final String PATH_POST_RECORD_ALERT = PATH_PERSISTENCE + "recordAlert";

    public static final String PATH_GET_ALL_CURRENT_DC_ACTIVE_DC_ONE_WAY_CLUSTERS = "/api/outclient/clusters/one_way";

    public static final String PATH_GET_ALL_CURRENT_DC_ONE_WAY_CLUSTERS = "/api/outclient/current/dc/clusters/one_way";

    public static final String PATH_PUT_CHECKER_LEADER_MERGE_ALERT = "/api/mail/{alertType}";
    
}
