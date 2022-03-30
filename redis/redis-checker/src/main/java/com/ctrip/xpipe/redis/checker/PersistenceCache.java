package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

import java.util.Date;
import java.util.Map;
import java.util.Set;

public interface PersistenceCache {
    boolean isClusterOnMigration(String clusterId);

    void updateRedisRole(RedisHealthCheckInstance instance, Server.SERVER_ROLE role);

    Set<String> sentinelCheckWhiteList();

    Set<String> clusterAlertWhiteList();

    Set<String> migratingClusterList();

    boolean isSentinelAutoProcess();

    boolean isAlertSystemOn();

    Date getClusterCreateTime(String clusterId);

    Map<String, Date> loadAllClusterCreateTime();

    void recordAlert(String eventOperator, AlertMessageEntity message, EmailResponse response);
}
