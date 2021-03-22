package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

import java.util.Date;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/9
 */
public interface Persistence {

    boolean isClusterOnMigration(String clusterId);

    void updateRedisRole(RedisHealthCheckInstance instance, Server.SERVER_ROLE role);

    Set<String> sentinelCheckWhiteList();

    boolean isSentinelAutoProcess();

    boolean isAlertSystemOn();

    Date getClusterCreateTime(String clusterId);

    Map<String, Date> loadAllClusterCreateTime();

    void recordAlert(AlertMessageEntity message, EmailResponse response);

}
