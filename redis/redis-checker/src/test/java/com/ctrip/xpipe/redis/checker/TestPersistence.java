package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/14
 */
public class TestPersistence implements Persistence {

    private Set<String> sentinelCheckWhiteList = new HashSet<>();

    private boolean sentinelAutoProcess = true;

    private boolean alertSystemOn = true;

    public void setSentinelAutoProcess(boolean val) {
        this.sentinelAutoProcess = val;
    }

    public void setAlertSystemOn(boolean val) {
        this.alertSystemOn = val;
    }

    @Override
    public boolean isClusterOnMigration(String clusterId) {
        return false;
    }

    @Override
    public void updateRedisRole(RedisHealthCheckInstance instance, Server.SERVER_ROLE role) {

    }

    @Override
    public Set<String> sentinelCheckWhiteList() {
        return sentinelCheckWhiteList;
    }

    @Override
    public boolean isSentinelAutoProcess() {
        return sentinelAutoProcess;
    }

    @Override
    public boolean isAlertSystemOn() {
        return alertSystemOn;
    }

    @Override
    public Date getClusterCreateTime(String clusterId) {
        return null;
    }

    @Override
    public Map<String, Date> loadAllClusterCreateTime() {
        return null;
    }

    @Override
    public void recordAlert(AlertMessageEntity message, EmailResponse response) {

    }
}
