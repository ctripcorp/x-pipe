package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.email.EmailResponse;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.alert.AlertMessageEntity;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import org.springframework.web.client.RestClientException;

import java.util.*;
import java.util.stream.Collectors;

public class CheckerPersistenceCache extends AbstractPersistenceCache {
    CheckerConsoleService service;
    public CheckerPersistenceCache(CheckerConfig config, CheckerConsoleService service) {
        super(config);
        this.service = service;
    }

    @Override
    public void updateRedisRole(RedisHealthCheckInstance instance, Server.SERVER_ROLE role) {
        service.updateRedisRole(config.getConsoleAddress(), instance, role);
    }

    @Override
    public void recordAlert(String eventOperator, AlertMessageEntity message, EmailResponse response) {
        service.recordAlert(config.getConsoleAddress(), eventOperator, message, response);
    }

    @Override
    public Set<String> doSentinelCheckWhiteList() {
        try {
            Set<String> originWhitelist = service.sentinelCheckWhiteList(config.getConsoleAddress());
            return originWhitelist.stream().map(String::toLowerCase).collect(Collectors.toSet());
        } catch (RestClientException e) {
            logger.warn("[doSentinelCheckWhiteList] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doSentinelCheckWhiteList] fail", th);
        }

        return Collections.emptySet();
    }

    @Override
    public Set<String> doClusterAlertWhiteList() {
        try {
            Set<String> originWhitelist = service.clusterAlertWhiteList(config.getConsoleAddress());
            return originWhitelist.stream().map(String::toLowerCase).collect(Collectors.toSet());
        } catch (RestClientException e) {
            logger.warn("[doClusterAlertWhiteList] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doClusterAlertWhiteList] fail", th);
        }

        return Collections.emptySet();
    }

    @Override
    Set<String> doGetMigratingClusterList() {
        try {
            return service.migratingClusterList(config.getConsoleAddress());
        } catch (RestClientException e) {
            logger.warn("[doGetMigratingClusterList] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doGetMigratingClusterList] fail", th);
        }

        return Collections.emptySet();
    }

    @Override
    public boolean doIsSentinelAutoProcess() {
        try {
            return service.isSentinelAutoProcess(config.getConsoleAddress());
        } catch (RestClientException e) {
            logger.warn("[doIsSentinelAutoProcess] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doIsSentinelAutoProcess] fail", th);
        }

        return true;
    }

    @Override
    public boolean doIsAlertSystemOn() {
        try {
            return service.isAlertSystemOn(config.getConsoleAddress());
        } catch (RestClientException e) {
            logger.warn("[doIsAlertSystemOn] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doIsAlertSystemOn] fail", th);
        }

        return true;
    }

    @Override
    boolean doIsKeeperBalanceInfoCollectOn() {
        try {
            return service.isKeeperBalanceInfoCollectOn(config.getConsoleAddress());
        } catch (RestClientException e) {
            logger.warn("[doIsKeeperBalanceInfoCollectOn] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doIsKeeperBalanceInfoCollectOn] fail", th);
        }

        return true;
    }

    @Override
    public Map<String, Date> doLoadAllClusterCreateTime() {
        try {
            return service.loadAllClusterCreateTime(config.getConsoleAddress());
        } catch (RestClientException e) {
            logger.warn("[doLoadAllClusterCreateTime] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doLoadAllClusterCreateTime] fail", th);
        }

        return Collections.emptyMap();
    }
}
