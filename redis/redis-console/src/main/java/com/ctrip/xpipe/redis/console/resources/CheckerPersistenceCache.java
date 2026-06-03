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

    private final CachedValue<Set<String>> sentinelCheckWhiteListCache = new CachedValue<>();
    private final CachedValue<Set<String>> clusterAlertWhiteListCache = new CachedValue<>();
    private final CachedValue<Set<String>> migratingClusterListCache = new CachedValue<>();
    private final CachedValue<Boolean> isSentinelAutoProcessCache = new CachedValue<>();
    private final CachedValue<Boolean> isAlertSystemOnCache = new CachedValue<>();
    private final CachedValue<Map<String, Date>> allClusterCreateTimeCache = new CachedValue<>();

    public CheckerPersistenceCache(CheckerConfig config, CheckerConsoleService service) {
        super(config);
        this.service = service;
    }

    @Override
    public void updateRedisRole(RedisHealthCheckInstance instance, Server.SERVER_ROLE role) {
        service.updateRedisRole(getConsoleAddress(), instance, role);
    }

    @Override
    public void recordAlert(String eventOperator, AlertMessageEntity message, EmailResponse response) {
        service.recordAlert(getConsoleAddress(), eventOperator, message, response);
    }

    @Override
    public Set<String> doSentinelCheckWhiteList() {
        try {
            Set<String> originWhitelist = service.sentinelCheckWhiteList(getConsoleAddress());
            Set<String> result = originWhitelist.stream().map(String::toLowerCase).collect(Collectors.toSet());
            sentinelCheckWhiteListCache.update(result);
            return result;
        } catch (RestClientException e) {
            logger.warn("[doSentinelCheckWhiteList] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doSentinelCheckWhiteList] fail", th);
        }

        return sentinelCheckWhiteListCache.getOrElse(Collections.emptySet());
    }

    @Override
    public Set<String> doClusterAlertWhiteList() {
        try {
            Set<String> originWhitelist = service.clusterAlertWhiteList(getConsoleAddress());
            Set<String> result = originWhitelist.stream().map(String::toLowerCase).collect(Collectors.toSet());
            clusterAlertWhiteListCache.update(result);
            return result;
        } catch (RestClientException e) {
            logger.warn("[doClusterAlertWhiteList] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doClusterAlertWhiteList] fail", th);
        }

        return clusterAlertWhiteListCache.getOrElse(Collections.emptySet());
    }

    @Override
    Set<String> doGetMigratingClusterList() {
        try {
            Set<String> result = service.migratingClusterList(getConsoleAddress());
            migratingClusterListCache.update(result);
            return result;
        } catch (RestClientException e) {
            logger.warn("[doGetMigratingClusterList] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doGetMigratingClusterList] fail", th);
        }

        return migratingClusterListCache.getOrElse(Collections.emptySet());
    }

    @Override
    public boolean doIsSentinelAutoProcess() {
        try {
            boolean result = service.isSentinelAutoProcess(getConsoleAddress());
            isSentinelAutoProcessCache.update(result);
            return result;
        } catch (RestClientException e) {
            logger.warn("[doIsSentinelAutoProcess] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doIsSentinelAutoProcess] fail", th);
        }

        return isSentinelAutoProcessCache.getOrElse(true);
    }

    @Override
    public boolean doIsAlertSystemOn() {
        try {
            boolean result = service.isAlertSystemOn(getConsoleAddress());
            isAlertSystemOnCache.update(result);
            return result;
        } catch (RestClientException e) {
            logger.warn("[doIsAlertSystemOn] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doIsAlertSystemOn] fail", th);
        }

        return isAlertSystemOnCache.getOrElse(true);
    }

    @Override
    public Map<String, Date> doLoadAllClusterCreateTime() {
        try {
            Map<String, Date> result = service.loadAllClusterCreateTime(getConsoleAddress());
            allClusterCreateTimeCache.update(result);
            return result;
        } catch (RestClientException e) {
            logger.warn("[doLoadAllClusterCreateTime] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[doLoadAllClusterCreateTime] fail", th);
        }

        return allClusterCreateTimeCache.getOrElse(Collections.emptyMap());
    }

    protected String getConsoleAddress() {
        return config.getConsoleAddress();
    }

    private static class CachedValue<T> {
        private volatile boolean initialized;
        private volatile T value;

        void update(T newValue) {
            this.value = newValue;
            this.initialized = true;
        }

        T getOrElse(T defaultValue) {
            return initialized ? value : defaultValue;
        }
    }
}
