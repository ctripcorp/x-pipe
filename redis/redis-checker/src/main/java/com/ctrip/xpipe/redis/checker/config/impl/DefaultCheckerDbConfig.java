package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * @author lishanglin
 * date 2021/3/13
 */
public class DefaultCheckerDbConfig implements CheckerDbConfig {

    private static final String KEY_CACHE_EXPIRED = "checker.cache.expired";

    private static final String DEFAULT_CACHE_EXPIRED = "10000";

    private Persistence persistence;

    private TimeBoundCache<Set<String>> sentinelCheckWhiteListCache;

    private TimeBoundCache<Boolean> sentinelAutoProcessCache;

    private TimeBoundCache<Boolean> alertSystemOn;

    public DefaultCheckerDbConfig(Persistence persistence, long cacheExpired) {
        this.persistence = persistence;

        sentinelCheckWhiteListCache = new TimeBoundCache<>(cacheExpired, () ->
                this.persistence.sentinelCheckWhiteList().stream().map(String::toLowerCase).collect(Collectors.toSet()));
        alertSystemOn = new TimeBoundCache<>(cacheExpired, this.persistence::isAlertSystemOn);
        sentinelAutoProcessCache = new TimeBoundCache<>(cacheExpired, this.persistence::isSentinelAutoProcess);
    }

    @Autowired
    public DefaultCheckerDbConfig(Persistence persistence) {
        this(persistence, Integer.parseInt(System.getProperty(KEY_CACHE_EXPIRED, DEFAULT_CACHE_EXPIRED)));
    }

    @Override
    public boolean isAlertSystemOn() {
        return alertSystemOn.getData(false);
    }

    @Override
    public boolean isSentinelAutoProcess() {
        return sentinelAutoProcessCache.getData(false);
    }

    @Override
    public boolean shouldSentinelCheck(String cluster) {
        if (StringUtil.isEmpty(cluster)) return false;

        Set<String> whiteList = sentinelCheckWhiteList();
        return null != whiteList && !whiteList.contains(cluster.toLowerCase());
    }

    @Override
    public Set<String> sentinelCheckWhiteList() {
        return sentinelCheckWhiteListCache.getData(false);
    }

    private static class TimeBoundCache<T> {

        private T data;

        private long expiredAt;

        private long timeoutMill;

        private Supplier<T> dataSupplier;

        public TimeBoundCache(long timeoutMill, Supplier<T> dataSupplier) {
            this.data = null;
            this.expiredAt = 0L;
            this.timeoutMill = timeoutMill;
            this.dataSupplier = dataSupplier;
        }

        public T getData(boolean disableCache) {
            if (!disableCache && null != data && expiredAt > System.currentTimeMillis()) {
                return data;
            }

            this.data = dataSupplier.get();
            this.expiredAt = System.currentTimeMillis() + timeoutMill;
            return this.data;
        }

    }

}
