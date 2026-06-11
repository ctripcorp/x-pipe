package com.ctrip.xpipe.redis.console.cache.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.cache.AzCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.AzTbl;
import com.ctrip.xpipe.redis.console.service.AzService;
import com.ctrip.xpipe.utils.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yihaohuang
 */
@Service
public class AzCacheImpl implements AzCache {

    private static final Logger logger = LoggerFactory.getLogger(AzCacheImpl.class);

    private AzService azService;

    private ConsoleConfig config;

    private Map<String, TimeBoundCache<AzTbl>> azNameToTbl;

    private Map<Long, TimeBoundCache<AzTbl>> azIdToTbl;

    @Autowired
    public AzCacheImpl(AzService azService, ConsoleConfig config) {
        this.azService = azService;
        this.config = config;
        this.azNameToTbl = new HashMap<>();
        this.azIdToTbl = new HashMap<>();
    }

    @Override
    public AzTbl find(String azName) {
        return MapUtils.getOrCreate(azNameToTbl, azName,
                () -> new TimeBoundCache<>(config::getCacheRefreshInterval, () -> azService.getAvailableZoneTblByAzName(azName)))
                .getData(false);
    }

    @Override
    public AzTbl find(long azId) {
        return MapUtils.getOrCreate(azIdToTbl, azId,
                () -> new TimeBoundCache<>(config::getCacheRefreshInterval, () -> azService.getAvailableZoneTblById(azId)))
                .getData(false);
    }

    @Override
    public Long findId(String azName) {
        if (azName == null) return null;
        AzTbl azTbl = find(azName);
        if (azTbl == null) {
            logger.warn("[findId] azName not found: {}", azName);
            return null;
        }
        return azTbl.getId();
    }

}
