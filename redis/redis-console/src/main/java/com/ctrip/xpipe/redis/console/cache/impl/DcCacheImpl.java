package com.ctrip.xpipe.redis.console.cache.impl;

import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/4/17
 */
@Service
public class DcCacheImpl implements DcCache {

    private DcService dcService;

    private ConsoleConfig config;

    private Map<Long, TimeBoundCache<DcTbl>> dcIdToTbl;

    private Map<String, TimeBoundCache<DcTbl>> dcNameToTbl;

    @Autowired
    public DcCacheImpl(DcService dcService, ConsoleConfig config) {
        this.dcService = dcService;
        this.config = config;
        this.dcIdToTbl = new HashMap<>();
        this.dcNameToTbl = new HashMap<>();
    }

    @Override
    public DcTbl find(String dcName) {
        return MapUtils.getOrCreate(dcNameToTbl, dcName.toUpperCase(),
                () -> new TimeBoundCache<>(config::getCacheRefreshInterval, () -> dcService.find(dcName)))
                .getData(false);
    }

    @Override
    public DcTbl find(long dcId) {
        return MapUtils.getOrCreate(dcIdToTbl, dcId,
                () -> new TimeBoundCache<>(config::getCacheRefreshInterval, () -> dcService.find(dcId)))
                .getData(false);
    }

}
