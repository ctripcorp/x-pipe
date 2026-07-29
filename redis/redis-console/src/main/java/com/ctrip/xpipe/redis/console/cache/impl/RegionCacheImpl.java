package com.ctrip.xpipe.redis.console.cache.impl;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.cache.DcCache;
import com.ctrip.xpipe.redis.console.cache.RegionCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.ZoneTbl;
import com.ctrip.xpipe.redis.console.service.ZoneService;
import com.ctrip.xpipe.utils.MapUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Time-bound Region / Zone cache, patterned after {@link DcCacheImpl}.
 */
@Service
public class RegionCacheImpl implements RegionCache {

    private final DcCache dcCache;

    private final ZoneService zoneService;

    private final ConsoleConfig config;

    private final Map<Long, TimeBoundCache<ZoneTbl>> zoneIdToTbl;

    private final Map<String, TimeBoundCache<ZoneTbl>> zoneNameToTbl;

    @Autowired
    public RegionCacheImpl(DcCache dcCache, ZoneService zoneService, ConsoleConfig config) {
        this.dcCache = dcCache;
        this.zoneService = zoneService;
        this.config = config;
        this.zoneIdToTbl = new HashMap<>();
        this.zoneNameToTbl = new HashMap<>();
    }

    @Override
    public String regionOf(String dcName) {
        if (StringUtils.isEmpty(dcName)) {
            return "";
        }
        DcTbl dc = dcCache.find(dcName);
        if (dc == null) {
            return "";
        }
        return regionNameOfZoneId(dc.getZoneId());
    }

    @Override
    public String regionOf(long dcId) {
        if (dcId <= 0) {
            return "";
        }
        DcTbl dc = dcCache.find(dcId);
        if (dc == null) {
            return "";
        }
        return regionNameOfZoneId(dc.getZoneId());
    }

    @Override
    public ZoneTbl find(long zoneId) {
        return MapUtils.getOrCreate(zoneIdToTbl, zoneId,
                () -> new TimeBoundCache<>(config::getCacheRefreshInterval, () -> zoneService.findById(zoneId)))
                .getData(false);
    }

    @Override
    public ZoneTbl find(String zoneName) {
        if (StringUtils.isEmpty(zoneName)) {
            return null;
        }
        return MapUtils.getOrCreate(zoneNameToTbl, zoneName.toUpperCase(),
                () -> new TimeBoundCache<>(config::getCacheRefreshInterval, () -> findZoneByName(zoneName)))
                .getData(false);
    }

    private String regionNameOfZoneId(long zoneId) {
        ZoneTbl zone = find(zoneId);
        if (zone == null || zone.getZoneName() == null) {
            return "";
        }
        return zone.getZoneName();
    }

    private ZoneTbl findZoneByName(String zoneName) {
        List<ZoneTbl> zones = zoneService.findAllZones();
        if (zones == null) {
            return null;
        }
        for (ZoneTbl zone : zones) {
            if (zoneName.equalsIgnoreCase(zone.getZoneName())) {
                return zone;
            }
        }
        return null;
    }

}
