package com.ctrip.xpipe.redis.console.cache;

import com.ctrip.xpipe.redis.console.model.ZoneTbl;

/**
 * Region (= ZONE_TBL) lookup and DC → Region mapping.
 */
public interface RegionCache {

    /**
     * Region name of the given DC. Empty string when DC / zone missing.
     */
    String regionOf(String dcName);

    /**
     * Region name of the given DC id. Empty string when DC / zone missing.
     */
    String regionOf(long dcId);

    ZoneTbl find(long zoneId);

    ZoneTbl find(String zoneName);

}
