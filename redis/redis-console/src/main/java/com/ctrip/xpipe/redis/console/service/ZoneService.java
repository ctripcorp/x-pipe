package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ZoneTbl;

import java.util.List;
import java.util.Map;

/**
 * @author taotaotu
 * May 23, 2019
 */
public interface ZoneService {

    ZoneTbl findById(long id);

    List<ZoneTbl> findAllZones();

    Map<Long, String> zoneNameMap();

    void insertRecord(String zoneName);

}
