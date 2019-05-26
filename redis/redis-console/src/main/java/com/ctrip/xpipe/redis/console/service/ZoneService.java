package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ZoneTbl;

import java.util.List;

/**
 * @author taotaotu
 * May 23, 2019
 */
public interface ZoneService {

    ZoneTbl findById(long id);

    List<ZoneTbl> findAllZones();

    void insertRecord(String zoneName);

}
