package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzCreateInfo;
import com.ctrip.xpipe.redis.console.model.AzTbl;

import java.util.List;

/**
 * @author:
 * @date:
 */
public interface AzService {
    void addAvailableZone(AzCreateInfo createInfo);

    void updateAvailableZone(AzCreateInfo createInfo);

    List<AzCreateInfo> getDcAvailableZones(String dcName);

    List<AzCreateInfo> getAllAvailableZones();

    void deleteAvailableZoneByName(String azName, String dcName);

    AzTbl getAzinfoByid(long id);

}