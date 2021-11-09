package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.controller.api.data.meta.AzCreateInfo;
import com.ctrip.xpipe.redis.console.model.AzTbl;

import java.util.List;

/**
 * @author: Song_Yu
 * @date: 2021/11/8
 */
public interface AzService {
    void addAvailableZone(AzCreateInfo createInfo);

    void updateAvailableZone(AzCreateInfo createInfo);

    List<AzTbl> getDcAvailableZonetbl(String dcName);

    List<AzCreateInfo> getDcAvailableZones(String dcName);

    List<AzCreateInfo> getAllAvailableZones();

    void deleteAvailableZoneByName(String azName);

    AzTbl getAzinfoByid(long id);

}