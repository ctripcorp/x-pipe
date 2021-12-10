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

    List<AzTbl> getDcActiveAvailableZoneTbls(String dcName);

    List<AzTbl> getDcAvailableZoneTbls(String dcName);

    List<AzCreateInfo> getDcAvailableZoneInfos(String dcName);

    List<AzCreateInfo> getAllAvailableZoneInfos();

    AzTbl getAvailableZoneTblByAzName(String azName);

    AzTbl getAvailableZoneTblById(Long azId);

    void deleteAvailableZoneByName(String azName);

}