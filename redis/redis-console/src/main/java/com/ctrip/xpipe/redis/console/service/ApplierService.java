package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ApplierTbl;

import java.util.List;

public interface ApplierService {

    ApplierTbl find(long id);

    ApplierTbl findByIpPort(String ip, int port);

    List<ApplierTbl> findByShardAndReplDirection(long shardId, long replDirectionId);

    List<ApplierTbl> findAllAppliersWithSameIp(String ip);

    List<ApplierTbl> findAllAppliercontainerCountInfo();
}
