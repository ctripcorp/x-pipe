package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ApplierTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;

import java.util.List;
import java.util.function.BiPredicate;

public interface ApplierService {

    ApplierTbl findApplierTblById(long id);

    ApplierTbl findApplierTblByIpPort(String ip, int port);

    List<ApplierTbl> findApplierTblByShardAndReplDirection(long shardId, long replDirectionId);

    List<ApplierTbl> findAllApplierTblsWithSameIp(String ip);

    void updateAppliers(String dcName, String clusterName, String shardName, ShardModel sourceShard, long replDirectionId);

    List<ApplierTbl> findAllAppliercontainerCountInfo();


    List<ApplierBasicInfo> findBestAppliers(String dcName, int beginPort,
                                            BiPredicate<String, Integer> applierGood, String clusterName);
}
