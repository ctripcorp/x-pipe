package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ReplDirectionInfoModel;
import com.ctrip.xpipe.redis.console.model.ReplDirectionTbl;

import java.util.List;

public interface ReplDirectionService {
    ReplDirectionTbl find(long id);

    List<ReplDirectionTbl> findAllReplDirectionByCluster(long clusterId);

    List<ReplDirectionInfoModel> findReplDirectionInfoModelByClusterAndToDc(String cluterName, String toDcName);

    void updateReplDirection(ReplDirectionInfoModel model);

}
