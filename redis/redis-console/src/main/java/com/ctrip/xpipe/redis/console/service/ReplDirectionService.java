package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ReplDirectionInfoModel;
import com.ctrip.xpipe.redis.console.model.ReplDirectionTbl;

import java.util.List;

public interface ReplDirectionService {

    ReplDirectionTbl findReplDirectionTblById(long id);

    List<ReplDirectionTbl> findAllReplDirection();

    List<ReplDirectionTbl> findAllReplDirectionTblsByCluster(long clusterId);

    ReplDirectionInfoModel findReplDirectionInfoModelByClusterAndSrcToDc(String clusterName,
                                                                         String srcDcName, String toDcName);

    ReplDirectionTbl findByClusterAndSrcToDc(String clusterName,
                                             String srcDcName, String toDcName);

    List<ReplDirectionInfoModel> findReplDirectionInfoModelsByClusterAndToDc(String cluterName, String toDcName);

    ReplDirectionInfoModel findReplDirectionInfoModelById(long id);

    ReplDirectionTbl addReplDirectionByInfoModel(String clusterName, ReplDirectionInfoModel replDirectionInfoModel);

    List<ReplDirectionInfoModel> findAllReplDirectionInfoModelsByCluster(String clusterName);

    void updateClusterReplDirections(ClusterTbl clusterTbl, List<ReplDirectionInfoModel> replDirections);

    void updateReplDirectionBatch(List<ReplDirectionTbl> replDirections);

    void deleteReplDirectionBatch(List<ReplDirectionTbl> replDirectionTbls);
}