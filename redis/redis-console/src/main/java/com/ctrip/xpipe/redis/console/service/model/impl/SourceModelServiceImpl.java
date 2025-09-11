package com.ctrip.xpipe.redis.console.service.model.impl;

import com.ctrip.xpipe.redis.console.model.ReplDirectionInfoModel;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.model.SourceModel;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.model.ShardModelService;
import com.ctrip.xpipe.redis.console.service.model.SourceModelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class SourceModelServiceImpl implements SourceModelService {

    @Autowired
    private ShardService shardService;

    @Autowired
    private ReplDirectionService replDirectionService;

    @Autowired
    private ShardModelService shardModelService;

    @Override
    public List<SourceModel> getAllSourceModels(String dcName, String clusterName) {
        List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);
        if (null == shards) return null;

        List<ReplDirectionInfoModel> replDirectionInfoModels =
                replDirectionService.findReplDirectionInfoModelsByClusterAndToDc(clusterName, dcName);
        if (replDirectionInfoModels == null || replDirectionInfoModels.isEmpty()) return null;

        List<SourceModel> sourceModels = new ArrayList<>();

        for (ReplDirectionInfoModel replDirectionInfoModel : replDirectionInfoModels) {
            SourceModel sourceModel = new SourceModel().setReplDirectionInfoModel(replDirectionInfoModel);
            List<ShardModel> shardModels = shardModelService.getMultiShardModel(dcName, clusterName,
                shards, true, replDirectionInfoModel);
            sourceModel.setShards(shardModels);
            sourceModels.add(sourceModel);
        }
        return sourceModels;
    }

    @Override
    public SourceModel getAllSourceModelsByClusterAndReplDirection(String dcName, String clusterName, ReplDirectionInfoModel replDirection) {
        if (replDirection == null) return null;

        List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);
        if (null == shards) return null;

        SourceModel sourceModel = new SourceModel().setReplDirectionInfoModel(replDirection);
        List<ShardModel> shardModels = shardModelService.getMultiShardModel(dcName, clusterName, shards, true, replDirection);
        sourceModel.setShards(shardModels);

        return sourceModel;
    }
}