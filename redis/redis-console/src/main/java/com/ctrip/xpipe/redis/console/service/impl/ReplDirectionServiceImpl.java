package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class ReplDirectionServiceImpl  extends AbstractConsoleService<ReplDirectionTblDao>
        implements ReplDirectionService {

    @Autowired
    ClusterService clusterService;

    @Autowired
    DcService dcService;

    @Autowired
    DcClusterShardService dcClusterShardService;

    @Autowired
    RedisService redisService;

    @Autowired
    ApplierService applierService;

    @Override
    public ReplDirectionTbl findReplDirectionTblById(long id) {
        return queryHandler.handleQuery(new DalQuery<ReplDirectionTbl>() {
            @Override
            public ReplDirectionTbl doQuery() throws DalException {
                return dao.findByPK(id, ReplDirectionTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<ReplDirectionTbl> findAllReplDirection() {
        return queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findAllReplDirection(ReplDirectionTblEntity.READSET_REPL_DIRECTION_CLUSTER_INFO);
            }
        });
    }

    @Override
    public List<ReplDirectionTbl> findAllReplDirectionTblsByCluster(long clusterId) {

        return queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findReplDirectionsByCluster(clusterId, ReplDirectionTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<ReplDirectionInfoModel> findAllReplDirectionInfoModelsByCluster(String clusterName) {

        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            throw new IllegalArgumentException(String.format("cluster %s does not exist", clusterName));
        }
        List<ReplDirectionTbl> replDirectionTbls = findAllReplDirectionTblsByCluster(clusterTbl.getId());

        Map<Long, String> dcNameMap = dcService.dcNameMap();
        List<ReplDirectionInfoModel> result = new ArrayList<>();
        for (ReplDirectionTbl replDirectionTbl : replDirectionTbls) {
            result.add(convertReplDirectionTblToReplDirectionInfoModel(replDirectionTbl, dcNameMap));
        }

        return result;
    }

    @Override
    public List<ReplDirectionInfoModel> findAllReplDirectionInfoModels() {
        List<ReplDirectionTbl> allReplDirectionTbls = findAllReplDirection();
        HashMap<Long, ReplDirectionInfoModel> replDirectionIdInfoMap = new HashMap<>();

        Map<Long, String> dcNameMap = dcService.dcNameMap();

        allReplDirectionTbls.forEach(replDirectionTbl -> {
            ReplDirectionInfoModel replDirectionInfoModel = new ReplDirectionInfoModel();
            replDirectionInfoModel.setId(replDirectionTbl.getId())
                    .setClusterId(replDirectionTbl.getClusterId())
                    .setSrcDcName(dcNameMap.get(replDirectionTbl.getSrcDcId()))
                    .setFromDcName(dcNameMap.get(replDirectionTbl.getSrcDcId()))
                    .setToDcName(dcNameMap.get(replDirectionTbl.getToDcId()))
                    .setTargetClusterName(replDirectionTbl.getTargetClusterName());

            ClusterTbl clusterTbl = clusterService.find(replDirectionTbl.getClusterId());
            if (clusterTbl != null) {
                replDirectionInfoModel.setClusterName(clusterTbl.getClusterName());

                List<DcClusterShardTbl> srcDcClusterShards =
                        dcClusterShardService.findAllByDcCluster(dcNameMap.get(replDirectionTbl.getSrcDcId()), clusterTbl.getClusterName());
                replDirectionInfoModel.setSrcShardCount(srcDcClusterShards == null ? 0: srcDcClusterShards.size());

                List<DcClusterShardTbl> toDcClusterShards =
                        dcClusterShardService.findAllByDcCluster(dcNameMap.get(replDirectionTbl.getToDcId()), clusterTbl.getClusterName());
                replDirectionInfoModel.setToShardCount(toDcClusterShards == null ? 0 : toDcClusterShards.size());

                List<RedisTbl> allKeepers =
                        redisService.findAllKeepersByDcClusterName(dcNameMap.get(replDirectionTbl.getSrcDcId()), clusterTbl.getClusterName());
                replDirectionInfoModel.setKeeperCount(allKeepers == null ? 0 : allKeepers.size());

                List<ApplierTbl> allAppliers =
                        applierService.findAppliersByClusterAndToDc(replDirectionTbl.getToDcId(), clusterTbl.getId());
                replDirectionInfoModel.setApplierCount(allAppliers == null ? 0 : allAppliers.size());
            }
            replDirectionIdInfoMap.put(replDirectionTbl.getId(), replDirectionInfoModel);
        });

        return new ArrayList<>(replDirectionIdInfoMap.values());
    }

    @Override
    public ReplDirectionInfoModel findReplDirectionInfoModelByClusterAndSrcToDc(String clusterName, String srcDcName, String toDcName) {
        ReplDirectionTbl replDirectionTbl = findByClusterAndSrcToDc(clusterName, srcDcName, toDcName);
        if(replDirectionTbl == null) return null;
        return convertReplDirectionTblToReplDirectionInfoModel(replDirectionTbl, dcService.dcNameMap());
    }

    @Override
    public ReplDirectionTbl findByClusterAndSrcToDc(String clusterName, String srcDcName, String toDcName) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            throw new IllegalArgumentException(String.format("cluster %s does not exist", clusterName));
        }

        DcTbl srcDcTbl = dcService.find(srcDcName);
        DcTbl toDcTbl = dcService.find(toDcName);
        if (srcDcTbl == null || toDcTbl == null) {
            throw new IllegalArgumentException(String.format("src dc %s or to dc %s does not exist", srcDcName, toDcName));
        }

        ReplDirectionTbl replDirectionTbl = queryHandler.handleQuery(new DalQuery<ReplDirectionTbl>() {
            @Override
            public ReplDirectionTbl doQuery() throws DalException {
                return dao.findReplDirectionByClusterAndSrcToDc(clusterTbl.getId(), srcDcTbl.getId(), toDcTbl.getId(), ReplDirectionTblEntity.READSET_FULL);
            }
        });
        return replDirectionTbl;
    }

    @Override
    public List<ReplDirectionInfoModel> findReplDirectionInfoModelsByClusterAndToDc(String clusterName, String toDcName) {
        ClusterTbl cluster = clusterService.find(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(String.format("cluster %s does not exist", clusterName));
        }

        DcTbl dc = dcService.find(toDcName);
        if (dc == null) {
            throw new IllegalArgumentException(String.format("dc %s does not exist", toDcName));
        }

        List<ReplDirectionTbl> replDirectionTbls = queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findReplDirectionsByClusterAndToDc(cluster.getId(), dc.getId(), ReplDirectionTblEntity.READSET_FULL);
            }
        });

        Map<Long, String> dcNameMap = dcService.dcNameMap();
        List<ReplDirectionInfoModel> result = new ArrayList<>();
        for (ReplDirectionTbl replDirectionTbl : replDirectionTbls) {
            result.add(convertReplDirectionTblToReplDirectionInfoModel(replDirectionTbl, dcNameMap));
        }

        return result;
    }

    private ReplDirectionInfoModel convertReplDirectionTblToReplDirectionInfoModel(ReplDirectionTbl replDirectionTbl,
                                                                                   Map<Long, String> dcNameMap) {
        ReplDirectionInfoModel replDirectionInfoModel = new ReplDirectionInfoModel();
        replDirectionInfoModel.setId(replDirectionTbl.getId())
                .setClusterName(clusterService.find(replDirectionTbl.getClusterId()).getClusterName())
                .setSrcDcName(dcNameMap.get(replDirectionTbl.getSrcDcId()))
                .setFromDcName(dcNameMap.get(replDirectionTbl.getFromDcId()))
                .setToDcName(dcNameMap.get(replDirectionTbl.getToDcId()))
                .setTargetClusterName(replDirectionTbl.getTargetClusterName());

        ClusterTbl clusterTbl = clusterService.find(replDirectionTbl.getClusterId());
        if (null == clusterTbl)
            throw new IllegalArgumentException(String.format("cluster %d does not exist", replDirectionTbl.getClusterId()));

        replDirectionInfoModel.setClusterName(clusterTbl.getClusterName());
        return replDirectionInfoModel;
    }
    @Override
    public ReplDirectionInfoModel findReplDirectionInfoModelById(long id) {
        ReplDirectionTbl replDirectionTbl = findReplDirectionTblById(id);
        if (replDirectionTbl == null) {
            return null;
        }
        return convertReplDirectionTblToReplDirectionInfoModel(replDirectionTbl, dcService.dcNameMap());
    }

    @Override
    public ReplDirectionTbl addReplDirectionByInfoModel(String clusterName, ReplDirectionInfoModel replDirectionInfoModel) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        if (clusterTbl == null) {
            throw new IllegalArgumentException(String.format("cluster %s does not exist",
                    replDirectionInfoModel.getClusterName()));
        }

        DcTbl srcDc = dcService.find(replDirectionInfoModel.getSrcDcName());
        DcTbl fromDc = dcService.find(replDirectionInfoModel.getFromDcName());
        DcTbl toDc = dcService.find(replDirectionInfoModel.getToDcName());
        if (srcDc == null || fromDc == null || toDc == null) {
            throw new IllegalArgumentException(String.format("srcDc:%s or fromDc:%s or toDc:%s does not exist",
                    replDirectionInfoModel.getSrcDcName(), replDirectionInfoModel.getFromDcName(),
                    replDirectionInfoModel.getToDcName()));
        }

        ReplDirectionTbl proto = dao.createLocal();
        proto.setClusterId(clusterTbl.getId()).setFromDcId(fromDc.getId())
                .setSrcDcId(srcDc.getId()).setToDcId(toDc.getId())
                .setTargetClusterName(replDirectionInfoModel.getTargetClusterName());

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.insert(proto);
            }
        });

        return proto;
    }

    @Override
    public void createReplDirectionBatch(List<ReplDirectionTbl> replDirections) {
        queryHandler.handleBatchInsert(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return dao.insertBatch(replDirections.toArray(new ReplDirectionTbl[replDirections.size()]));
            }
        });
    }

    @Override
    public void deleteReplDirectionBatch(List<ReplDirectionTbl> replDirections) {
        queryHandler.handleBatchDelete(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return dao.deleteBatch(replDirections.toArray(new ReplDirectionTbl[replDirections.size()]),
                        ReplDirectionTblEntity.UPDATESET_FULL);
            }
        }, true);
    }

    @Override
    public void updateReplDirectionBatch(List<ReplDirectionTbl> replDirections) {
        queryHandler.handleBatchUpdate(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                return dao.updateBatch(replDirections.toArray(new ReplDirectionTbl[replDirections.size()]),
                        ReplDirectionTblEntity.UPDATESET_FULL);
            }
        });
    }

    @Override
    public void validateReplDirection(ClusterTbl cluster, List<ReplDirectionTbl> replDirectionTbls) {
        replDirectionTbls.forEach(replDirectionTbl -> {
            if (cluster.getId() != replDirectionTbl.getClusterId()) {
                throw new BadRequestException(String.format(
                        "[updateClusterReplDirections] repl direction should belong to cluster:%d," +
                                " but belong to cluster:%d", cluster.getId(), replDirectionTbl.getClusterId()));
            }
            if (replDirectionTbl.getSrcDcId() != cluster.getActivedcId()) {
                throw new BadRequestException(String.format(
                        "[updateClusterReplDirections] repl direction should copy from src dc:%d, but from %d",
                        cluster.getActivedcId(), replDirectionTbl.getSrcDcId()));
            }
        });
    }

    @Override
    public List<ReplDirectionTbl> convertReplDirectionInfoModelsToReplDirectionTbls(
            List<ReplDirectionInfoModel> replDirections, Map<String, Long> dcNameIdMap) {

        List<ReplDirectionTbl> result = new ArrayList<>();
        if (replDirections == null || replDirections.isEmpty()) {
            return result;
        }

        replDirections.forEach(replDirection ->  {
            result.add(convertReplDirectionInfoModelToReplDirectionTbl(replDirection, dcNameIdMap));
        });
        return result;
    }


    @Override
    public ReplDirectionTbl convertReplDirectionInfoModelToReplDirectionTbl(ReplDirectionInfoModel replDirection,
                                                                            Map<String, Long> dcNameIdMap) {
        ClusterTbl cluster = clusterService.find(replDirection.getClusterName());
        if (cluster == null) {
            throw new BadRequestException(String.format("cluster %d does not exist", replDirection.getClusterName()));
        }
        ReplDirectionTbl result = new ReplDirectionTbl();
        if (replDirection.getId() != 0) {
            result.setId(replDirection.getId());
        }
        result.setClusterId(cluster.getId()).setSrcDcId(dcNameIdMap.get(replDirection.getSrcDcName()))
                .setFromDcId(dcNameIdMap.get(replDirection.getFromDcName()))
                .setToDcId(dcNameIdMap.get(replDirection.getToDcName()))
                .setTargetClusterName(replDirection.getTargetClusterName());
        return result;
    }
}
