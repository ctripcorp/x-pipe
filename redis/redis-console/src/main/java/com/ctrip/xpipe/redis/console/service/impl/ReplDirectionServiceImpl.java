package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.DcGroupType;
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
    public List<ReplDirectionTbl> findAllReplDirectionJoinClusterTbl() {
        List<ReplDirectionTbl> replDirectionTbls = queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findAllReplDirectionJoinClusterTbl(ReplDirectionTblEntity.READSET_REPL_DIRECTION_CLUSTER_INFO);
            }
        });

        Map<Long, Long> clusterIdActiveDcIdMap = new HashMap<>();
        List<ClusterTbl> allHeteroClusters = clusterService.findClustersByGroupType(DcGroupType.MASTER.name());
        allHeteroClusters.forEach(heteroCluster -> {
            clusterIdActiveDcIdMap.put(heteroCluster.getId(), heteroCluster.getActivedcId());
        });

        replDirectionTbls.forEach(replDirectionTbl -> {
            Long dcId = clusterIdActiveDcIdMap.get(replDirectionTbl.getClusterId());
            if (dcId != null) {
                if (replDirectionTbl.getSrcDcId() == 0) {
                    replDirectionTbl.setSrcDcId(dcId);
                }

                if (replDirectionTbl.getFromDcId() == 0) {
                    replDirectionTbl.setFromDcId(dcId);
                }
            }
        });

        return replDirectionTbls;
    }

    private List<ReplDirectionTbl> findAllReplDirectionTblsByCLusterAndToDc(long clusterId, long toDcId) {
        return queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findReplDirectionsByClusterAndToDc(clusterId, toDcId, ReplDirectionTblEntity.READSET_FULL);
            }
        });
    }

    private List<ReplDirectionTbl> findAllReplDirections() {
        return queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findAllReplDirections(ReplDirectionTblEntity.READSET_FULL);
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
    public List<ReplDirectionTbl> findAllReplDirectionTblsByClusterWithSrcDcAndFromDc(long clusterId) {

        ClusterTbl cluster = clusterService.find(clusterId);

        List<ReplDirectionTbl> replDirections = queryHandler.handleQuery(new DalQuery<List<ReplDirectionTbl>>() {
            @Override
            public List<ReplDirectionTbl> doQuery() throws DalException {
                return dao.findReplDirectionsByCluster(clusterId, ReplDirectionTblEntity.READSET_FULL);
            }
        });
        replDirections.forEach(replDirection -> {
            replDirection.setSrcDcId(replDirection.getSrcDcId() == 0 ? cluster.getActivedcId() : replDirection.getSrcDcId());
            replDirection.setFromDcId(replDirection.getFromDcId() == 0 ? cluster.getActivedcId() : replDirection.getFromDcId());
        });
        return replDirections;
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
        List<ReplDirectionTbl> allReplDirectionTbls = findAllReplDirections();
        HashMap<Long, ReplDirectionInfoModel> replDirectionIdInfoMap = new HashMap<>();
        Map<Long, String> dcNameMap = dcService.dcNameMap();

        for (ReplDirectionTbl replDirectionTbl : allReplDirectionTbls) {
            ReplDirectionInfoModel replDirectionInfoModel = new ReplDirectionInfoModel();
            replDirectionInfoModel.setId(replDirectionTbl.getId())
                    .setClusterId(replDirectionTbl.getClusterId())
                    .setToDcName(dcNameMap.get(replDirectionTbl.getToDcId()))
                    .setTargetClusterName(replDirectionTbl.getTargetClusterName());

            ClusterTbl clusterTbl = clusterService.find(replDirectionTbl.getClusterId());
            if (clusterTbl != null) {
                replDirectionInfoModel.setClusterName(clusterTbl.getClusterName());
                String srcDcName = replDirectionTbl.getSrcDcId() == 0 ? dcNameMap.get(clusterTbl.getActivedcId()) : dcNameMap.get(replDirectionTbl.getSrcDcId());
                String fromDcName = replDirectionTbl.getFromDcId() == 0 ? dcNameMap.get(clusterTbl.getActivedcId()) : dcNameMap.get(replDirectionTbl.getFromDcId());
                replDirectionInfoModel.setSrcDcName(srcDcName).setFromDcName(fromDcName);

                List<DcClusterShardTbl> srcDcClusterShards =
                        dcClusterShardService.findAllByDcCluster(srcDcName, clusterTbl.getClusterName());
                replDirectionInfoModel.setSrcShardCount(srcDcClusterShards == null ? 0: srcDcClusterShards.size());

                List<DcClusterShardTbl> toDcClusterShards =
                        dcClusterShardService.findAllByDcCluster(dcNameMap.get(replDirectionTbl.getToDcId()), clusterTbl.getClusterName());
                replDirectionInfoModel.setToShardCount(toDcClusterShards == null ? 0 : toDcClusterShards.size());

                List<RedisTbl> allKeepers =
                        redisService.findAllKeepersByDcClusterName(srcDcName, clusterTbl.getClusterName());
                replDirectionInfoModel.setKeeperCount(allKeepers == null ? 0 : allKeepers.size());

                List<ApplierTbl> allAppliers =
                        applierService.findAppliersByClusterAndToDc(replDirectionTbl.getToDcId(), clusterTbl.getId());
                replDirectionInfoModel.setApplierCount(allAppliers == null ? 0 : allAppliers.size());
            }
            replDirectionIdInfoMap.put(replDirectionTbl.getId(), replDirectionInfoModel);
        }

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

        List<ReplDirectionTbl> allReplDirectionTblsByCLusterAndToDc = findAllReplDirectionTblsByCLusterAndToDc(clusterTbl.getId(), toDcTbl.getId());
        for (ReplDirectionTbl replDirectionTbl : allReplDirectionTblsByCLusterAndToDc) {
            if ((replDirectionTbl.getSrcDcId() == 0 && srcDcTbl.getId() == clusterTbl.getActivedcId())
                    || replDirectionTbl.getSrcDcId() == srcDcTbl.getId())
                return replDirectionTbl;
        }

        return null;
    }

    @Override
    public List<ReplDirectionInfoModel> findReplDirectionInfoModelsByClusterAndToDc(String clusterName, String toDcName) {
        ClusterTbl cluster = clusterService.find(clusterName);
        if (cluster == null) {
            throw new IllegalArgumentException(String.format("cluster %s does not exist", clusterName));
        }

        DcTbl toDc = dcService.find(toDcName);
        if (toDc == null) {
            throw new IllegalArgumentException(String.format("dc %s does not exist", toDcName));
        }

        List<ReplDirectionTbl> replDirectionTbls = findAllReplDirectionTblsByCLusterAndToDc(cluster.getId(), toDc.getId());
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
        ClusterTbl cluster = clusterService.find(replDirectionTbl.getClusterId());
        if (null == cluster)
            throw new IllegalArgumentException(String.format("cluster %d does not exist", replDirectionTbl.getClusterId()));
        replDirectionInfoModel.setClusterName(cluster.getClusterName());

        String srcDcName = replDirectionTbl.getSrcDcId() == 0 ? dcNameMap.get(cluster.getActivedcId()) : dcNameMap.get(replDirectionTbl.getSrcDcId());
        String fromDcName = replDirectionTbl.getFromDcId() == 0 ? dcNameMap.get(cluster.getActivedcId()) : dcNameMap.get(replDirectionTbl.getFromDcId());

        replDirectionInfoModel.setId(replDirectionTbl.getId())
                .setClusterId(replDirectionTbl.getClusterId())
                .setClusterName(clusterService.find(replDirectionTbl.getClusterId()).getClusterName())
                .setSrcDcName(srcDcName)
                .setFromDcName(fromDcName)
                .setToDcName(dcNameMap.get(replDirectionTbl.getToDcId()))
                .setTargetClusterName(replDirectionTbl.getTargetClusterName());

        return replDirectionInfoModel;
    }

    private ReplDirectionTbl findReplDirectionTblById(long id) {
        return queryHandler.handleQuery(new DalQuery<ReplDirectionTbl>() {
            @Override
            public ReplDirectionTbl doQuery() throws DalException {
                return dao.findByPK(id, ReplDirectionTblEntity.READSET_FULL);
            }
        });
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
        proto.setClusterId(clusterTbl.getId())
                .setSrcDcId(srcDc.getId() == clusterTbl.getActivedcId() ? 0 : srcDc.getId())
                .setFromDcId(fromDc.getId() == clusterTbl.getActivedcId() ? 0 : fromDc.getId())
                .setToDcId(toDc.getId())
                .setTargetClusterName(replDirectionInfoModel.getTargetClusterName());

        queryHandler.handleInsert(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.insert(proto);
            }
        });

        proto.setFromDcId(fromDc.getId()).setSrcDcId(srcDc.getId());
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
            if (replDirectionTbl.getSrcDcId() != 0 && replDirectionTbl.getSrcDcId() != cluster.getActivedcId()) {
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
        result.setClusterId(cluster.getId())
                .setSrcDcId(dcNameIdMap.get(replDirection.getSrcDcName()) == cluster.getActivedcId() ? 0 : dcNameIdMap.get(replDirection.getSrcDcName()))
                .setFromDcId(dcNameIdMap.get(replDirection.getFromDcName()) == cluster.getActivedcId() ? 0 : dcNameIdMap.get(replDirection.getFromDcName()))
                .setToDcId(dcNameIdMap.get(replDirection.getToDcName()))
                .setTargetClusterName(replDirection.getTargetClusterName());
        return result;
    }
}
