package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.AbstractConsoleService;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.ReplDirectionService;
import com.ctrip.xpipe.utils.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Service
public class ReplDirectionServiceImpl  extends AbstractConsoleService<ReplDirectionTblDao>
        implements ReplDirectionService {

    @Autowired
    ClusterService clusterService;

    @Autowired
    DcService dcService;

    private Comparator<ReplDirectionTbl> replDirectionTblComparator = new Comparator<ReplDirectionTbl>() {
        @Override
        public int compare(ReplDirectionTbl o1, ReplDirectionTbl o2) {
            if (o1 != null && o2 != null
                    && ObjectUtils.equals(o1.getId(), o2.getId())) {
                return 0;
            }
            return -1;
        }
    };

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
    public ReplDirectionInfoModel findReplDirectionInfoModelByClusterAndSrcToDc(String clusterName, String srcDcName, String toDcName) {
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
        if (null == replDirectionTbl)  return null;
        return convertReplDirectionTblToReplDirectionInfoModel(replDirectionTbl, dcService.dcNameMap());
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
    public void addReplDirectionByInfoModel(ReplDirectionInfoModel replDirectionInfoModel) {
        ClusterTbl clusterTbl = clusterService.find(replDirectionInfoModel.getClusterName());
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
    }

    @Override
    public void updateClusterReplDirections(ClusterTbl clusterTbl, List<ReplDirectionInfoModel> replDirections) {
        if (clusterTbl == null) {
            throw new BadRequestException("[updateClusterReplDirections] cluster can not be null!");
        }
        Map<String, Long> dcNameIdMap = dcService.dcNameIdMap();

        List<ReplDirectionTbl> originReplDirections = findAllReplDirectionTblsByCluster(clusterTbl.getId());
        List<ReplDirectionTbl> targetReplDirections =
                convertReplDirectionInfoModelsToReplDirectionTbls(replDirections, dcNameIdMap);

        validateReplDirection(clusterTbl, targetReplDirections);
        updateClusterReplDirections(originReplDirections, targetReplDirections);
    }

    private void validateReplDirection(ClusterTbl cluster, List<ReplDirectionTbl> replDirectionTbls) {
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

    private void updateClusterReplDirections(List<ReplDirectionTbl> originReplDirections,
                                             List<ReplDirectionTbl> targetReplDirections) {

        List<ReplDirectionTbl> toCreate = (List<ReplDirectionTbl>) setOperator.difference(ReplDirectionTbl.class,
                targetReplDirections, originReplDirections, replDirectionTblComparator);

        List<ReplDirectionTbl> toDelete = (List<ReplDirectionTbl>) setOperator.difference(ReplDirectionTbl.class,
                originReplDirections, targetReplDirections, replDirectionTblComparator);

        List<ReplDirectionTbl> toUpdate = (List<ReplDirectionTbl>) setOperator.intersection(ReplDirectionTbl.class,
                originReplDirections, targetReplDirections, replDirectionTblComparator);

        try {
            handleUpdateReplDirecitons(toCreate, toDelete, toUpdate);
        } catch (Exception e) {
            throw new ServerException(e.getMessage());
        }
    }

    private void handleUpdateReplDirecitons(List<ReplDirectionTbl> toCreate, List<ReplDirectionTbl> toDelete,
                                             List<ReplDirectionTbl> toUpdate) {
        if (toCreate != null && !toCreate.isEmpty()) {
            logger.info("[updateClusterReplDirections] create repl direction {}", toCreate);
            createReplDirectionBatch(toCreate);
        }

        if (toDelete != null && !toDelete.isEmpty()) {
            logger.info("[updateClusterReplDirections] delete repl direction {}", toDelete);
            deleteReplDirectionBatch(toDelete);
        }

        if (toUpdate != null && !toUpdate.isEmpty()) {
            logger.info("[updateClusterReplDirections] update repl direction {}", toUpdate);
            updateReplDirectionBatch(toUpdate);
        }
    }

    private void createReplDirectionBatch(List<ReplDirectionTbl> replDirections) {
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


    private List<ReplDirectionTbl> convertReplDirectionInfoModelsToReplDirectionTbls(
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

    private ReplDirectionTbl convertReplDirectionInfoModelToReplDirectionTbl(ReplDirectionInfoModel replDirection,
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
