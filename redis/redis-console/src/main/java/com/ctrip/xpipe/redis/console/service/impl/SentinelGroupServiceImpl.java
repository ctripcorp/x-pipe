package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.EventType;
import com.ctrip.xpipe.redis.console.notifier.ShardEventHandler;
import com.ctrip.xpipe.redis.console.notifier.shard.AbstractShardEvent;
import com.ctrip.xpipe.redis.console.notifier.shard.ShardEvent;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.util.SentinelUtil;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.console.service.impl.ClusterServiceImpl.CLUSTER_DEFAULT_TAG;

@Service
public class SentinelGroupServiceImpl extends AbstractConsoleService<SentinelGroupTblDao> implements SentinelGroupService {

    private DcClusterShardTblDao dcClusterShardTblDao;

    @Autowired
    private SentinelService sentinelService;

    @Autowired
    private DcService dcService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private ShardEventHandler shardEventHandler;

    private static final long CROSS_DC_CONSTANT = 0L;
    private static final String ALL_REGION = "ALL_REGION";
    private static final String MASTER_REGION = "MASTER_REGION";
    @Autowired
    private DcClusterService dcClusterService;
    @Autowired
    private MetaCache metaCache;

    @PostConstruct
    private void postConstruct() {
        try {
            dcClusterShardTblDao = ContainerLoader.getDefaultContainer().lookup(DcClusterShardTblDao.class);
        } catch (ComponentLookupException e) {
            throw new ServerException("Dao construct failed.", e);
        }
    }

    @Override
    public List<SentinelGroupModel> findAllByDcName(String dcName) {
        List<SentinelGroupTbl> sentinelGroups = queryHandler.handleQuery(new DalQuery<List<SentinelGroupTbl>>() {
            @Override
            public List<SentinelGroupTbl> doQuery() throws DalException {
                return dao.findAll(SentinelGroupTblEntity.READSET_FULL);
            }
        });
        return sentinelGroupInfoList(dcName,sentinelGroups);
    }

    @Override
    public List<SentinelGroupModel> findAllByDcAndType(String dcName, ClusterType clusterType) {
        List<SentinelGroupTbl> typeGroups = queryHandler.handleQuery(new DalQuery<List<SentinelGroupTbl>>() {
            @Override
            public List<SentinelGroupTbl> doQuery() throws DalException {
                return dao.findByType(clusterType.name(), SentinelGroupTblEntity.READSET_FULL);
            }
        });
        return sentinelGroupInfoList(dcName,typeGroups);
    }

    List<SentinelGroupModel> sentinelGroupInfoList(String dcName, List<SentinelGroupTbl> sentinelGroups){
        List<SentinelTbl> dcSentinels = sentinelService.findAllByDcName(dcName);
        Map<Long, SentinelGroupTbl> sentinelGroupMap=toMap(sentinelGroups);
        Map<Long, SentinelGroupModel> sentinelGroupInfoMap = new HashMap<>();
        for (SentinelTbl sentinelTbl : dcSentinels) {
            long sentinelGroupId = sentinelTbl.getSentinelGroupId();
            SentinelGroupTbl sentinelGroupTbl = sentinelGroupMap.get(sentinelGroupId);
            if (sentinelGroupTbl != null)
                sentinelGroupInfoMap.compute(sentinelGroupId, (k, v) -> {
                    if (v == null)
                        v = new SentinelGroupModel(sentinelGroupTbl);
                    v.addSentinel(new SentinelInstanceModel(sentinelTbl));
                    return v;
                });
        }
        return new ArrayList<>(sentinelGroupInfoMap.values());
    }

    Map<Long, SentinelGroupTbl> toMap(List<SentinelGroupTbl> sentinelGroups) {
        Map<Long, SentinelGroupTbl> sentinelGroupMap = new HashMap<>();
        sentinelGroups.forEach(sentinelGroupTbl -> {
            sentinelGroupMap.put(sentinelGroupTbl.getSentinelGroupId(), sentinelGroupTbl);
        });
        return sentinelGroupMap;
    }

    @Override
    public SentinelGroupModel findById(long id) {
        SentinelGroupTbl sentinelGroupTbl = queryHandler.handleQuery(new DalQuery<SentinelGroupTbl>() {
            @Override
            public SentinelGroupTbl doQuery() throws DalException {
                return dao.findByPK(id, SentinelGroupTblEntity.READSET_FULL);
            }
        });

        if (sentinelGroupTbl == null)
            return null;

        List<SentinelTbl> sentinels = sentinelService.findBySentinelGroupId(id);

        SentinelGroupModel sentinelGroupModel = new SentinelGroupModel(sentinelGroupTbl);
        sentinels.forEach(sentinelTbl -> {
            sentinelGroupModel.addSentinel(new SentinelInstanceModel(sentinelTbl));
        });
        return sentinelGroupModel;
    }

    @Override
    public Map<Long, SentinelGroupModel> findByShard(long shardId) {
        List<DcClusterShardTbl> dcClusterShards = queryHandler.handleQuery(new DalQuery<List<DcClusterShardTbl>>() {
            @Override
            public List<DcClusterShardTbl> doQuery() throws DalException {
                return dcClusterShardTblDao.findAllByShardId(shardId, DcClusterShardTblEntity.READSET_FULL);
            }
        });

        Map<Long, SentinelGroupModel> res = new HashMap<>(dcClusterShards.size());
        for(DcClusterShardTbl dcClusterShard : dcClusterShards) {
            SentinelGroupTbl sentinelGroup = queryHandler.handleQuery(new DalQuery<SentinelGroupTbl>() {
                @Override
                public SentinelGroupTbl doQuery() throws DalException {
                    return dao.findByPK(dcClusterShard.getSetinelId(), SentinelGroupTblEntity.READSET_FULL);
                }
            });
            if (null != sentinelGroup) {
                List<SentinelTbl> sentinelTbls = sentinelService.findBySentinelGroupId(sentinelGroup.getSentinelGroupId());
                for (SentinelTbl sentinelTbl : sentinelTbls) {
                    res.putIfAbsent(sentinelTbl.getDcId(), new SentinelGroupModel(sentinelGroup));
                    res.get(sentinelTbl.getDcId()).addSentinel(new SentinelInstanceModel(sentinelTbl));
                }
            }
        }
        return res;
    }

    @Override
    public void addSentinelGroup(SentinelGroupModel sentinelGroupModel) {

        SentinelGroupTbl sentinelGroupTbl;
        if (sentinelGroupModel.getSentinelGroupId() > 0) {
            sentinelGroupTbl = queryHandler.handleQuery(new DalQuery<SentinelGroupTbl>() {
                @Override
                public SentinelGroupTbl doQuery() throws DalException {
                    return dao.findByPK(sentinelGroupModel.getSentinelGroupId(), SentinelGroupTblEntity.READSET_FULL);
                }
            });
            if (sentinelGroupTbl == null)
                throw new IllegalArgumentException(String.format("sentinel group with id:%s not exist", sentinelGroupModel.getSentinelGroupId()));
        } else {
            SentinelGroupTbl insertTbl = new SentinelGroupTbl()
                    .setClusterType(sentinelGroupModel.getClusterType())
                    .setTag(sentinelGroupModel.getTag().trim().toUpperCase())
                    .setSentinelDescription(sentinelGroupModel.getDesc())
                    .setActive(sentinelGroupModel.getActive());
            queryHandler.handleQuery(new DalQuery<Integer>() {
                @Override
                public Integer doQuery() throws DalException {
                    return dao.insert(insertTbl);
                }
            });
            sentinelGroupTbl = insertTbl;
        }

        long sentinelGroupId = sentinelGroupTbl.getSentinelGroupId();
        sentinelGroupModel.getSentinels().forEach(sentinelInstanceModel -> {
            sentinelService.insert(new SentinelTbl().
                    setSentinelGroupId(sentinelGroupId).
                    setDcId(sentinelInstanceModel.getDcId()).
                    setSentinelIp(sentinelInstanceModel.getSentinelIp()).
                    setSentinelPort(sentinelInstanceModel.getSentinelPort()));
        });

    }

    @Override
    public List<SentinelGroupModel> getSentinelGroupsWithUsageByType(ClusterType clusterType, String... region) {
        List<SentinelGroupTbl> sentinelGroupTbls = queryHandler.handleQuery(new DalQuery<List<SentinelGroupTbl>>() {
            @Override
            public List<SentinelGroupTbl> doQuery() throws DalException {
                return dao.findByType(clusterType.name(),SentinelGroupTblEntity.READSET_FULL);
            }
        });

        return getSentinelGroups(sentinelGroupTbls, region);
    }

    @Override
    public List<SentinelGroupModel> getAllSentinelGroupsWithUsage(String... region) {
        List<SentinelGroupTbl> sentinelGroupTbls = queryHandler.handleQuery(new DalQuery<List<SentinelGroupTbl>>() {
            @Override
            public List<SentinelGroupTbl> doQuery() throws DalException {
                return dao.findAll(SentinelGroupTblEntity.READSET_FULL);
            }
        });

        return getSentinelGroups(sentinelGroupTbls, region);
    }

    List<SentinelGroupModel> getSentinelGroups(List<SentinelGroupTbl> sentinelGroupTbls, String... region) {
        Map<Long, SentinelGroupModel> groupMap = sentinelGroupTbls.stream()
            .collect(Collectors.toMap(SentinelGroupTbl::getSentinelGroupId, SentinelGroupModel::new));

        List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.findAll();
        Map<Long, Set<Pair<Long, Long>>> sentinelShardMap = dcClusterShardTbls.stream()
            .filter(dcClusterShardTbl -> groupMap.containsKey(dcClusterShardTbl.getSetinelId()))
            .collect(Collectors.toMap(DcClusterShardTbl::getSetinelId,
                dcClusterShardTbl -> Sets.newHashSet(
                    new Pair<>(ClusterType.lookup(groupMap.get(dcClusterShardTbl.getSetinelId()).getClusterType()).equals(ClusterType.CROSS_DC) ? CROSS_DC_CONSTANT : dcClusterShardTbl.getDcClusterId(), dcClusterShardTbl.getShardId())),
                (v1, v2) -> {
                    v1.addAll(v2);
                    return v1;
                }));

        // 筛选主机房所在region的哨兵分片（对sentinelShardMap中dcClusterId作判断）
        if (region != null && region.length > 0 && Objects.equals(MASTER_REGION, region[0])) {
            logger.info("[getAllSentinelGroupsWithUsage] {} selecting", MASTER_REGION);
            // 1. 获取当前所有哨兵分片对应的dcClusterId
            Set<Long> dcClusterIds = new HashSet<>();
            sentinelShardMap.values().forEach(pairSet -> {
                pairSet.forEach(pair -> {
                    Long dcClusterId = pair.getKey();
                    if (!dcClusterId.equals(CROSS_DC_CONSTANT)) {
                        dcClusterIds.add(dcClusterId);
                    }
                });
            });
            // 2. 准备
            // 2.1 便于通过 dcClusterIds 得到 clusterId 和 dcId
            List<DcClusterTbl> dcClusterTbls = dcClusterService.findAllDcClusters();
            List<ClusterTbl> clusterTbls = clusterService.findAllClusters();
            // 2.2 为clusterId和activeDcId作映射
            Map<Long, Long> clusterActiveIdMap = clusterTbls.stream().collect(Collectors.toMap(ClusterTbl::getId, ClusterTbl::getActivedcId));

            // 3. 获取 dcCluster 跨区信息
            Map<Long, Boolean> isCrossRegionDcCluster = dcClusterTbls.stream()
                    .filter(dcClusterTbl -> dcClusterIds.contains(dcClusterTbl.getDcClusterId()))
                    .collect(Collectors.toMap(DcClusterTbl::getDcClusterId, dcClusterTbl -> metaCache.isCrossRegion(clusterActiveIdMap.get(dcClusterTbl.getClusterId()).toString(), String.valueOf(dcClusterTbl.getDcId()))));

            // 4. 清除所有跨区的哨兵分片
            sentinelShardMap.values().forEach(pairSet -> pairSet.removeIf(pair -> isCrossRegionDcCluster.getOrDefault(pair.getKey(), false)));
        }

        groupMap.forEach((k, v) -> {
            Set<Pair<Long, Long>> shardSet = sentinelShardMap.get(k);
            if (shardSet != null) {
                v.setShardCount(shardSet.size());
            }
        });

        List<SentinelTbl> sentinelTbls = sentinelService.findAllWithDcName();
        sentinelTbls.forEach(sentinelTbl -> {
            if(groupMap.containsKey(sentinelTbl.getSentinelGroupId())){
                groupMap.get(sentinelTbl.getSentinelGroupId()).addSentinel(new SentinelInstanceModel(sentinelTbl));
            }
        });

        return new ArrayList<>(groupMap.values());
    }

    @Override
    public Map<String, SentinelUsageModel> getAllSentinelsUsage(String clusterType, String... regionType) {
        String type = Strings.isNullOrEmpty(clusterType) ? ClusterType.ONE_WAY.name() : clusterType;
        List<SentinelGroupModel> allSentinelGroups = getAllSentinelGroupsWithUsage(regionType);
        Map<String, SentinelUsageModel> result = new HashMap<>();
        allSentinelGroups.forEach(sentinelGroupModel -> {
            if (sentinelGroupModel.getClusterType().equalsIgnoreCase(type)) {
                Set<String> dcs = sentinelGroupModel.dcInfos().keySet();
                for (String dc : dcs) {
                    result.putIfAbsent(dc, new SentinelUsageModel(dc));
                    result.get(dc).addSentinelUsage(sentinelGroupModel.getSentinelsAddressString(), sentinelGroupModel.getShardCount());
                    if (!StringUtil.trimEquals(CLUSTER_DEFAULT_TAG, sentinelGroupModel.getTag(), true)) {
                        result.get(dc).addSentinelTag(sentinelGroupModel.getTag(), sentinelGroupModel.getSentinelsAddressString(), sentinelGroupModel.getShardCount());
                    }
                }
            }
        });
        return result;
    }

    @Override
    public void updateSentinelGroupAddress(SentinelGroupModel sentinelGroupModel) {
        SentinelGroupTbl sentinelGroupTbl = queryHandler.handleQuery(new DalQuery<SentinelGroupTbl>() {
            @Override
            public SentinelGroupTbl doQuery() throws DalException {
                return dao.findByPK(sentinelGroupModel.getSentinelGroupId(), SentinelGroupTblEntity.READSET_FULL);
            }
        });
        if(sentinelGroupTbl==null){
            throw new IllegalArgumentException(String.format("sentinel group with id:%s not exist",sentinelGroupModel.getSentinelGroupId()));
        }
        List<SentinelTbl> sentinelTbls = sentinelService.findBySentinelGroupId(sentinelGroupModel.getSentinelGroupId());

        if(sentinelTbls.size()!=sentinelGroupModel.getSentinels().size())
            throw new IllegalArgumentException(String.format("sentinel num incorrect, previous num:%d, future num:%d",sentinelTbls.size(),sentinelGroupModel.getSentinels().size()));

        for(int i=0;i<sentinelTbls.size();i++){
           SentinelTbl current= sentinelTbls.get(i);
           SentinelInstanceModel future=sentinelGroupModel.getSentinels().get(i);

           current.setDcId(future.getDcId());
           current.setSentinelIp(future.getSentinelIp());
           current.setSentinelPort(future.getSentinelPort());
           sentinelService.updateByPk(current);
        }

    }

    @Override
    public RetMessage removeSentinelMonitor(String clusterName) {
        ClusterTbl clusterTbl = clusterService.find(clusterName);
        ClusterType clusterType = ClusterType.lookup(clusterTbl.getClusterType());
        if (null != clusterType && clusterType.supportMultiActiveDC()) {
            return RetMessage.createFailMessage("cluster type " + clusterType + " not support remove sentinel");
        }

        long activedcId = clusterService.find(clusterName).getActivedcId();
        String dcName = dcService.getDcName(activedcId);
        List<DcClusterShardTbl> dcClusterShardTbls = dcClusterShardService.findAllByDcCluster(dcName, clusterName);
        for(DcClusterShardTbl dcClusterShard : dcClusterShardTbls) {
            try {
                removeSentinelMonitorByShard(dcName, clusterName, clusterType, dcClusterShard);
            } catch (Exception e) {
                return RetMessage.createFailMessage("[stl-id: " + dcClusterShard.getSetinelId() + "]" + e.getMessage());
            }
        }
        return RetMessage.createSuccessMessage();
    }

    @VisibleForTesting
    protected void removeSentinelMonitorByShard(String activeIdc, String clusterName, ClusterType clusterType, DcClusterShardTbl dcClusterShard) {
        ShardTbl shardTbl = shardService.find(dcClusterShard.getShardId());
        SentinelGroupServiceImpl.RemoveShardSentinelMonitorEvent shardEvent = new SentinelGroupServiceImpl.RemoveShardSentinelMonitorEvent(clusterName,
                shardTbl.getShardName(), MoreExecutors.directExecutor());
        shardEvent.setClusterType(clusterType);
        shardEvent.setShardSentinels(findById(dcClusterShard.getSetinelId()).getSentinelsAddressString());
        shardEvent.setShardMonitorName(SentinelUtil.getSentinelMonitorName(clusterName, shardTbl.getSetinelMonitorName(), activeIdc));
        shardEventHandler.handleShardDelete(shardEvent);
    }

    @Override
    public void delete(long id) {
        SentinelGroupTbl sentinelGroupTbl = queryHandler.handleQuery(new DalQuery<SentinelGroupTbl>() {
            @Override
            public SentinelGroupTbl doQuery() throws DalException {
                return dao.findByPK(id, SentinelGroupTblEntity.READSET_FULL);
            }
        });
        if (sentinelGroupTbl == null) {
            throw new IllegalArgumentException(String.format("sentinel group with id:%s not exist", id));
        }
        List<SentinelTbl> sentinelTbls = sentinelService.findBySentinelGroupId(id);
        sentinelTbls.forEach(sentinelTbl -> {
            sentinelService.delete(sentinelTbl.getSentinelId());
        });

        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.deleteSentinelGroup(sentinelGroupTbl, SentinelGroupTblEntity.UPDATESET_FULL);
            }
        });
    }

    @Override
    public void reheal(long id) {
        SentinelGroupTbl sentinelGroupTbl = queryHandler.handleQuery(new DalQuery<SentinelGroupTbl>() {
            @Override
            public SentinelGroupTbl doQuery() throws DalException {
                return dao.findByPK(id, SentinelGroupTblEntity.READSET_FULL);
            }
        });
        sentinelGroupTbl.setDeleted(0);
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateByPK(sentinelGroupTbl, SentinelGroupTblEntity.UPDATESET_FULL);
            }
        });

        List<SentinelTbl> deletedSentinels = sentinelService.findBySentinelGroupIdDeleted(id);
        deletedSentinels.forEach(sentinelTbl -> {
            sentinelService.reheal(sentinelTbl.getSentinelId());
        });
    }

    @Override
    public void updateActive(long id, int active) {
        SentinelGroupTbl sentinelGroupTbl = queryHandler.handleQuery(new DalQuery<SentinelGroupTbl>() {
            @Override
            public SentinelGroupTbl doQuery() throws DalException {
                return dao.findByPK(id, SentinelGroupTblEntity.READSET_FULL);
            }
        });
        if (sentinelGroupTbl == null) {
            throw new IllegalArgumentException(String.format("sentinel group with id:%s not exist", id));
        }
        sentinelGroupTbl.setActive(active);
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateByPK(sentinelGroupTbl, SentinelGroupTblEntity.UPDATESET_FULL);
            }
        });
    }

    @Override
    public SentinelGroupTbl updateTag(long id, String tag) {
        SentinelGroupTbl sentinelGroupTbl = queryHandler.handleQuery(new DalQuery<SentinelGroupTbl>() {
            @Override
            public SentinelGroupTbl doQuery() throws DalException {
                return dao.findByPK(id, SentinelGroupTblEntity.READSET_FULL);
            }
        });
        if (sentinelGroupTbl == null) {
            throw new IllegalArgumentException(String.format("sentinel group with id:%s not exist", id));
        }
        if (tag == null) {
            throw new IllegalArgumentException("sentinel group tag can not be null!");
        }
        sentinelGroupTbl.setTag(tag.trim().toUpperCase());
        queryHandler.handleUpdate(new DalQuery<Integer>() {
            @Override
            public Integer doQuery() throws DalException {
                return dao.updateByPK(sentinelGroupTbl, SentinelGroupTblEntity.UPDATESET_FULL);
            }
        });
        return sentinelGroupTbl;
    }

    private static class RemoveShardSentinelMonitorEvent extends AbstractShardEvent {

        public RemoveShardSentinelMonitorEvent(String clusterName, String shardName, Executor executor) {
            super(clusterName, shardName, executor);
        }

        @Override
        public EventType getShardEventType() {
            return EventType.DELETE;
        }

        @Override
        protected ShardEvent getSelf() {
            return SentinelGroupServiceImpl.RemoveShardSentinelMonitorEvent.this;
        }
    }

    @VisibleForTesting
    void setDcClusterShardTblDao(DcClusterShardTblDao dcClusterShardTblDao) {
        this.dcClusterShardTblDao = dcClusterShardTblDao;
    }

    @VisibleForTesting
    void setSentinelService(SentinelService sentinelService) {
        this.sentinelService = sentinelService;
    }

    @VisibleForTesting
    void setDcService(DcService dcService) {
        this.dcService = dcService;
    }

    @VisibleForTesting
    void setDcClusterShardService(DcClusterShardService dcClusterShardService) {
        this.dcClusterShardService = dcClusterShardService;
    }

    @VisibleForTesting
    void setClusterService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @VisibleForTesting
    void setShardService(ShardService shardService) {
        this.shardService = shardService;
    }

    @VisibleForTesting
    void setShardEventHandler(ShardEventHandler shardEventHandler) {
        this.shardEventHandler = shardEventHandler;
    }
}
