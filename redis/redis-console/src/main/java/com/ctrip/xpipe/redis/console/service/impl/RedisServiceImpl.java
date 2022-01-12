package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.ClusterMonitorModifiedNotifier;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MathUtil;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@Service
public class RedisServiceImpl extends AbstractConsoleService<RedisTblDao> implements RedisService {

    @Autowired
    protected RedisDao redisDao;
    @Autowired
    protected ClusterService clusterService;
    @Autowired
    protected DcClusterShardService dcClusterShardService;
    @Autowired
    protected KeeperContainerService keeperContainerService;
    @Autowired
    protected ClusterMetaModifiedNotifier notifier;
    @Autowired
    protected ClusterMonitorModifiedNotifier monitorNotifier;
    @Autowired
    protected DcService dcService;
    @Autowired
    protected ConsoleConfig consoleConfig;

    private Comparator<RedisTbl> redisComparator = new Comparator<RedisTbl>() {
        @Override
        public int compare(RedisTbl o1, RedisTbl o2) {
            if (o1 != null && o2 != null
                    && ObjectUtils.equals(o1.getId(), o2.getId())) {
                return 0;
            }
            return -1;
        }
    };

    @Override
    public RedisTbl find(final long id) {
        return queryHandler.handleQuery(new DalQuery<RedisTbl>() {
            @Override
            public RedisTbl doQuery() throws DalException {
                return dao.findByPK(id, RedisTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<RedisTbl> findAllRedisWithSameIP(String ip) {
        return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return dao.findByIp(ip, RedisTblEntity.READSET_IP_AND_PORT);
            }
        });
    }

    @Override
    public List<RedisTbl> findAllByDcClusterShard(final long dcClusterShardId) {
        return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return dao.findAllByDcClusterShardId(dcClusterShardId, null, RedisTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<RedisTbl> findAllRedisesByDcClusterName(String dcId, String clusterId) {

        return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return dao.findAllByDcClusterName(dcId, clusterId, XPipeConsoleConstant.ROLE_REDIS, RedisTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<RedisTbl> findAllByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {

        return doFindAllByDcClusterShard(dcId, clusterId, shardId, null);
    }

    @Override
    public List<RedisTbl> findRedisesByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {
        return doFindAllByDcClusterShard(dcId, clusterId, shardId, XPipeConsoleConstant.ROLE_REDIS);
    }

    @Override
    public List<RedisTbl> findKeepersByDcClusterShard(String dcId, String clusterId, String shardId) throws ResourceNotFoundException {
        return doFindAllByDcClusterShard(dcId, clusterId, shardId, XPipeConsoleConstant.ROLE_KEEPER);
    }


    private List<RedisTbl> doFindAllByDcClusterShard(String dcId, String clusterId, String shardId, String redisRole) throws ResourceNotFoundException {
        final DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcId, clusterId, shardId);
        if (dcClusterShardTbl == null) {
            throw new ResourceNotFoundException(dcId, clusterId, shardId);
        }
        return doFindAllByDcClusterShardId(dcClusterShardTbl.getDcClusterShardId(), redisRole);
    }

    private List<RedisTbl> doFindAllByDcClusterShardId(long dcClusterShardId, String redisRole) {

        List<RedisTbl> redisTbls = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return dao.findAllByDcClusterShardId(dcClusterShardId, redisRole, RedisTblEntity.READSET_FULL);
            }
        });
        return redisTbls;
    }

    protected int[] insert(List<RedisTbl> redises) {
        return redisDao.createRedisesBatch(redises);

    }
    protected int[] insert(RedisTbl redis) {
        return redisDao.createRedisesBatch(Lists.newArrayList(redis));
    }

    @Override
    public synchronized void insertRedises(String dcId, String clusterId, String shardId,
                                           List<Pair<String, Integer>> redisAddresses)
            throws DalException, ResourceNotFoundException {

        doInsertInstances(XPipeConsoleConstant.ROLE_REDIS, dcId, clusterId, shardId, redisAddresses);

        notifyClusterUpdate(dcId, clusterId);
    }

    @Override
    public synchronized int insertKeepers(String dcId, String clusterId, String shardId,
                                          List<KeeperBasicInfo> keepers) throws DalException, ResourceNotFoundException {

        logger.info("[insertKeepers]{}, {}, {}, {}", dcId, clusterId, shardId, keepers);
        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcId, clusterId, shardId);
        if (dcClusterShardTbl == null) {
            throw new ResourceNotFoundException(dcId, clusterId, shardId);
        }

        List<RedisTbl> insertKeepers = new LinkedList<>();

        keepers.forEach(keeperBasicInfo -> {
            RedisTbl keeper = createRedisTbl(new Pair<>(keeperBasicInfo.getHost(), keeperBasicInfo.getPort()), XPipeConsoleConstant.ROLE_KEEPER);
            keeper.setKeepercontainerId(keeperBasicInfo.getKeeperContainerId());
            keeper.setDcClusterShardId(dcClusterShardTbl.getDcClusterShardId());
            insertKeepers.add(keeper);
        });

        validateKeepers(insertKeepers);

        int[] insert = insert(insertKeepers);
        notifyClusterUpdate(dcId, clusterId);
        return MathUtil.sum(insert);
    }

    @Override
    public List<RedisTbl> deleteKeepers(String dcId, String clusterId, String shardId) throws DalException, ResourceNotFoundException {

        List<RedisTbl> keepersByDcClusterShard = findKeepersByDcClusterShard(dcId, clusterId, shardId);

        queryHandler.handleQuery(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                redisDao.deleteRedisesBatch(keepersByDcClusterShard);
                return null;
            }
        });
        return keepersByDcClusterShard;
    }


    private void doInsertInstances(String role, String dcId, String clusterId, String shardId, List<Pair<String, Integer>> redisAddresses) throws ResourceNotFoundException, DalException {

        DcClusterShardTbl dcClusterShardTbl = dcClusterShardService.find(dcId, clusterId, shardId);
        if (dcClusterShardTbl == null) {
            throw new ResourceNotFoundException(dcId, clusterId, shardId);
        }
        List<RedisTbl> redisTbls = doFindAllByDcClusterShardId(dcClusterShardTbl.getDcClusterShardId(), XPipeConsoleConstant.ROLE_REDIS);

        List<Pair<String, Integer>> toAdd = sub(redisAddresses, redisTbls);

        logger.info("[doInsertInstances]{}", toAdd);

        insertInstancesToDb(dcClusterShardTbl.getDcClusterShardId(), role, toAdd.toArray(new Pair[0]));
    }

    @Override
    public void deleteRedises(String dcId, String clusterId, String shardId, List<Pair<String, Integer>> redisAddresses) throws ResourceNotFoundException {

        List<RedisTbl> redisTbls = findRedisesByDcClusterShard(dcId, clusterId, shardId);
        List<RedisTbl> toDelete = inter(redisAddresses, redisTbls);
        logger.info("[deleteRedises]{}", toDelete);
        queryHandler.handleQuery(new DalQuery<int[]>() {
            @Override
            public int[] doQuery() throws DalException {
                redisDao.deleteRedisesBatch(toDelete);
                return null;
            }
        });
        notifyClusterUpdate(dcId, clusterId);
    }

    private RedisTbl createRedisTbl(Pair<String, Integer> addr, String role) {
        return dao.createLocal()
                .setRedisIp(addr.getKey().trim())
                .setRedisPort(addr.getValue())
                .setRedisRole(role.trim())
                .setRunId("unknown");
    }

    @Override
    public void updateByPK(final RedisTbl redis) {
        if (null != redis) {
            queryHandler.handleQuery(new DalQuery<Integer>() {
                @Override
                public Integer doQuery() throws DalException {
                    return dao.updateByPK(redis, RedisTblEntity.UPDATESET_FULL);
                }
            });
        }
    }

    @Override
    public void updateBatchMaster(List<RedisTbl> redises) {

        redisDao.updateBatchMaster(redises);
    }

    @Override
    public void updateBatchKeeperActive(List<RedisTbl> redises) {
        redisDao.updateBatchKeeperActive(redises);
    }


    @Override
    public void updateRedises(String dcName, String clusterName, String shardName, ShardModel shardModel) {

        if (null == shardModel) {
            throw new BadRequestException("RequestBody cannot be null.");
        }
        final DcClusterShardTbl dcClusterShard = dcClusterShardService.find(dcName, clusterName, shardName);
        if (null == dcClusterShard) {
            throw new BadRequestException("Cannot find related dc-cluster-shard.");
        }

        List<RedisTbl> originRedises = findAllByDcClusterShard(dcClusterShard.getDcClusterShardId());
        List<RedisTbl> toUpdateRedises = formatRedisesFromShardModel(dcClusterShard, shardModel);

        updateRedises(originRedises, toUpdateRedises);

        // update current cluster to xpipe-interested
        ClusterTbl clusterInfo = clusterService.find(clusterName);
        if (null != clusterInfo && !clusterInfo.isIsXpipeInterested()) {
            clusterInfo.setIsXpipeInterested(true);
            clusterService.update(clusterInfo);
        }

        // Notify metaserver
        notifyClusterUpdate(dcName, clusterName);
    }

    @Override
    public List<RedisTbl> findAllKeeperContainerCountInfo() {
        return queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return dao.countContainerKeeperAndClusterAndShard(RedisTblEntity.READSET_CONTAINER_LOAD);
            }
        });
    }

    @Override
    public List<Long> findClusterIdsByKeeperContainer(long keeperContainerId) {
        List<RedisTbl> keepers = queryHandler.handleQuery(new DalQuery<List<RedisTbl>>() {
            @Override
            public List<RedisTbl> doQuery() throws DalException {
                return dao.findDcClusterByKeeperContainer(keeperContainerId, RedisTblEntity.READSET_CLUSTER_ID);
            }
        });

        Set<Long> clusterIdSet = new HashSet<>();
        keepers.forEach(keeper -> clusterIdSet.add(keeper.getDcClusterInfo().getClusterId()));

        return new ArrayList<>(clusterIdSet);
    }

    protected void notifyClusterUpdate(String dcName, String clusterName) {
        ClusterTbl cluster = clusterService.find(clusterName);
        if (null == cluster) throw new IllegalArgumentException("not exist cluster " + clusterName);

        ClusterType type = ClusterType.lookup(cluster.getClusterType());
        if (consoleConfig.shouldNotifyClusterTypes().contains(type.name())) {
            if (type.supportMultiActiveDC()) {
                List<DcTbl> dcTbls = dcService.findClusterRelatedDc(clusterName);
                if (null != dcTbls) notifier.notifyClusterUpdate(clusterName,
                        dcTbls.stream().map(DcTbl::getDcName).collect(Collectors.toList()));
            } else {
                notifier.notifyClusterUpdate(clusterName, Collections.singletonList(dcName));
            }
        }

        if (type.supportMigration()) {
            monitorNotifier.notifyClusterUpdate(clusterName, cluster.getClusterOrgId());
        }
    }

    private void updateRedises(List<RedisTbl> origin, List<RedisTbl> target) {

        validateKeepers(RedisDao.findWithRole(target, XPipeConsoleConstant.ROLE_KEEPER));

        List<RedisTbl> toCreate = (List<RedisTbl>) setOperator.difference(RedisTbl.class, target, origin,
                redisComparator);
        List<RedisTbl> toDelete = (List<RedisTbl>) setOperator.difference(RedisTbl.class, origin, target,
                redisComparator);
        List<RedisTbl> left = (List<RedisTbl>) setOperator.intersection(RedisTbl.class, origin, target,
                redisComparator);

        updateRedises(toCreate, toDelete, left);
    }

    private void updateRedises(final List<RedisTbl> toCreate, final List<RedisTbl> toDelete, final List<RedisTbl> left) {
        try {
            redisDao.handleUpdate(toCreate, toDelete, left);
        } catch (DalException e) {
            throw new ServerException(e.getMessage());
        }
    }

    private void addRedisTbl(DcClusterShardTbl dcClusterShard, List<RedisTbl> result, List<RedisTbl> redises, String defaultRole) {

        if (redises == null) {
            return;
        }

        for (RedisTbl redis : redises) {
            RedisTbl proto = dao.createLocal();
            if (null != redis.getRunId()) {
                proto.setRunId(redis.getRunId());
            } else {
                proto.setRunId("unknown");
            }
            proto.setId(redis.getId()).setRedisIp(redis.getRedisIp()).setRedisPort(redis.getRedisPort())
                    .setKeeperActive(redis.isKeeperActive()).setKeepercontainerId(redis.getKeepercontainerId());

            proto.setMaster(redis.isMaster());
            if (!StringUtil.isEmpty(redis.getRedisRole())) {
                proto.setRedisRole(redis.getRedisRole());
            } else {
                proto.setRedisRole(defaultRole);
            }
            if (null != dcClusterShard) {
                proto.setDcClusterShardId(dcClusterShard.getDcClusterShardId());
            }
            result.add(proto);
        }

    }


    private List<RedisTbl> formatRedisesFromShardModel(DcClusterShardTbl dcClusterShard, ShardModel shardModel) {
        List<RedisTbl> result = new LinkedList<>();
        if (null == shardModel) {
            return result;
        }

        addRedisTbl(dcClusterShard, result, shardModel.getRedises(), XPipeConsoleConstant.ROLE_REDIS);
        addRedisTbl(dcClusterShard, result, shardModel.getKeepers(), XPipeConsoleConstant.ROLE_KEEPER);
        return result;
    }

    // Use protected for Unit Test available
    protected void validateKeepers(List<RedisTbl> keepers) {
        if (2 != keepers.size()) {
            if (0 == keepers.size()) {
                return;
            }
            throw new BadRequestException("Keepers' size must be 0 or 2");
        }

        if (keepers.get(0).getKeepercontainerId() == keepers.get(1).getKeepercontainerId()) {
            throw new BadRequestException("Keepers should be assigned to different keepercontainer" + keepers);
        }


        List<RedisTbl> originalKeepers = RedisDao.findWithRole(findAllByDcClusterShard(keepers.get(0).getDcClusterShardId()), XPipeConsoleConstant.ROLE_KEEPER);
        Set<Long> keepercontainerAvialableZones = new HashSet<>();
        for (int cnt = 0; cnt != 2; ++cnt) {
            final RedisTbl keeper = keepers.get(cnt);
            KeepercontainerTbl keepercontainer = keeperContainerService.find(keeper.getKeepercontainerId());
            if (null == keepercontainer) {
                throw new BadRequestException("Cannot find related keepercontainer");
            }
            if (!keeper.getRedisIp().equals(keepercontainer.getKeepercontainerIp())) {
                throw new BadRequestException("Keeper's ip should be equal to keepercontainer's ip");
            }
            if(keepercontainer.getAzId() != 0 && !keepercontainerAvialableZones.add(keepercontainer.getAzId())) {
                throw new BadRequestException("Keepers should be in different available zones");
            }

            // port check
            RedisTbl redisWithSameConfiguration = queryHandler.handleQuery(new DalQuery<RedisTbl>() {
                @Override
                public RedisTbl doQuery() throws DalException {
                    return dao.findWithIpPort(keeper.getRedisIp(), keeper.getRedisPort(), RedisTblEntity.READSET_FULL);
                }
            });
            if (null != redisWithSameConfiguration
                    && !ObjectUtils.equals(keeper.getId(), redisWithSameConfiguration.getId())) {
                throw new BadRequestException("Already in use for keeper's port : "
                        + String.valueOf(redisWithSameConfiguration.getRedisPort()));
            }

            // keepercontainer check
            for (RedisTbl originalKeeper : originalKeepers) {
                if (originalKeeper.getKeepercontainerId() == keeper.getKeepercontainerId()
                        && !originalKeeper.getId().equals(keeper.getId())) {
                    throw new BadRequestException("If you wanna change keeper port in same keepercontainer,please delete it first.");
                }
            }
        }
    }


    protected List<Pair<String, Integer>> sub(List<Pair<String, Integer>> userGiven, List<RedisTbl> redisTbls) {

        List<Pair<String, Integer>> result = new LinkedList<>();
        userGiven.forEach(new Consumer<Pair<String, Integer>>() {
            @Override
            public void accept(Pair<String, Integer> addr) {

                boolean exist = false;

                for (RedisTbl redisTbl : redisTbls) {
                    if (addr.getKey().equalsIgnoreCase(redisTbl.getRedisIp())
                            && addr.getValue().equals(redisTbl.getRedisPort())) {
                        exist = true;
                        break;
                    }
                }
                if (!exist) {
                    result.add(addr);
                }
            }
        });
        return result;
    }


    protected List<RedisTbl> inter(List<Pair<String, Integer>> userGiven, List<RedisTbl> redisTbls) {

        List<RedisTbl> result = new LinkedList<>();
        redisTbls.forEach(new Consumer<RedisTbl>() {
            @Override
            public void accept(RedisTbl redisTbl) {
                boolean exist = false;
                for (Pair<String, Integer> addr : userGiven) {
                    if (addr.getKey().equalsIgnoreCase(redisTbl.getRedisIp())
                            && addr.getValue().equals(redisTbl.getRedisPort())) {
                        exist = true;
                        break;
                    }
                }
                if (exist) {
                    result.add(redisTbl);
                }
            }
        });
        return result;
    }

    public void insertInstancesToDb(long dcClusterShardId, String role, Pair<String, Integer>... addrs) throws DalException {

        for (Pair<String, Integer> addr : addrs) {
            insert(createRedisTbl(addr, role).setDcClusterShardId(dcClusterShardId));
        }

    }
}
