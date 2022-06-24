package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ApplierDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.notifier.ClusterMonitorModifiedNotifier;
import com.ctrip.xpipe.redis.console.query.DalQuery;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.spring.AbstractSpringConfigContext;
import com.ctrip.xpipe.utils.MapUtils;
import com.ctrip.xpipe.utils.ObjectUtils;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

@Service
public class ApplierServiceImpl extends AbstractConsoleService<ApplierTblDao> implements ApplierService {

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private AppliercontainerService appliercontainerService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private ClusterMetaModifiedNotifier notifier;

    @Autowired
    private DcService dcService;

    @Autowired
    private ApplierDao applierDao;

    @Autowired
    private ClusterMonitorModifiedNotifier monitorNotifier;

    @Resource(name = AbstractSpringConfigContext.GLOBAL_EXECUTOR)
    ExecutorService executor;

    private Comparator<ApplierTbl> applierComparator = new Comparator<ApplierTbl>() {
        @Override
        public int compare(ApplierTbl o1, ApplierTbl o2) {
            if (o1 != null && o2 != null
                    && ObjectUtils.equals(o1.getId(), o2.getId())) {
                return 0;
            }
            return -1;
        }
    };

    @Override
    public ApplierTbl findApplierTblById(long id) {
        return queryHandler.handleQuery(new DalQuery<ApplierTbl>() {
            @Override
            public ApplierTbl doQuery() throws DalException {
                return dao.findByPK(id, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public ApplierTbl findApplierTblByIpPort(String ip, int port) {
        return queryHandler.handleQuery(new DalQuery<ApplierTbl>() {
            @Override
            public ApplierTbl doQuery() throws DalException {
                return dao.findByIpPort(ip, port, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<ApplierTbl> findAll() {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return dao.findAll(ApplierTblEntity.READSET_FULL_WITH_SHARD_INFO);
            }
        });
    }

    @Override
    public List<ApplierTbl> findApplierTblByShardAndReplDirection(long shardId, long replDirectionId) {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return dao.findAllByShardAndReplDirection(shardId, replDirectionId, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public List<ApplierTbl> findAllApplierTblsWithSameIp(String ip) {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return dao.findByIp(ip, ApplierTblEntity.READSET_FULL);
            }
        });
    }

    @Override
    public void updateAppliers(String dcName, String clusterName, String shardName, ShardModel sourceShard,
                               long replDirectionId) {
        if (null == sourceShard) {
            throw new BadRequestException("[updateAppliers]sourceModel can not be null");
        }

        List<ApplierTbl> originAppliers =
                findApplierTblByShardAndReplDirection(sourceShard.getShardTbl().getId(), replDirectionId);
        List<ApplierTbl> targetAppliers = formatAppliersFromSourceModel(sourceShard, replDirectionId);
        updateAppliers(originAppliers, targetAppliers);

        notifyClusterUpdate(dcName, clusterName);
    }

    private void updateAppliers(List<ApplierTbl> originAppliers, List<ApplierTbl> targetAppliers) {
        validateAppliers(originAppliers, targetAppliers);

        List<ApplierTbl> toCreate = (List<ApplierTbl>) setOperator.difference(ApplierTbl.class, targetAppliers,
                originAppliers, applierComparator);
        List<ApplierTbl> toDelete = (List<ApplierTbl>) setOperator.difference(ApplierTbl.class, originAppliers,
                targetAppliers, applierComparator);
        List<ApplierTbl> left = (List<ApplierTbl>) setOperator.intersection(ApplierTbl.class, originAppliers,
                targetAppliers, applierComparator);

        updateAppliers(toCreate, toDelete, left);

    }

    private void updateAppliers(final List<ApplierTbl> toCreate, final List<ApplierTbl> toDelete,
                                final List<ApplierTbl> left) {
        try {
            applierDao.handleUpdate(toCreate, toDelete, left);
        } catch (Exception e) {
            throw new ServerException(e.getMessage());
        }

    }

    private void validateAppliers(List<ApplierTbl> originAppliers,  List<ApplierTbl> targetAppliers) {
        if (targetAppliers.size() != 2) {
            if (targetAppliers.size() == 0) {
                return;
            }
            throw new BadRequestException("size of appliers must be 0 or 2");
        }

        if (targetAppliers.get(0).getContainerId() == targetAppliers.get(1).getContainerId()) {
            throw new BadRequestException("appliers should be assigned to different applier containers : " + targetAppliers);
        }

        Set<Long> applierContainerAvailableZones = new HashSet<>();
        for (int cnt = 0; cnt != 2; cnt++) {
            final ApplierTbl applier = targetAppliers.get(cnt);
            AppliercontainerTbl appliercontainer = appliercontainerService.findAppliercontainerTblById(applier.getContainerId());
            if (appliercontainer == null) {
                throw new BadRequestException("can not find related applier containers " + applier.getContainerId());
            }
            if (!applier.getIp().equals(appliercontainer.getAppliercontainerIp())) {
                throw new BadRequestException(String.format("applier's ip : %s should be equal to applier container's ip : %s",
                        applier.getIp(), appliercontainer.getAppliercontainerIp()));
            }
            if (appliercontainer.getAppliercontainerAz() != 0
                    && !applierContainerAvailableZones.add(appliercontainer.getAppliercontainerAz())) {
                logger.error("appliers {}:{} and {}:{} are in the same available zone {}",
                        targetAppliers.get(0).getIp(), targetAppliers.get(0).getPort(),
                        targetAppliers.get(1).getIp(), targetAppliers.get(1).getPort(),
                        appliercontainer.getAppliercontainerAz()
                );
            }

            ApplierTbl applierWithSameIpPort = findApplierTblByIpPort(applier.getIp(), applier.getPort());
            if (applierWithSameIpPort != null && !ObjectUtils.equals(applier.getId(), applier.getPort())) {
                throw new BadRequestException(
                        String.format("Already int use for applier`s port:%d", applierWithSameIpPort.getPort()));
            }

            for (ApplierTbl originApplier : originAppliers) {
                if (applier.getContainerId() == originApplier.getContainerId()
                        && !ObjectUtils.equals(applier.getId(), originApplier.getId())) {
                    throw new BadRequestException("If you wanna change applier port in same applier container" +
                            ", please delete it first!!");
                }
            }
        }
    }

    private List<ApplierTbl> formatAppliersFromSourceModel(ShardModel sourceShard, long replDirectionId) {
        List<ApplierTbl> result = new ArrayList<>();

        List<ApplierTbl> appliers = sourceShard.getAppliers();
        if (null == appliers) return result;

        for (ApplierTbl applier : appliers) {
            ApplierTbl proto = dao.createLocal();
            proto.setIp(applier.getIp()).setPort(applier.getPort())
                    .setContainerId(applier.getContainerId()).setActive(applier.isActive());

            proto.setShardId(sourceShard.getShardTbl().getId());
            proto.setReplDirectionId(replDirectionId);
            result.add(proto);
        }

        return result;
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

    @Override
    public List<ApplierTbl> findAllAppliercontainerCountInfo() {
        return queryHandler.handleQuery(new DalQuery<List<ApplierTbl>>() {
            @Override
            public List<ApplierTbl> doQuery() throws DalException {
                return dao.countContainerApplierAndClusterAndShard(ApplierTblEntity.READSET_CONTAINER_LOAD);
            }
        });
    }

    @Override
    public List<ApplierBasicInfo> findBestAppliers(String dcName, int beginPort,
                                                   BiPredicate<String, Integer> applierGood, String clusterName) {
        return findBestAppliers(dcName, beginPort, applierGood, clusterName, 2);
    }

    private List<ApplierBasicInfo> findBestAppliers(String dcName, int beginPort,
                                    BiPredicate<String, Integer> applierGood, String clusterName, int returnCount) {
        List<ApplierBasicInfo> result = new ArrayList<>();

        List<AppliercontainerTbl> appliercontainerTbls =
                appliercontainerService.findBestAppliercontainersByDcCluster(dcName, clusterName);
        if (appliercontainerTbls.size() < returnCount) {
            throw new IllegalStateException(String.format("find appliercontainer size:%d, but we need:%d",
                    appliercontainerTbls.size(), returnCount));
        }

        fillInResult(appliercontainerTbls, result, beginPort, applierGood, returnCount);
        return result;
    }

    private void fillInResult(List<AppliercontainerTbl> appliercontainerTbls, List<ApplierBasicInfo> result,
                              int beginPort, BiPredicate<String, Integer> applierGood, int returnCount) {

        Map<String, Set<Integer>> ipUsedPortsMap = getUsedPortsOfAppliercontainers(appliercontainerTbls);

        for (int i = 0; i < returnCount; i++) {
            AppliercontainerTbl appliercontainerTbl = appliercontainerTbls.get(i);
            ApplierBasicInfo applierSelected = new ApplierBasicInfo();

            applierSelected.setAppliercontainerId(appliercontainerTbl.getAppliercontainerId())
                    .setHost(appliercontainerTbl.getAppliercontainerIp())
                    .setPort(findAvailablePort(appliercontainerTbl, beginPort, applierGood, result, ipUsedPortsMap));

            result.add(applierSelected);
        }

    }

    private int findAvailablePort(AppliercontainerTbl appliercontainerTbl, int beginPort, BiPredicate<String, Integer> applierGood,
                                  List<ApplierBasicInfo> result, Map<String, Set<Integer>> ipUsedPortsMap) {
        int port = beginPort;
        String ip = appliercontainerTbl.getAppliercontainerIp();

        Set<Integer> usedPorts = ipUsedPortsMap.get(ip);
        for (;; port++) {
            if (isAlreadySelected(ip, port, result)) continue;

            if (!(applierGood.test(ip, port))) continue;

            if (usedPorts.contains(port)) continue;

            break;
        }

        return port;
    }

    private boolean isAlreadySelected(String ip, int port, List<ApplierBasicInfo> result) {
        for (ApplierBasicInfo applierSelected : result) {
            if (applierSelected.getHost().equalsIgnoreCase(ip) && applierSelected.getPort() == port) {
                return true;
            }
        }

        return false;
    }

    private Map<String, Set<Integer>> getUsedPortsOfAppliercontainers(List<AppliercontainerTbl> appliercontainerTbls) {
        Map<String, Set<Integer>> ipUsedPortsMap = Maps.newHashMap();
        appliercontainerTbls.forEach(appliercontainerTbl ->
                MapUtils.getOrCreate(ipUsedPortsMap, appliercontainerTbl.getAppliercontainerIp(), Sets::newHashSet));

        List<Future> futures = new ArrayList<>(ipUsedPortsMap.size());
        for (Map.Entry<String,Set<Integer>> entry : ipUsedPortsMap.entrySet()) {
            String containerIp = entry.getKey();
            Set<Integer> usedPorts = entry.getValue();

            Future future = executor.submit(() -> {
                List<ApplierTbl> applierWithSameIp = findAllApplierTblsWithSameIp(containerIp);
                applierWithSameIp.forEach(applierTbl -> usedPorts.add(applierTbl.getPort()));
            });

            futures.add(future);
        }

        for (Future future : futures) {
            try {
                future.get();
            } catch (InterruptedException ignore) {
                logger.info("[getUsedPortsOfAppliercontainers] failed by ignored InterruptedException", ignore);
            } catch (ExecutionException e) {
                for(Future futureToCancel : futures) {
                    if(!futureToCancel.isDone() || !futureToCancel.isCancelled()) {
                        futureToCancel.cancel(true);
                    }
                }
                return getUsedPortsOfAppliercontainers(appliercontainerTbls);
            }
        }

        return ipUsedPortsMap;
    }

}
