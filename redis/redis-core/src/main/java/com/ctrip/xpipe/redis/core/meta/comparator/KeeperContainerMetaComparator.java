package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.KeeperContainerDetailInfo;
import com.ctrip.xpipe.tuple.Pair;
import org.unidal.tuple.Triple;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author yu
 */
public class KeeperContainerMetaComparator extends AbstractInstanceNodeComparator {

    private DcMeta current, future, currentAllDcMeta, futureAllDcMeta;

    public KeeperContainerMetaComparator(DcMeta current, DcMeta future, DcMeta currentAllDcMeta, DcMeta futureAllDcMeta) {
        this.current = current;
        this.future = future;
        this.currentAllDcMeta = currentAllDcMeta;
        this.futureAllDcMeta = futureAllDcMeta;
    }

    @Override
    public void compare() {
        Map<Long, KeeperContainerDetailInfo> currentDetailInfo = getAllKeeperContainerDetailInfoFromDcMeta(current, currentAllDcMeta);
        Map<Long, KeeperContainerDetailInfo> futureDetailInfo= getAllKeeperContainerDetailInfoFromDcMeta(future, futureAllDcMeta);

        Triple<Set<Long>, Set<Long>, Set<Long>> result = getDiff(currentDetailInfo.keySet(), futureDetailInfo.keySet());
        result.getFirst().forEach(id -> added.addAll(futureDetailInfo.get(id).getKeeperInstances()));
        result.getLast().forEach(id -> removed.addAll(currentDetailInfo.get(id).getKeeperInstances()));
        result.getMiddle().forEach(id -> {
            compareInstanceNode(getAllKeeperInstances(currentDetailInfo.get(id).getKeeperInstances()),
                                getAllKeeperInstances(futureDetailInfo.get(id).getKeeperInstances()));
        });

        Set<InstanceNode> currentRedises = getAllRedisesFromKeeperDetailInfo(currentDetailInfo);
        Set<InstanceNode> futureRedises = getAllRedisesFromKeeperDetailInfo(futureDetailInfo);
        Triple<Set<InstanceNode>, Set<InstanceNode>, Set<InstanceNode>> redisResult = getDiff(currentRedises, futureRedises);
        redisResult.getFirst().forEach(redis -> added.add(redis));
        redisResult.getLast().forEach(redis -> removed.add(redis));
    }

    private Set<InstanceNode> getAllRedisesFromKeeperDetailInfo(Map<Long, KeeperContainerDetailInfo> currentDetailInfo) {
        Set<InstanceNode> result = new HashSet<>();
        currentDetailInfo.values().forEach(keeperContainerDetailInfo -> {
            result.addAll(keeperContainerDetailInfo.getRedisInstances());
        });
        return result;
    }

    public void compareInstanceNode(List<InstanceNode> current, List<InstanceNode> future) {
        Pair<List<InstanceNode>, List<Pair<InstanceNode, InstanceNode>>> subResult = sub(future, current);
        List<InstanceNode> tAdded = subResult.getKey();
        added.addAll(tAdded);

        List<Pair<InstanceNode, InstanceNode>> modified = subResult.getValue();
        compareConfigConfig(modified);

        List<InstanceNode> tRemoved = sub(current, future).getKey();
        removed.addAll(tRemoved);

    }

    private List<InstanceNode> getAllKeeperInstances(List<KeeperMeta> keeperInstances) {
        List<InstanceNode> result = new LinkedList<>();
        result.addAll(keeperInstances);
        return result;
    }

    private List<InstanceNode> getAllRedisInstances(List<RedisMeta> redisInstances) {
        List<InstanceNode> result = new LinkedList<>();
        result.addAll(redisInstances);
        return result;
    }

    @Override
    public String idDesc() {
        return current.getId();
    }

    public static Map<Long, KeeperContainerDetailInfo> getAllKeeperContainerDetailInfoFromDcMeta(DcMeta dcMeta, DcMeta allDcMeta) {
        Map<Long, KeeperContainerDetailInfo> map = dcMeta.getKeeperContainers().stream()
                .collect(Collectors.toMap(KeeperContainerMeta::getId,
                        keeperContainerMeta -> new KeeperContainerDetailInfo(keeperContainerMeta, new ArrayList<>(), new ArrayList<>())));
        allDcMeta.getClusters().values().forEach(clusterMeta -> {
            for (ShardMeta shardMeta : clusterMeta.getAllShards().values()){
                if (shardMeta.getKeepers() == null || shardMeta.getKeepers().isEmpty()) continue;
                RedisMeta monitorRedis = getMonitorRedisMeta(shardMeta.getRedises());
                shardMeta.getKeepers().forEach(keeperMeta -> {
                    if (map.containsKey(keeperMeta.getKeeperContainerId())) {
                        map.get(keeperMeta.getKeeperContainerId()).getKeeperInstances().add(keeperMeta);
                        if (monitorRedis != null)
                            map.get(keeperMeta.getKeeperContainerId()).getRedisInstances().add(monitorRedis);
                    }
                });
            }
        });

        return map;
    }

    public static RedisMeta getMonitorRedisMeta(List<RedisMeta> redisMetas) {
        if (redisMetas == null || redisMetas.isEmpty()) return null;

        List<RedisMeta> candidates = redisMetas.stream().filter(r -> !r.isMaster())
                                                .sorted((r1, r2) -> (r1.getIp().hashCode() - r2.getIp().hashCode()))
                                                .collect(Collectors.toList());
        return candidates.isEmpty() ? null : candidates.get(0);
    }
}


