package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.InstanceNode;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperContainerDetailInfo;
import com.ctrip.xpipe.tuple.Pair;
import org.unidal.tuple.Triple;

import java.util.*;
import java.util.stream.Collectors;

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
            List<InstanceNode> currentKeeperInstances = getAllKeeperInstances(currentDetailInfo.get(id).getKeeperInstances());
            List<InstanceNode> futureKeeperInstances = getAllKeeperInstances(futureDetailInfo.get(id).getKeeperInstances());

            Pair<List<InstanceNode>, List<Pair<InstanceNode, InstanceNode>>> subResult = sub(futureKeeperInstances, currentKeeperInstances);
            List<InstanceNode> tAdded = subResult.getKey();
            added.addAll(tAdded);

            List<Pair<InstanceNode, InstanceNode>> modified = subResult.getValue();
            compareConfigConfig(modified);

            List<InstanceNode> tRemoved = sub(currentKeeperInstances, futureKeeperInstances).getKey();
            removed.addAll(tRemoved);
        });
    }

    private List<InstanceNode> getAllKeeperInstances(List<KeeperMeta> keeperInstances) {
        List<InstanceNode> result = new LinkedList<>();
        result.addAll(keeperInstances);
        return result;
    }

    @Override
    public String idDesc() {
        return current.getId();
    }

    private Map<Long, KeeperContainerDetailInfo> getAllKeeperContainerDetailInfoFromDcMeta(DcMeta dcMeta, DcMeta allDcMeta) {
        Map<Long, KeeperContainerDetailInfo> map = dcMeta.getKeeperContainers().stream()
                .collect(Collectors.toMap(KeeperContainerMeta::getId,
                        keeperContainerMeta -> new KeeperContainerDetailInfo(keeperContainerMeta, new ArrayList<KeeperMeta>())));
        allDcMeta.getClusters().values().forEach(clusterMeta -> {
            clusterMeta.getAllShards().values().forEach(shardMeta -> {
                shardMeta.getKeepers().forEach(keeperMeta -> {
                    if (map.containsKey(keeperMeta.getKeeperContainerId()))
                        map.get(keeperMeta.getKeeperContainerId()).getKeeperInstances().add(keeperMeta);
                });
            });
        });

        return map;
    }
}
