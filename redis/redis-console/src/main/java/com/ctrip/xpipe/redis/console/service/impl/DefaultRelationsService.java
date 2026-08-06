package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.RelationsService;
import com.ctrip.xpipe.redis.checker.model.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;


public class DefaultRelationsService implements RelationsService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private MetaCache metaCache;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled ;

    private final AtomicInteger delayPerDistance = new AtomicInteger(2000);
    private final AtomicReference<String> relationsConfig = new AtomicReference<>();
    private final AtomicReference<Map<Pair<String, String>, Integer>> dcsDistance = new AtomicReference<>();
    private final AtomicReference<Map<String, Map<Pair<String, String>, Integer>>> clusterDcsDistance = new AtomicReference<>();
    private final AtomicReference<Map<Pair<String, String>, Integer>> regionDistance = new AtomicReference<>();
    private final AtomicReference<DcsPriority> dcLevelPriority = new AtomicReference<>();
    private final AtomicReference<Map<String, DcsPriority>> clusterLevelDcPriority = new AtomicReference<>();

    private final Map<Pair<String, Set<String>>, List<String>> dcLevelTargetDcsCache = new ConcurrentHashMap<>();

    @PostConstruct
    public void start() throws Exception {
        refresh();
        scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                refresh();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    List<String> getTargetDcsByPriority(String clusterName, String downDc, List<String> availableDcs) {
        boolean dcLevel = false;

        String localClusterName = clusterName.toLowerCase();
        String localDownDc = downDc.toUpperCase();
        Set<String> localAvailableDcs = availableDcs.stream().map(String::toUpperCase).collect(Collectors.toSet());

        // cluster level first
        DcPriority downDcPriority = getClusterLevelDcPriority(localClusterName, localDownDc);

        // dc level next if no cluster level config
        if (downDcPriority == null) {
            downDcPriority = getDcLevelPriority(localDownDc);
            dcLevel = true;
        }

        if (downDcPriority == null) return availableDcs;

        if (dcLevel) {
            DcPriority finalDownDcPriority = downDcPriority;
            return MapUtils.getOrCreate(dcLevelTargetDcsCache, new Pair<>(localDownDc, localAvailableDcs), new ObjectFactory<List<String>>() {
                @Override
                public List<String> create() {
                    return getTargetDcs(finalDownDcPriority, localAvailableDcs);
                }
            });
        }

        return getTargetDcs(downDcPriority, localAvailableDcs);
    }

    @Override
    public String getClusterTargetDcByPriority(long clusterId, String clusterName, String downDc, List<String> availableDcs) {
        if (availableDcs == null || availableDcs.isEmpty()) return null;

        List<String> targetDcs = getTargetDcsByPriority(clusterName, downDc, availableDcs);
        if (targetDcs.isEmpty()) return null;

        int dcCount = targetDcs.size();
        int index = (int) (clusterId % dcCount);
        return targetDcs.get(index);
    }

    @Override
    public Set<String> getExcludedDcsForBiCluster(String clusterName, Set<String> downDcs, Set<String> availableDcs) {
        if (availableDcs.isEmpty())
            return new HashSet<>();

        Set<String> excludedDcs = new HashSet<>();
        Set<String> downDcsToUpperCase = downDcs.stream().map(String::toUpperCase).collect(Collectors.toSet());
        Set<String> availableDcsToUpperCase = availableDcs.stream().map(String::toUpperCase).collect(Collectors.toSet());

        for (String downDc: downDcsToUpperCase) {
            Set<String> reachableDcs = new HashSet<>(availableDcsToUpperCase);
            Set<String> unreachableDcs = getClusterIgnoreDcs(clusterName, downDc);
            reachableDcs.removeAll(unreachableDcs);
            if (reachableDcs.isEmpty()) {
                // no other dc for downgrade, do not exclude down dc
                continue;
            }
            excludedDcs.add(downDc);
        }

        return excludedDcs;
    }

    Set<String> doGetExcludedDcs(Set<String> downDcs, Set<String> availableDcs) {
        Set<String> excludedDcs = new HashSet<>();
        String targetDc = availableDcs.iterator().next();
        availableDcs.remove(targetDc);
        excludedDcs.addAll(availableDcs);
        excludedDcs.addAll(downDcs);
        return excludedDcs;
    }

    Set<String> getClusterIgnoreDcs(String clusterName, String downDc) {
        Set<String> ignoreDcs = new HashSet<>();
        DcPriority downDcClusterPriority = getClusterLevelDcPriority(clusterName, downDc);

        if (downDcClusterPriority != null) {
            ignoreDcs.addAll(getIgnoreDcs(downDcClusterPriority));
        } else {
            DcPriority downDcPriority = getDcLevelPriority(downDc);
            if (null != downDcPriority) {
                ignoreDcs.addAll(getIgnoreDcs(downDcPriority));
            }
        }

        return ignoreDcs;
    }

    private Set<String> getIgnoreDcs(DcPriority dcPriority) {
        Map<Integer, List<String>> priority2Dcs = dcPriority.getPriority2Dcs();
        return priority2Dcs.entrySet().stream()
                .filter(entry -> entry.getKey() < 0)
                .map(Map.Entry::getValue).flatMap(Collection::stream).collect(Collectors.toSet());
    }

    @Override
    public Integer getClusterDcsDelay(String clusterName, String fromDc, String toDc) {
        Map<String, Map<Pair<String, String>, Integer>> clusterDcsDistanceMap = clusterDcsDistance.get();
        if (clusterDcsDistanceMap != null) {
            Map<Pair<String, String>, Integer> dcsDistanceMap = clusterDcsDistanceMap.get(clusterName.toLowerCase());
            Integer distance = getDirectionalDistance(dcsDistanceMap, fromDc, toDc);
            if (distance != null) {
                return delayPerDistance.get() * distance;
            }
        }
        return null;
    }

    @Override
    public Integer getDcsDelay(String fromDc, String toDc) {
        Integer distance = getDirectionalDistance(dcsDistance.get(), fromDc, toDc);
        if (distance != null) {
            return delayPerDistance.get() * distance;
        }
        return null;
    }

    private Integer getDirectionalDistance(Map<Pair<String, String>, Integer> distanceMap, String fromDc, String toDc) {
        if (distanceMap == null) return null;
        return distanceMap.get(new Pair<>(fromDc.toUpperCase(), toDc.toUpperCase()));
    }

    @Override
    public boolean isReachableRegion(String dc1, String dc2) {
        String zone1 = metaCache.getDcZone(dc1);
        String zone2 = metaCache.getDcZone(dc2);
        if (zone1 == null || zone2 == null) {
            return false;
        }
        if (zone1.equalsIgnoreCase(zone2)) {
            return true;
        }
        Integer distance = getRegionDistance(zone1, zone2);
        return distance != null && distance != -1;
    }

    @Override
    public Integer getRegionDelay(String fromDc, String toDc) {
        String zone1 = metaCache.getDcZone(fromDc);
        String zone2 = metaCache.getDcZone(toDc);
        if (zone1 == null || zone2 == null || zone1.equalsIgnoreCase(zone2)) {
            return null;
        }
        Integer distance = getRegionDistance(zone1, zone2);
        if (distance != null && distance > 0) {
            return delayPerDistance.get() * distance;
        }
        return null;
    }

    private DcPriority getClusterLevelDcPriority(String clusterName, String downDc) {
        Map<String, DcsPriority> clusterDcsPriorityMap = clusterLevelDcPriority.get();
        if (clusterDcsPriorityMap == null)
            return null;

        DcsPriority clusterDcsPriority = clusterDcsPriorityMap.get(clusterName.toLowerCase());
        if (clusterDcsPriority != null) {
            return clusterDcsPriority.getDcPriority(downDc.toUpperCase());
        }

        return null;
    }

    private DcPriority getDcLevelPriority(String downDc) {
        DcsPriority dcsPriority = dcLevelPriority.get();
        if (dcsPriority == null) return null;

        return dcsPriority.getDcPriority(downDc.toUpperCase());
    }

    List<String> getTargetDcs(DcPriority dcPriority, Set<String> availableDcs) {
        Map<Integer, List<String>> priority2Dcs = dcPriority.getPriority2Dcs();
        Map<Integer, List<String>> priority2AvailableDcs = new TreeMap<>();
        for (int priority : priority2Dcs.keySet()) {
            if (priority > 0) priority2AvailableDcs.put(priority, priority2Dcs.get(priority));
        }

        if (priority2AvailableDcs.isEmpty())
            return new ArrayList<>();

        for (int priority : priority2AvailableDcs.keySet()) {
            List<String> copy = Lists.newArrayList(priority2AvailableDcs.get(priority));
            copy.retainAll(availableDcs);
            if (!copy.isEmpty())
                return copy;
        }

        return new ArrayList<>();
    }



    private void buildDcPriority(Map<String, DcPriority> dcPriorityMap, String fromDc, String toDc, int distance) {
        DcPriority dcPriority = dcPriorityMap.getOrDefault(fromDc, new DcPriority().setDc(fromDc));
        dcPriority.addPriorityAndDc(distance, toDc);
        dcPriorityMap.put(fromDc, dcPriority);
    }

    void refresh() {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;

        try {
            transaction.logTransaction("dc.relations", "refresh", new Task() {
                @Override
                public void go() throws Exception {
                    String remoteRelationsConfig = config.getRelations();
                    if (relationsConfig.get() == null || !remoteRelationsConfig.equalsIgnoreCase(relationsConfig.get())) {
                        Relations relations = JsonCodec.INSTANCE.decode(remoteRelationsConfig, Relations.class);
                        relationsConfig.set(remoteRelationsConfig);
                        delayPerDistance.set(relations.getDelayPerDistance());
                        dcsDistance.set(buildDcsDistance(relations.getDcLevel()));
                        clusterDcsDistance.set(buildClusterDcsDistance(relations.getClusterLevel()));
                        regionDistance.set(buildRegionDistance(relations.getRegionLevel()));
                        clusterLevelDcPriority.set(buildClusterLevelDcPriority(relations.getClusterLevel()));
                        dcLevelPriority.set(buildDcLevelPriority(relations.getDcLevel()));
                        dcLevelTargetDcsCache.clear();
                    }
                }

                @Override
                public Map<String, Object> getData() {
                    Map<String, Object> transactionData = new HashMap<>();
                    transactionData.put("delayPerDistance", delayPerDistance.get());
                    transactionData.put("dcsDistance", dcsDistance.get());
                    transactionData.put("clusterDcsDistance", clusterDcsDistance.get());
                    transactionData.put("regionDistance", regionDistance.get());
                    transactionData.put("clusterLevelDcPriority", clusterLevelDcPriority.get());
                    transactionData.put("dcLevelPriority", dcLevelPriority.get());
                    return transactionData;
                }
            });
        } catch (Throwable th) {
            logger.error("refresh dc priority failed", th);
        }

    }

    private Map<Pair<String, String>, Integer> buildDcsDistance(List<Relation> relations) {
        return buildDirectionalDistance(relations);
    }

    private Map<String, Map<Pair<String, String>, Integer>> buildClusterDcsDistance(List<ClusterRelations> relations) {
        if (relations == null) return null;
        Map<String, Map<Pair<String, String>, Integer>> clusterDcsDistance = new HashMap<>();
        relations.forEach(clusterRelations -> {
            Map<Pair<String, String>, Integer> dcsDistanceMap = buildDirectionalDistance(clusterRelations.getRelations());
            clusterDcsDistance.put(clusterRelations.getClusterName().toLowerCase(), dcsDistanceMap);
        });
        return clusterDcsDistance;
    }

    private Map<Pair<String, String>, Integer> buildRegionDistance(List<Relation> regionLevel) {
        return buildDirectionalDistance(regionLevel);
    }

    private Map<Pair<String, String>, Integer> buildDirectionalDistance(List<Relation> relations) {
        if (relations == null) return null;
        Map<Pair<String, String>, Integer> distanceMap = new HashMap<>();
        relations.forEach(relation -> {
            String src = relation.getSrc().toUpperCase();
            String dst = relation.getDst().toUpperCase();
            distanceMap.put(new Pair<>(src, dst), relation.getDistance());
            if (relation.isBiDirection()) {
                distanceMap.put(new Pair<>(dst, src), relation.getDistance());
            }
        });
        return distanceMap;
    }

    private Map<String, DcsPriority> buildClusterLevelDcPriority(List<ClusterRelations> clusterLevel) {
        if (clusterLevel == null) return null;

        Map<String, DcsPriority> clusterLevelDcPriority = new HashMap<>();
        for (ClusterRelations clusterRelations : clusterLevel) {
            String clusterName = clusterRelations.getClusterName();
            List<Relation> relations = clusterRelations.getRelations();

            DcsPriority dcsPriority = buildDcLevelPriority(relations);
            clusterLevelDcPriority.put(clusterName.toLowerCase(), dcsPriority);
        }
        return clusterLevelDcPriority;
    }

    private DcsPriority buildDcLevelPriority(List<Relation> relations) {
        if (relations == null) return null;

        Map<String, DcPriority> dcPriorityMap = new HashMap<>();
        for (Relation relation : relations) {
            String src = relation.getSrc().toUpperCase();
            String dst = relation.getDst().toUpperCase();
            int distance = relation.getDistance();

            buildDcPriority(dcPriorityMap, src, dst, distance);
            if (relation.isBiDirection()) {
                buildDcPriority(dcPriorityMap, dst, src, distance);
            }
        }

        return new DcsPriority().setDcPriorityMap(dcPriorityMap);
    }

    @VisibleForTesting
    Integer getRegionDistance(String fromRegion, String toRegion) {
        Map<Pair<String, String>, Integer> regionDistanceMap = regionDistance.get();
        if (regionDistanceMap == null) return null;
        return regionDistanceMap.get(new Pair<>(fromRegion.toUpperCase(), toRegion.toUpperCase()));
    }

    @VisibleForTesting
    DcsPriority getDcLevelPriority() {
        return dcLevelPriority.get();
    }

    @VisibleForTesting
    Map<String, DcsPriority> getClusterLevelDcPriority() {
        return clusterLevelDcPriority.get();
    }

    @VisibleForTesting
    Integer getDelayPerDistance() {
        return delayPerDistance.get();
    }

    @VisibleForTesting
    Map<Pair<String, String>, Integer> getDcsDistance() {
        return dcsDistance.get();
    }

    @VisibleForTesting
    Map<String, Map<Pair<String, String>, Integer>> getClusterDcsDistance() {
        return clusterDcsDistance.get();
    }
}
