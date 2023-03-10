package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.checker.model.*;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;


public class DefaultDcRelationsService implements DcRelationsService {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private ConsoleConfig config;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled ;

    private final AtomicInteger delayPerDistance = new AtomicInteger(2000);
    private final AtomicReference<Map<Set<String>, Integer>> dcsDistance = new AtomicReference<>();
    private final AtomicReference<Map<String, Map<Set<String>, Integer>>> clusterDcsDistance = new AtomicReference<>();
    private final AtomicReference<DcsPriority> dcLevelPriority = new AtomicReference<>();
    private final AtomicReference<Map<String, DcsPriority>> clusterLevelDcPriority = new AtomicReference<>();

    @PostConstruct
    public void start() throws Exception {
        scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    refresh();
                } catch (Throwable th) {
                    try {
                        logger.error("refresh dc priority failed", th);
                    } catch (Throwable innerTh) {
                        //ignore
                    }
                }
            }
        }, 0, 60, TimeUnit.SECONDS);
    }

    @Override
    public List<String> getTargetDcsByPriority(String clusterName, String downDc, List<String> availableDcs) {
        // cluster level first
        DcPriority downDcPriority = getClusterLevelDcPriority(clusterName, downDc);

        // dc level next if no cluster level config
        if (downDcPriority == null) downDcPriority = getDcLevelPriority(downDc);

        if (downDcPriority == null) return availableDcs;

        return getTargetDcs(downDcPriority, availableDcs);
    }

    @Override
    public Set<String> getExcludedDcsForBiCluster(String clusterName, Set<String> downDcs, Set<String> availableDcs) {
        if (availableDcs.isEmpty())
            return new HashSet<>();

        Set<String> downDcsToUpperCase = downDcs.stream().map(String::toUpperCase).collect(Collectors.toSet());
        Set<String> availableDcsToUpperCase = availableDcs.stream().map(String::toUpperCase).collect(Collectors.toSet());
        Map<String, DcsPriority> clusterDcsPriorityMap = clusterLevelDcPriority.get();
        if (clusterDcsPriorityMap == null || clusterDcsPriorityMap.get(clusterName.toLowerCase()) == null) {
            return new HashSet<>();
        }

        Set<String> ignoreDcs = new HashSet<>();
        for (String downDc : downDcsToUpperCase) {
            ignoreDcs.addAll(getClusterIgnoreDcs(clusterName, downDc));
        }
        availableDcsToUpperCase.removeAll(ignoreDcs);
        if (availableDcsToUpperCase.isEmpty())
            return new HashSet<>();

        return doGetExcludedDcs(downDcsToUpperCase, availableDcsToUpperCase);
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
        DcPriority downDcPriority = getClusterLevelDcPriority(clusterName, downDc);

        if (downDcPriority != null) {
            ignoreDcs.addAll(getIgnoreDcs(downDcPriority));
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
        Map<String, Map<Set<String>, Integer>> clusterDcsDistanceMap = clusterDcsDistance.get();
        if (clusterDcsDistanceMap != null) {
            Map<Set<String>, Integer> dcsDistanceMap = clusterDcsDistanceMap.get(clusterName.toLowerCase());
            if (dcsDistanceMap != null) {
                Integer distance = dcsDistanceMap.get(Sets.newHashSet(fromDc.toUpperCase(), toDc.toUpperCase()));
                if (distance != null) {
                    return delayPerDistance.get() * distance;
                }
            }
        }
        return null;
    }

    @Override
    public Integer getDcsDelay(String fromDc, String toDc) {
        Map<Set<String>, Integer> dcsDistanceMap = dcsDistance.get();
        if (dcsDistanceMap != null) {
            Integer distance = dcsDistanceMap.get(Sets.newHashSet(fromDc.toUpperCase(), toDc.toUpperCase()));
            if (distance != null) {
                return delayPerDistance.get() * distance;
            }
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

    private List<String> getTargetDcs(DcPriority dcPriority, List<String> availableDcs) {
        Map<Integer, List<String>> priority2Dcs = dcPriority.getPriority2Dcs();
        Map<Integer, List<String>> priority2AvailableDcs = new TreeMap<>();
        for (int priority : priority2Dcs.keySet()) {
            if (priority > 0) priority2AvailableDcs.put(priority, priority2Dcs.get(priority));
        }

        if (priority2AvailableDcs.isEmpty())
            return new ArrayList<>();

        List<String> availableDcsCopy = availableDcs.stream().map(String::toUpperCase).collect(Collectors.toList());

        for (int priority : priority2AvailableDcs.keySet()) {
            List<String> copy = Lists.newArrayList(priority2AvailableDcs.get(priority));
            copy.retainAll(availableDcsCopy);
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

    void refresh() throws Exception {
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;

        transaction.logTransaction("dc.relations", "refresh", new Task() {
            @Override
            public void go() throws Exception {
                DcsRelations dcsRelations = config.getDcsRelations();
                delayPerDistance.set(dcsRelations.getDelayPerDistance());
                dcsDistance.set(buildDcsDistance(dcsRelations.getDcLevel()));
                clusterDcsDistance.set(buildClusterDcsDistance(dcsRelations.getClusterLevel()));
                clusterLevelDcPriority.set(buildClusterLevelDcPriority(dcsRelations.getClusterLevel()));
                dcLevelPriority.set(buildDcLevelPriority(dcsRelations.getDcLevel()));
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("delayPerDistance", delayPerDistance.get());
                transactionData.put("dcsDistance", dcsDistance.get());
                transactionData.put("clusterDcsDistance", clusterDcsDistance.get());
                transactionData.put("clusterLevelDcPriority", clusterLevelDcPriority.get());
                transactionData.put("dcLevelPriority", dcLevelPriority.get());
                return transactionData;
            }
        });

    }

    private Map<Set<String>, Integer> buildDcsDistance(List<DcRelation> dcRelations) {
        if (dcRelations == null) return null;
        Map<Set<String>, Integer> dcsDistanceMap = new HashMap<>();
        dcRelations.forEach(dcDistance -> {
            Set<String> dcSet = Sets.newHashSet(dcDistance.getDcs().toUpperCase().split("\\s*,\\s*"));
            dcsDistanceMap.put(dcSet, dcDistance.getDistance());
        });
        return dcsDistanceMap;
    }

    private Map<String, Map<Set<String>, Integer>> buildClusterDcsDistance(List<ClusterDcRelations> relations) {
        if (relations == null) return null;
        Map<String, Map<Set<String>, Integer>> clusterDcsDistance = new HashMap<>();
        relations.forEach(clusterDcRelations -> {
            Map<Set<String>, Integer> dcsDistanceMap = buildDcsDistance(clusterDcRelations.getRelations());
            clusterDcsDistance.put(clusterDcRelations.getClusterName().toLowerCase(), dcsDistanceMap);
        });
        return clusterDcsDistance;
    }

    private Map<String, DcsPriority> buildClusterLevelDcPriority(List<ClusterDcRelations> clusterLevel) {
        if (clusterLevel == null) return null;

        Map<String, DcsPriority> clusterLevelDcPriority = new HashMap<>();
        for (ClusterDcRelations clusterDcRelations : clusterLevel) {
            String clusterName = clusterDcRelations.getClusterName();
            List<DcRelation> dcRelations = clusterDcRelations.getRelations();

            DcsPriority dcsPriority = buildDcLevelPriority(dcRelations);
            clusterLevelDcPriority.put(clusterName.toLowerCase(), dcsPriority);
        }
        return clusterLevelDcPriority;
    }

    private DcsPriority buildDcLevelPriority(List<DcRelation> dcRelations) {
        if (dcRelations == null) return null;

        Map<String, DcPriority> dcPriorityMap = new HashMap<>();
        for (DcRelation dcRelation : dcRelations) {
            String dcsStr = dcRelation.getDcs();
            int distance = dcRelation.getDistance();

            String[] dcs = dcsStr.split("\\s*,\\s*");
            buildDcPriority(dcPriorityMap, dcs[0].toUpperCase(), dcs[1].toUpperCase(), distance);
            buildDcPriority(dcPriorityMap, dcs[1].toUpperCase(), dcs[0].toUpperCase(), distance);
        }

        return new DcsPriority().setDcPriorityMap(dcPriorityMap);
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
    Map<Set<String>, Integer> getDcsDistance() {
        return dcsDistance.get();
    }

    @VisibleForTesting
    Map<String, Map<Set<String>, Integer>> getClusterDcsDistance() {
        return clusterDcsDistance.get();
    }
}
