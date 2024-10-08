package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DefaultMonitorClusterManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMonitorClusterManager.class);

    private final MetaCache metaCache;
    private final int virtualServiceNum;
    private final List<MonitorService> services = new ArrayList<>();
    private final SortedMap<Integer, MonitorService> ring = new TreeMap<>();
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(2,
        XpipeThreadFactory.create(getClass().getSimpleName()));

    public DefaultMonitorClusterManager(MetaCache metaCache, int virtualServiceNum, List<MonitorService> services,
        long checkInterval) {
        this.metaCache = metaCache;
        this.virtualServiceNum = virtualServiceNum;
        for (MonitorService service : services) {
            this.addService(service);
        }

        if (null == metaCache || null == metaCache.getXpipeMeta()) {
            logger.info("not checker mode, don't execute sync ring task");
            return;
        }
        Random random = new Random();
        long randomDelay = 6L + random.nextInt(4);
        executor.scheduleWithFixedDelay(new ClustersRingSyncTask(), checkInterval + randomDelay,
            checkInterval + 100L, TimeUnit.SECONDS);
    }

    public void addService(MonitorService service) {
        for (int i = 0; i < virtualServiceNum * service.getWeight() / MonitorService.MAX_WEIGHT; i++) {
            this.addVirtualService(service, i);
        }
        this.services.add(service);
    }

    public void removeService(MonitorService service) {
        for (int i = 0; i < virtualServiceNum; i++) {
            this.removeVirtualService(service, i);
        }
        this.services.remove(service);
    }

    public MonitorService getService(String clusterName) {
        if (ring.isEmpty()) {
            return null;
        }
        int hash = hash(clusterName);
        if (!ring.containsKey(hash)) {
            SortedMap<Integer, MonitorService> tailMap = ring.tailMap(hash);
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    public List<MonitorService> getServices() {
        return this.services;
    }

    public void updateServiceWeight(MonitorService service, int newWeight) {
        if (!this.services.contains(service)) {
            return;
        }
        int oldWeight = service.getWeight();
        newWeight = Math.max(newWeight, MonitorService.MIN_WEIGHT);
        newWeight = Math.min(newWeight, MonitorService.MAX_WEIGHT);
        service.setWeight(newWeight);

        int startNodeIndex = virtualServiceNum * oldWeight / MonitorService.MAX_WEIGHT;
        int endNodeIndex = virtualServiceNum * newWeight / MonitorService.MAX_WEIGHT;
        if (oldWeight < newWeight) {
            for (int i = startNodeIndex; i < endNodeIndex; i++) {
                this.addVirtualService(service, i);
            }
        } else if (oldWeight > newWeight) {
            for (int i = startNodeIndex - 1; i >= endNodeIndex; i--) {
                this.removeVirtualService(service, i);
            }
        }
    }

    private void addVirtualService(MonitorService service, int virtualIndex) {
        String virtualService = service.getName() + "-VS" + virtualIndex;
        int hash = hash(virtualService);
        ring.put(hash, service);
    }

    private void removeVirtualService(MonitorService service, int virtualIndex) {
        String virtualService = service.getName() + "-VS" + virtualIndex;
        int hash = hash(virtualService);
        ring.remove(hash);
    }


    // FNV1_32_HASH
    private int hash(String key) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < key.length(); i++) {
            hash = (hash ^ key.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return hash;
    }



    private class ClustersRingSyncTask extends AbstractExceptionLogTask {

        private final String onewaySystem = BeaconSystem.XPIPE_ONE_WAY.getSystemName();
        private final String biSystem = BeaconSystem.XPIPE_BI_DIRECTION.getSystemName();

        @Override
        protected void doRun() throws Exception {
            Set<String> manageClusters = new HashSet<>();
            XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
            Map<String, DcMeta> map = xpipeMeta.getDcs();
            for (DcMeta dcMeta : map.values()) {
                manageClusters.addAll(dcMeta.getClusters().keySet());
            }
            syncMonitors(manageClusters, onewaySystem);
            syncMonitors(manageClusters, biSystem);
        }

        private void syncMonitors(Set<String> manageClusters, String system) {
            Map<MonitorService, Set<String>> serviceClustersMap = new HashMap<>();
            services.forEach(service -> {
                try {
                    Set<String> clusters = service.fetchAllClusters(system);
                    serviceClustersMap.put(service, clusters);
                } catch (XpipeRuntimeException e) {
                    logger.warn("get xpipe clusters fail, service: {}, system: {}, error: {}",
                        service, system, e);
                }
            });

            serviceClustersMap.forEach((service, clusters) -> {
                clusters.forEach(cluster -> {
                    MonitorService rightService = getService(cluster);
                    if (!Objects.equals(service, rightService)) {
                        if (manageClusters.contains(cluster)) {
                            Set<String> actualClusters = serviceClustersMap.get(rightService);
                            if (actualClusters != null && actualClusters.contains(cluster)) {
                                try {
                                    service.unregisterCluster(system, cluster);
                                } catch (XpipeRuntimeException e) {
                                    logger.warn("unregister fail， service: {}, system: {}, cluster: {},"
                                        + " error: {}", service, system, cluster, e);
                                }
                            } else {
                                logger.error("service: {} register a wrong cluster: {}, need unregister soon",
                                    service, cluster);
                            }
                        } else {
                            logger.warn("service: {} may register a wrong cluster: {}", service, cluster);
                        }
                    }
                });
            });
        }

        @Override
        protected Logger getLogger() {
            return logger;
        }
    }

    @VisibleForTesting
    public void startSyncRingTaskNow() {
        this.executor.schedule(new ClustersRingSyncTask(), 5, TimeUnit.MILLISECONDS);
    }

    @VisibleForTesting
    protected SortedMap<Integer, MonitorService> getRing() {
        return ring;
    }

}
