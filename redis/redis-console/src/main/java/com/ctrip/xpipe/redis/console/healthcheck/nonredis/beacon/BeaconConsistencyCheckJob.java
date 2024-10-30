package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.*;
import java.util.stream.Collectors;

public class BeaconConsistencyCheckJob extends AbstractCommand<Void> {

    private Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg;
    private Map<Long, List<MonitorService>> services;
    private MetaCache metaCache;
    private ConsoleCommonConfig config;
    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    public BeaconConsistencyCheckJob(Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg
    , Map<Long, List<MonitorService>> services, MetaCache metaCache, ConsoleCommonConfig config) {
        this.clustersByBeaconSystemOrg = clustersByBeaconSystemOrg;
        this.services = services;
        this.metaCache = metaCache;
        this.config = config;
    }
    @Override
    protected void doExecute() throws Throwable {
        try {
            doJob();
            future().setSuccess();
        } catch (Exception e) {
            future().setFailure(e);
        }
    }

    private void doJob() {
        Map<String, Set<String>> allClusters = new HashMap<>();
        clustersByBeaconSystemOrg.forEach(((beaconSystem, clustersByOrg) -> {
            String system = beaconSystem.getSystemName();
            clustersByOrg.forEach((orgId, clusters) -> {
                List<MonitorService> monitorServices = services.get(orgId);
                monitorServices.forEach(ms -> {
                    Map<String, Set<String>> temp = ms.getAllClusterWithDc(system);
                    allClusters.putAll(temp);

                });
            });
        }));

        Set<String> inconsistencyClusters = new HashSet<>();
        Set<String> notfoundCluster = new HashSet<>();

        Map<String, DcMeta> dcs = metaCache.getXpipeMeta().getDcs();
        Set<String> supportZones = config.getBeaconSupportZones();
        for (DcMeta dcMeta : dcs.values()) {
            if(!supportZones.isEmpty() && supportZones.stream().noneMatch(zone -> StringUtil.trimEquals(dcMeta.getZone(), zone, true))) {
                continue;
            }
            Map<String, ClusterMeta> clusterMetaMap = dcMeta.getClusters();
            for (ClusterMeta cluster : clusterMetaMap.values()) {
                String clusterName = cluster.getId();
                if(StringUtil.trimEquals(ClusterType.ONE_WAY.toString(), cluster.getType(), true)) {
                    String dc = cluster.getActiveDc().toUpperCase();
                    if(!allClusters.containsKey(clusterName)) {
                        notfoundCluster.add(clusterName);
                    } else {
                        Set<String> loaclDcSet = new HashSet<>();
                        loaclDcSet.add(dc);
                        Set<String> beaconDcSet = getBeaconClusterDcSet(allClusters, clusterName);
                        if(!beaconDcSet.equals(loaclDcSet)) {
                            inconsistencyClusters.add(clusterName);
                        }
                    }
                } else if(StringUtil.trimEquals(ClusterType.BI_DIRECTION.toString(), cluster.getType(), true)) {
                    if(!allClusters.containsKey(clusterName)) {
                        notfoundCluster.add(clusterName);
                    } else {
                        Set<String> beaconDcSet = getBeaconClusterDcSet(allClusters, clusterName);
                        Set<String> loaclDcSet = Arrays.stream(cluster.getDcs().split(","))
                                .map(String::toUpperCase)
                                .collect(Collectors.toSet());
                        if(!beaconDcSet.equals(loaclDcSet)) {
                            inconsistencyClusters.add(clusterName);
                        }
                    }
                }
            }
        }


        for (String clusterName : inconsistencyClusters) {
            MetricData metricData = getMetricData(clusterName, "INCONSISTENT");
            sendMetricData(metricData);
        }
        for (String clusterName : notfoundCluster) {
            MetricData metricData = getMetricData(clusterName, "NOTFOUND");
            sendMetricData(metricData);
        }

    }

    private  Set<String> getBeaconClusterDcSet(Map<String, Set<String>> allClusters, String clusterName) {
        return  allClusters
                .get(clusterName)
                .stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet());
    }

    private MetricData getMetricData(String clusterId, String consistency) {
        MetricData metricData = new MetricData("beacon.console", null, clusterId, null);
        metricData.setValue(1);
        metricData.setTimestampMilli(System.currentTimeMillis());
        metricData.addTag("consistency", String.valueOf(consistency));
        return metricData;
    }

    private void sendMetricData(MetricData metricData) {
        try {
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (MetricProxyException e) {
            getLogger().error("[sendMetricData]", e);
        }
    }

    @Override
    protected void doReset() {

    }

    @Override
    public String getName() {
        return "[BeaconConsistencyCheckJob]";
    }
}
