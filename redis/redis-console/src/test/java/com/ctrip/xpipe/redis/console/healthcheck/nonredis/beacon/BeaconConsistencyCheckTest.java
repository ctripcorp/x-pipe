package com.ctrip.xpipe.redis.console.healthcheck.nonredis.beacon;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.ctrip.xpipe.redis.console.migration.auto.DefaultMonitorManager;
import com.ctrip.xpipe.redis.console.resources.DefaultMetaCache;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.meta.impl.DefaultXpipeMetaManager;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class BeaconConsistencyCheckTest extends AbstractConsoleTest {

    @Mock
    ConsoleCommonConfig config;
    @Mock
    ConsoleConfig consoleConfig;
    @Mock
    MonitorService monitorService;
    @Mock
    MetricProxy metricProxy;

    private BeaconConsistencyCheckJob consistencyCheckJob;

    private Map<Long, List<MonitorService>> buildServices() {
        HashMap result = new HashMap<>();
        result.put(1L, Collections.singletonList(monitorService));
        return result;
    }

    private ClusterMeta buildClusterMeta() {
        ClusterMeta clusterMeta = new ClusterMeta("oneway_test");
        clusterMeta.setType(ClusterType.ONE_WAY.toString());
        clusterMeta.setActiveDc("AWS");
        clusterMeta.setDcs("AWS,PTJQ");
        clusterMeta.setOrgId(1);
        return clusterMeta;
    }

    private ClusterMeta buildBiClusterMeta() {
        ClusterMeta clusterMeta = new ClusterMeta("bi_test");
        clusterMeta.setType(ClusterType.BI_DIRECTION.toString());
        clusterMeta.setDcs("AWS,PTJQ");
        clusterMeta.setOrgId(1);
        return clusterMeta;
    }

    private ClusterMeta buildOneWaySHA() {
        ClusterMeta clusterMeta = new ClusterMeta("oneway_sha");
        clusterMeta.setType(ClusterType.ONE_WAY.toString());
        clusterMeta.setActiveDc("PTJQ");
        clusterMeta.setDcs("AWS,PTJQ");
        clusterMeta.setOrgId(1);
        return clusterMeta;
    }

    private MetaCache buildMetaCache() {
        ClusterMeta oneWay = buildClusterMeta();
        ClusterMeta bi = buildBiClusterMeta();
        ClusterMeta oneWaySHA = buildOneWaySHA();

        XpipeMeta currentMeta = new XpipeMeta();
        DcMeta jqMeta = new DcMeta("PTJQ");
        jqMeta.setZone("SHA");
        jqMeta.addCluster(oneWay);
        jqMeta.addCluster(bi);
        jqMeta.addCluster(oneWaySHA);

        DcMeta awsMeta = new DcMeta("AWS");
        awsMeta.setZone("AWS");
        awsMeta.addCluster(oneWay);
        awsMeta.addCluster(bi);
        awsMeta.addCluster(oneWaySHA);

        currentMeta.addDc(jqMeta)
                .addDc(awsMeta);
        DefaultMetaCache metaCache = new DefaultMetaCache();
        Pair<XpipeMeta, XpipeMetaManager> meta = new Pair<>(currentMeta, new DefaultXpipeMetaManager(currentMeta));
        metaCache.setMeta(meta);
        return metaCache;
    }

    @Before
    public void beforeBeaconConsistencyCheckTest() {

        Mockito.when(config.getBeaconSupportZones()).thenReturn(Stream.of("SHA").collect(Collectors.toSet()));
        Mockito.when(consoleConfig.getServerMode()).thenReturn("CONSOLE");
        Mockito.when(consoleConfig.getClusterHealthCheckInterval()).thenReturn(10000);
        List<BeaconOrgRoute> routes = new ArrayList<>();
        routes.add(new BeaconOrgRoute(1L, new ArrayList<>(), 1));
        Mockito.when(consoleConfig.getBeaconOrgRoutes()).thenReturn(routes);

        DefaultMonitorManager defaultMonitorManager = new DefaultMonitorManager(buildMetaCache(), consoleConfig, config);

        consistencyCheckJob = new BeaconConsistencyCheckJob(defaultMonitorManager.clustersByBeaconSystemOrg(),
                buildServices(),
                buildMetaCache(),
                config);

    }

    @Test
    public void testCheck() throws Throwable {
        consistencyCheckJob.setMetricProxy(metricProxy);
        HashMap<String, Set<String>> oneWays = new HashMap<>();
        oneWays.put("oneway_sha", Collections.singleton("PTJQ"));
        Mockito.when(monitorService.getAllClusterWithDc("xpipe")).thenReturn(oneWays);
        HashMap<String, Set<String>> allCluster = new HashMap<>();
        allCluster.put("bi_test", Collections.singletonList("PTJQ").stream().collect(Collectors.toSet()));

        Mockito.when(monitorService.getAllClusterWithDc("xpipe-bi")).thenReturn(allCluster);
        consistencyCheckJob.doExecute();

        verify(metricProxy, times(1))
                .writeBinMultiDataPoint(
                        argThat(new ArgumentMatcher<MetricData>() {
                            @Override
                            public boolean matches(MetricData argument) {
                                return argument.getTags().get("consistency").equals("CONSISTENT");
                            }
                        }));

    }

    @Test
    public void testCheckInConsistency() throws Throwable {
        consistencyCheckJob.setMetricProxy(metricProxy);
        HashMap<String, Set<String>> oneWays = new HashMap<>();
        oneWays.put("oneway_sha", Collections.singleton("PTJQ"));
        Mockito.when(monitorService.getAllClusterWithDc("xpipe")).thenReturn(oneWays);
        HashMap<String, Set<String>> allCluster = new HashMap<>();
        allCluster.put("bi_test", Arrays.asList("PTJQ", "AWS").stream().collect(Collectors.toSet()));

        Mockito.when(monitorService.getAllClusterWithDc("xpipe-bi")).thenReturn(allCluster);
        consistencyCheckJob.doExecute();

        verify(metricProxy, times(1))
                .writeBinMultiDataPoint(
                        argThat(new ArgumentMatcher<MetricData>() {
                            @Override
                            public boolean matches(MetricData argument) {
                                return argument.getTags().get("consistency").equals("INCONSISTENT")
                                        && argument.getClusterName().equals("bi_test");
                            }
                        }));
    }

    @Test
    public void testCheckInNotFound() throws Throwable {
        consistencyCheckJob.setMetricProxy(metricProxy);
        HashMap<String, Set<String>> oneWays = new HashMap<>();
        Mockito.when(monitorService.getAllClusterWithDc("xpipe")).thenReturn(oneWays);
        HashMap<String, Set<String>> allCluster = new HashMap<>();
        allCluster.put("bi_test", Collections.singletonList("PTJQ").stream().collect(Collectors.toSet()));

        Mockito.when(monitorService.getAllClusterWithDc("xpipe-bi")).thenReturn(allCluster);
        consistencyCheckJob.doExecute();

        verify(metricProxy, times(1))
                .writeBinMultiDataPoint(
                        argThat(new ArgumentMatcher<MetricData>() {
                            @Override
                            public boolean matches(MetricData argument) {
                                return argument.getTags().get("consistency").equals("NOTFOUND")
                                        && argument.getClusterName().equals("oneway_sha");
                            }
                        }));

    }


}
