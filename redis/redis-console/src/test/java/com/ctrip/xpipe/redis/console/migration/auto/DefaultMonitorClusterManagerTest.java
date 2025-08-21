package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMonitorClusterManagerTest {

    private static final List<String> CLUSTERS1 = Lists.newArrayList("shipXProduct", "SmartTrip_Redis",
        "tour_servicetrack", "bus_data_shift", "MKT_Lat_Finance", "BDAI_FEEDS_DICT_CP11", "CC_trunkvoice_data",
        "TrainTicketPayRedis");
    private static final List<String> CLUSTERS2 = Lists.newArrayList("CORP_BIRoomSort", "CORP_BIRecommend",
        "FNC_IBU_pament_market", "BDAI_FEEDS_DICT_CP10");

    @Mock
    private MetaCache metaCache;
    @Mock
    private XpipeMeta xpipeMeta;
    @Mock
    private DcMeta dcMeta;
    @Mock
    private Map<String, ClusterMeta> clusterMetaMap;
    @Mock
    private MonitorService monitorService1;
    @Mock
    private MonitorService monitorService2;


    private final Set<String> clusters = new HashSet<String>() {
        {
            addAll(CLUSTERS1);
            addAll(CLUSTERS2);
        }
    };

    private final Set<String> clusters1 = new HashSet<>(CLUSTERS1);
    private final Set<String> clusters2 = new HashSet<>(CLUSTERS2);

    private DefaultMonitorClusterManager monitorClusterManager;

    @Before
    public void setUp() {
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);
        Map<String, DcMeta> dcMetaMap = new HashMap<>();
        dcMetaMap.put("dc", dcMeta);
        Mockito.when(xpipeMeta.getDcs()).thenReturn(dcMetaMap);
        Mockito.when(dcMeta.getClusters()).thenReturn(clusterMetaMap);
        Mockito.when(clusterMetaMap.keySet()).thenReturn(clusters);

        Mockito.when(monitorService1.getName()).thenReturn("beacon-1");
        Mockito.when(monitorService1.getWeight()).thenReturn(100);
        Mockito.when(monitorService2.getName()).thenReturn("beacon-2");
        Mockito.when(monitorService2.getWeight()).thenReturn(60);

        Mockito.when(monitorService1.fetchAllClusters("xpipe")).thenReturn(clusters1);
        Mockito.when(monitorService2.fetchAllClusters("xpipe")).thenReturn(clusters2);
        Mockito.when(monitorService1.fetchAllClusters("xpipe-bi")).thenReturn(new HashSet<>());
        Mockito.when(monitorService2.fetchAllClusters("xpipe-bi")).thenReturn(new HashSet<>());

        this.monitorClusterManager = new DefaultMonitorClusterManager(metaCache, 10,
            Lists.newArrayList(monitorService1), 1L);
    }

    @Test
    public void testAddService() {
        String cluster = "FNC_IBU_pament_market";
        MonitorService service = monitorClusterManager.getService(cluster);
        Assert.assertEquals(monitorService1, service);

        monitorClusterManager.addService(monitorService2);
        service = monitorClusterManager.getService(cluster);
        Assert.assertEquals(monitorService2, service);
    }

    @Test
    public void testRemoveService() {
        monitorClusterManager.addService(monitorService2);
        MonitorService service = monitorClusterManager.getService("cluster");
        Assert.assertEquals(service, monitorService1);

        monitorClusterManager.removeService(monitorService1);
        service = monitorClusterManager.getService("cluster");
        Assert.assertEquals(service, monitorService2);
    }

    @Test
    public void testGetService() {
        MonitorService service1 = monitorClusterManager.getService("cluster");
        Assert.assertEquals(service1, monitorService1);
        MonitorService service2 = monitorClusterManager.getService("cluster_name");
        Assert.assertEquals(service2, monitorService1);
    }

    @Test
    public void testGetServices() {
        List<MonitorService> services = monitorClusterManager.getServices();
        Assert.assertEquals(services, Lists.newArrayList(monitorService1));
        System.out.println(new Date());
    }

    @Test
    public void testUpdateServiceWeight() throws Exception {
        monitorClusterManager.addService(monitorService2);

        monitorClusterManager.updateServiceWeight(monitorService2, 100);
        Mockito.verify(monitorService2).setWeight(100);
        clusters2.addAll(Arrays.asList("SmartTrip_Redis", "bus_data_shift", "BDAI_FEEDS_DICT_CP11"));

        this.monitorClusterManager.startSyncRingTaskNow();
        Thread.sleep(100L);

        Mockito.verify(monitorService1).unregisterCluster("xpipe", "bus_data_shift");
        Mockito.verify(monitorService1).unregisterCluster("xpipe", "SmartTrip_Redis");
        Mockito.verify(monitorService1).unregisterCluster("xpipe", "BDAI_FEEDS_DICT_CP11");

    }

    @Test
    public void testUpdateServiceHost() throws Exception {
        DefaultMonitorClusterManager monitorClusterManager1 = new DefaultMonitorClusterManager(metaCache, 10,
                new ArrayList<>(), 1L);
        monitorClusterManager1.addService(monitorService1);
        monitorClusterManager1.addService(monitorService2);

        DefaultMonitorClusterManager monitorClusterManager2 = new DefaultMonitorClusterManager(metaCache, 10,
                new ArrayList<>(), 1L);
        monitorClusterManager2.addService(monitorService1);
        monitorClusterManager2.addService(monitorService2);
        monitorService1.updateHost("127.0.0.1");
        for (String cluster : clusters) {
            Assert.assertEquals(monitorClusterManager1.getService(cluster), monitorClusterManager2.getService(cluster));
        }
        SortedMap<Integer, MonitorService> ring1 = monitorClusterManager1.getRing();
        SortedMap<Integer, MonitorService> ring2 = monitorClusterManager2.getRing();
        Assert.assertEquals(ring1, ring2);
        Assert.assertEquals(ring1.hashCode(), ring2.hashCode());
    }

}
