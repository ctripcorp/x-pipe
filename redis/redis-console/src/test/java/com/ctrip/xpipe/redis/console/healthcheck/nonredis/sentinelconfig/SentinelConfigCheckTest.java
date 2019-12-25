package com.ctrip.xpipe.redis.console.healthcheck.nonredis.sentinelconfig;

import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class SentinelConfigCheckTest {

    @InjectMocks
    SentinelConfigCheck sentinelConfigCheck;

    @Mock
    MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private ClusterService clusterService;

    private List<String> mockDcs = Arrays.asList("jq", "oy", "fra");

    private List<String> mockClusters = Arrays.asList("cluster1", "cluster2");

    private List<String> mockShards = Arrays.asList("shard1", "shard2");

    private Map<String, String> activeDcMap = new HashMap<String, String>() {{
        put("cluster1", "jq");
        put("cluster2", "fra");
    }};

    private List<Set<String> > regions = Arrays.asList(
            Sets.newHashSet("jq", "oy"),
            Sets.newHashSet("fra")
    );

    private Map<String, Set<String> > expectedUnsafeClusters = new HashMap<String, Set<String> >() {{
        put("jq", Sets.newHashSet("cluster1"));
        put("oy", Sets.newHashSet("cluster1"));
        put("fra", Sets.newHashSet("cluster2"));
    }};

    private List<Pair<String, String> > mockClusterShards = new ArrayList<Pair<String, String> >() {{
        for (String cluster: mockClusters) {
            mockShards.forEach(shard -> add(new Pair<>(cluster, shard)));
        }
    }};

    @Before
    public void beforeSentinelConfigCheckTest() {
        Mockito.when(metaCache.getDcs()).thenReturn(new HashSet<>(mockDcs));
        Mockito.when(metaCache.getDcClusterShard(Mockito.anyString())).thenReturn(mockClusterShards);

        Mockito.when(metaCache.getSentinels(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(Collections.emptySet());

        Mockito.when(metaCache.getActiveDc(Mockito.anyString(), Mockito.anyString())).then(invocationOnMock -> {
            String cluster = invocationOnMock.getArgumentAt(0, String.class);
            return activeDcMap.get(cluster);
        });

        Mockito.when(metaCache.isCrossRegion(Mockito.anyString(), Mockito.anyString())).then(invocationOnMock -> {
            String activeDc = invocationOnMock.getArgumentAt(0, String.class);
            String backupDc = invocationOnMock.getArgumentAt(1, String.class);
            for (Set<String> dcSet: regions) {
                if (dcSet.contains(activeDc)) return !dcSet.contains(backupDc);
            }

            return false;
        });
    }

    @Test
    public void testDoCheck() {
        Mockito.doAnswer(invocationOnMock -> {
            String dc = invocationOnMock.getArgumentAt(0, String.class);
            List<String> clusterList = invocationOnMock.getArgumentAt(1, List.class);

            for (String cluster: clusterList) {
                Assert.assertTrue(expectedUnsafeClusters.get(dc).contains(cluster));
            }
            return null;
        }).when(clusterService).reBalanceClusterSentinels(Mockito.anyString(), Mockito.anyList());

        Mockito.doAnswer(invocationOnMock -> {
           String dc = invocationOnMock.getArgumentAt(0, String.class);
           String cluster = invocationOnMock.getArgumentAt(1, String.class);
           String shard = invocationOnMock.getArgumentAt(2, String.class);

           Assert.assertTrue(expectedUnsafeClusters.get(dc).contains(cluster));
           Assert.assertTrue(mockShards.contains(shard));

           return null;
        }).when(alertManager).alert(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
                Mockito.any(), Mockito.any(), Mockito.any());

        sentinelConfigCheck.doCheck();
    }

}
