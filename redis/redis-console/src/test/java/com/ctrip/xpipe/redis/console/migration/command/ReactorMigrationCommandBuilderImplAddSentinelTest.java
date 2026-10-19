package com.ctrip.xpipe.redis.console.migration.command;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.impl.BeaconSentinelGrayTestConfigs;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.resources.LoopResources;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ReactorMigrationCommandBuilderImplAddSentinelTest {

    private static final String CLUSTER = "cluster1";
    private static final String FROM_DC = "SHARB";
    private static final String DEST_DC = "SHAXY";

    @Mock
    private MetaCache metaCache;
    @Mock
    private LoopResources loopResources;
    @Mock
    private ConnectionProvider connectionProvider;

    private ReactorMigrationCommandBuilderImpl builder;

    @Before
    public void setup() {
        Map<String, Set<String>> idcGray = new HashMap<>();
        idcGray.put("sharb", new HashSet<>(Collections.singletonList(ClusterType.ONE_WAY.name())));
        ConsoleConfig config = BeaconSentinelGrayTestConfigs.withIdcClusterTypes(idcGray);

        XpipeMeta xpipeMeta = new XpipeMeta();
        xpipeMeta.addDc(dcWithCluster(FROM_DC));
        xpipeMeta.addDc(dcWithCluster(DEST_DC));
        when(metaCache.getXpipeMeta()).thenReturn(xpipeMeta);

        builder = new ReactorMigrationCommandBuilderImpl(loopResources, connectionProvider, config, metaCache);
    }

    @Test
    public void addSentinelFalseWhenDestDcInIdcGray() {
        Boolean addSentinel = ReflectionTestUtils.invokeMethod(builder, "addSentinelRequestValue", FROM_DC, CLUSTER);
        Assert.assertEquals(Boolean.FALSE, addSentinel);
    }

    @Test
    public void addSentinelTrueWhenDestDcNotInIdcGray() {
        Boolean addSentinel = ReflectionTestUtils.invokeMethod(builder, "addSentinelRequestValue", DEST_DC, CLUSTER);
        Assert.assertEquals(Boolean.TRUE, addSentinel);
    }

    private static DcMeta dcWithCluster(String dc) {
        ClusterMeta clusterMeta = new ClusterMeta(CLUSTER)
                .setOrgId(10)
                .setType(ClusterType.ONE_WAY.name())
                .setActiveDc(dc);
        DcMeta dcMeta = new DcMeta(dc);
        dcMeta.addCluster(clusterMeta);
        return dcMeta;
    }
}
