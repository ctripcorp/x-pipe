package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster.impl;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.mockito.Mockito.*;

/**
 * @author ayq
 * <p>
 * 2022/4/9 16:13
 */
public class HeteroDownStreamDcKeeperMasterChooserAlgorithmTest extends AbstractDcKeeperMasterChooserTest {

    private HeteroDownStreamDcKeeperMasterChooserAlgorithm algorithm;

    @Mock
    protected MultiDcService multiDcService;

    @Before
    public void beforeBackupDcKeeperMasterChooserTest() {

        algorithm = new HeteroDownStreamDcKeeperMasterChooserAlgorithm(clusterHeteroId, shardDbId,
                dcMetaCache, currentMetaManager, multiDcService, scheduled);

        when(dcMetaCache.getCurrentDc()).thenReturn("fra");
        when(dcMetaCache.getUpstreamDc("fra", clusterHeteroId, shardDbId)).thenReturn(primaryDc);
    }

    @Test
    public void testGetUpstream() {

        algorithm.choose();

        verify(multiDcService, atLeast(1)).getActiveKeeper(primaryDc, clusterHeteroId, shardDbId);

        logger.info("[testGetUpstream][getActiveKeeper give a result]");
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp("localhost");
        keeperMeta.setPort(randomPort());
        when(multiDcService.getActiveKeeper(primaryDc, clusterHeteroId, shardDbId)).thenReturn(keeperMeta);

        Assert.assertEquals(new Pair<>(keeperMeta.getIp(), keeperMeta.getPort()), algorithm.choose());

        verify(multiDcService, atLeast(1)).getActiveKeeper(primaryDc, clusterHeteroId, shardDbId);
    }

}