package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TfsKeeperUtilsTest {

    @Mock
    private DcMetaCache dcMetaCache;

    @Test
    public void testIsTfsKeeperWhenContainerMissingReturnsFalse() {
        KeeperMeta keeperMeta = keeper("10.0.0.1", 6380, 99L);
        when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class)))
                .thenThrow(new IllegalArgumentException("[getKeeperContainer][unfound keepercontainer]"));

        Assert.assertFalse(TfsKeeperUtils.isTfsKeeper(keeperMeta, dcMetaCache));
        Assert.assertFalse(TfsKeeperUtils.shardHasTfsKeeper(Collections.singletonList(keeperMeta), dcMetaCache));
    }

    @Test
    public void testIsTfsKeeperWhenContainerNullReturnsFalse() {
        KeeperMeta keeperMeta = keeper("10.0.0.1", 6380, 1L);
        when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenReturn(null);

        Assert.assertFalse(TfsKeeperUtils.isTfsKeeper(keeperMeta, dcMetaCache));
    }

    @Test
    public void testMergeByIpPortSurviveWinsThenAppendsMetaOnly() {
        KeeperMeta surviveBm = keeper("10.0.0.1", 6380, 1L).setActive(true);
        KeeperMeta surviveTfs = keeper("10.0.0.2", 6380, 2L);
        KeeperMeta metaBm = keeper("10.0.0.1", 6380, 1L);
        KeeperMeta metaOnly = keeper("10.0.0.3", 6380, 3L);

        List<KeeperMeta> union = TfsKeeperUtils.mergeByIpPort(
                Arrays.asList(surviveBm, surviveTfs),
                Arrays.asList(metaBm, metaOnly));

        Assert.assertEquals(3, union.size());
        Assert.assertSame(surviveBm, union.get(0));
        Assert.assertSame(surviveTfs, union.get(1));
        Assert.assertEquals(metaOnly.getIp(), union.get(2).getIp());
        Assert.assertEquals(metaOnly.getPort(), union.get(2).getPort());
    }

    @Test
    public void testMergeByIpPortNullMetaReturnsSurvive() {
        KeeperMeta surviveBm = keeper("10.0.0.1", 6380, 1L);
        List<KeeperMeta> union = TfsKeeperUtils.mergeByIpPort(Collections.singletonList(surviveBm), null);
        Assert.assertEquals(1, union.size());
        Assert.assertSame(surviveBm, union.get(0));
    }

    @Test
    public void testMergeSurviveAndShardKeepersWhenGetShardKeepersThrowsFallbackSurvive() {
        KeeperMeta surviveBm = keeper("10.0.0.1", 6380, 1L);
        when(dcMetaCache.getShardKeepers(1L, 2L))
                .thenThrow(new IllegalArgumentException("unknown clusterDbId shardDbId 1 2"));

        List<KeeperMeta> union = TfsKeeperUtils.mergeSurviveAndShardKeepers(
                Collections.singletonList(surviveBm), dcMetaCache, 1L, 2L);

        Assert.assertEquals(1, union.size());
        Assert.assertSame(surviveBm, union.get(0));
    }

    @Test
    public void testMergeSurviveAndShardKeepersMergesMetaOnly() {
        KeeperMeta surviveBm = keeper("10.0.0.1", 6380, 1L);
        KeeperMeta metaOnly = keeper("10.0.0.3", 6380, 3L);
        when(dcMetaCache.getShardKeepers(1L, 2L)).thenReturn(Arrays.asList(surviveBm, metaOnly));

        List<KeeperMeta> union = TfsKeeperUtils.mergeSurviveAndShardKeepers(
                Collections.singletonList(surviveBm), dcMetaCache, 1L, 2L);

        Assert.assertEquals(2, union.size());
        Assert.assertSame(surviveBm, union.get(0));
        Assert.assertEquals(metaOnly.getIp(), union.get(1).getIp());
        Assert.assertEquals(metaOnly.getPort(), union.get(1).getPort());
    }

    private KeeperMeta keeper(String ip, int port, long keeperContainerId) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp(ip);
        keeperMeta.setPort(port);
        keeperMeta.setKeeperContainerId(keeperContainerId);
        return keeperMeta;
    }
}
