package com.ctrip.xpipe.redis.meta.server.keeper.elect;

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

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperMetaEnricherTest {

    @Mock
    private DcMetaCache dcMetaCache;

    @Test
    public void testEnrichMatchMissFillsDefaultPriority() {
        KeeperMeta survive = keeper(6000, 1L, null);
        when(dcMetaCache.getShardKeepers(1L, 1L)).thenReturn(Collections.emptyList());

        List<KeeperMeta> enriched = KeeperMetaEnricher.enrich(dcMetaCache, 1L, 1L,
                Collections.singletonList(survive));

        Assert.assertEquals(1, enriched.size());
        Assert.assertEquals(Integer.valueOf(KeeperPriorityUtils.DEFAULT_PRIORITY), enriched.get(0).getPriority());
    }

    @Test
    public void testEnrichCopiesPriorityFromNormalizedMeta() {
        KeeperMeta survive = keeper(6000, 1L, null);
        KeeperMeta fromMeta = keeper(6000, 1L, KeeperPriorityUtils.DEFAULT_PRIORITY);
        when(dcMetaCache.getShardKeepers(1L, 1L)).thenReturn(Collections.singletonList(fromMeta));

        List<KeeperMeta> enriched = KeeperMetaEnricher.enrich(dcMetaCache, 1L, 1L,
                Collections.singletonList(survive));

        Assert.assertEquals(Integer.valueOf(KeeperPriorityUtils.DEFAULT_PRIORITY), enriched.get(0).getPriority());
        Assert.assertEquals(Long.valueOf(1L), enriched.get(0).getKeeperContainerId());
    }

    @Test
    public void testEnrichPreservesExplicitZero() {
        KeeperMeta survive = keeper(6000, 1L, null);
        KeeperMeta fromMeta = keeper(6000, 1L, 0);
        when(dcMetaCache.getShardKeepers(1L, 1L)).thenReturn(Collections.singletonList(fromMeta));

        List<KeeperMeta> enriched = KeeperMetaEnricher.enrich(dcMetaCache, 1L, 1L,
                Collections.singletonList(survive));

        Assert.assertEquals(Integer.valueOf(0), enriched.get(0).getPriority());
    }

    @Test
    public void testEnrichCopiesPositivePriority() {
        KeeperMeta survive = keeper(6000, 1L, null);
        KeeperMeta fromMeta = keeper(6000, 1L, 5);
        when(dcMetaCache.getShardKeepers(1L, 1L)).thenReturn(Collections.singletonList(fromMeta));

        List<KeeperMeta> enriched = KeeperMetaEnricher.enrich(dcMetaCache, 1L, 1L,
                Arrays.asList(survive));

        Assert.assertEquals(Integer.valueOf(5), enriched.get(0).getPriority());
    }

    private KeeperMeta keeper(int port, long keeperContainerId, Integer priority) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp("127.0.0.1");
        keeperMeta.setPort(port);
        keeperMeta.setKeeperContainerId(keeperContainerId);
        keeperMeta.setPriority(priority);
        return keeperMeta;
    }
}
