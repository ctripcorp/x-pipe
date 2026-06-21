package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Assert;
import org.junit.Before;
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
public class StrategyAwareKeeperActiveElectAlgorithmTest {

    @Mock
    private DcMetaCache dcMetaCache;

    private KeeperMeta bmKeeper;
    private KeeperMeta tfsKeeper;

    @Before
    public void setUp() {
        bmKeeper = keeper(6000, 1L, 1);
        tfsKeeper = keeper(6001, 2L, 2);
        when(dcMetaCache.getKeeperContainer(any(KeeperMeta.class))).thenAnswer(invocation -> {
            KeeperMeta keeperMeta = invocation.getArgument(0);
            KeeperContainerMeta keeperContainerMeta = new KeeperContainerMeta();
            keeperContainerMeta.setId(keeperMeta.getKeeperContainerId());
            if (keeperMeta.getKeeperContainerId() == 2L || keeperMeta.getKeeperContainerId() == 3L) {
                keeperContainerMeta.setDiskType("tfs-1");
            } else {
                keeperContainerMeta.setDiskType("DEFAULT");
            }
            return keeperContainerMeta;
        });
    }

    @Test
    public void testAutoSelectHigherPriority() {
        KeeperMeta lowPriority = keeper(6002, 1L, 1);
        KeeperMeta highPriority = keeper(6003, 2L, 5);
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.AUTO, dcMetaCache);

        KeeperMeta selected = algorithm.select(1L, 1L, Arrays.asList(lowPriority, highPriority));
        Assert.assertEquals(highPriority, selected);
    }

    @Test
    public void testAutoSkipsPriorityZero() {
        KeeperMeta zeroPriority = keeper(6002, 1L, 0);
        KeeperMeta valid = keeper(6003, 2L, 1);
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.AUTO, dcMetaCache);

        KeeperMeta selected = algorithm.select(1L, 1L, Arrays.asList(zeroPriority, valid));
        Assert.assertEquals(valid, selected);
    }

    @Test
    public void testBmPreferSelectsBmWhenAvailable() {
        tfsKeeper.setPriority(10);
        bmKeeper.setPriority(1);
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.BM_PREFER, dcMetaCache);

        KeeperMeta selected = algorithm.select(1L, 1L, Arrays.asList(tfsKeeper, bmKeeper));
        Assert.assertEquals(bmKeeper, selected);
    }

    @Test
    public void testBmPreferFallsBackToAutoWhenNoBm() {
        tfsKeeper.setPriority(1);
        KeeperMeta anotherTfs = keeper(6002, 3L, 3);
        anotherTfs.setPriority(5);
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.BM_PREFER, dcMetaCache);

        KeeperMeta selected = algorithm.select(1L, 1L, Arrays.asList(tfsKeeper, anotherTfs));
        Assert.assertEquals(anotherTfs, selected);
    }

    @Test
    public void testSamePriorityKeepsZkOrder() {
        KeeperMeta first = keeper(6002, 1L, 2);
        KeeperMeta second = keeper(6003, 2L, 2);
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.AUTO, dcMetaCache);

        KeeperMeta selected = algorithm.select(1L, 1L, Arrays.asList(first, second));
        Assert.assertEquals(first, selected);
    }

    @Test
    public void testNoElectableKeeperReturnsNull() {
        KeeperMeta zeroPriority = keeper(6002, 1L, 0);
        StrategyAwareKeeperActiveElectAlgorithm algorithm =
                new StrategyAwareKeeperActiveElectAlgorithm(KeeperElectStrategy.AUTO, dcMetaCache);

        Assert.assertNull(algorithm.select(1L, 1L, Collections.singletonList(zeroPriority)));
    }

    private KeeperMeta keeper(int port, long keeperContainerId, int priority) {
        KeeperMeta keeperMeta = new KeeperMeta();
        keeperMeta.setIp("127.0.0.1");
        keeperMeta.setPort(port);
        keeperMeta.setKeeperContainerId(keeperContainerId);
        keeperMeta.setPriority(priority);
        return keeperMeta;
    }
}
