package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class KeeperRoleAssignerTest {

    @Mock
    private DcMetaCache dcMetaCache;

    @Before
    public void setUp() {
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
    public void testBmActiveWithOneTfs() {
        KeeperMeta bm = keeper(6000, 1L, 1);
        KeeperMeta tfs = keeper(6001, 2L, 2);
        bm.setActive(true);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(bm, Arrays.asList(bm, tfs), dcMetaCache);
        Assert.assertEquals(KeeperState.ACTIVE, roles.get(bm));
        Assert.assertEquals(KeeperState.BACKUP, roles.get(tfs));
    }

    @Test
    public void testTwoTfsOneActiveOnePrepare() {
        KeeperMeta tfs1 = keeper(6000, 2L, 2);
        KeeperMeta tfs2 = keeper(6001, 3L, 1);
        tfs1.setActive(true);

        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(tfs1, Arrays.asList(tfs1, tfs2), dcMetaCache);
        Assert.assertEquals(KeeperState.ACTIVE, roles.get(tfs1));
        Assert.assertEquals(KeeperState.PREPARE, roles.get(tfs2));
    }

    @Test
    public void testBmActiveWithTwoTfsHighestPriorityGetsBackup() {
        KeeperMeta bm = keeper(6000, 1L, 1);
        KeeperMeta lowTfs = keeper(6001, 2L, 1);
        KeeperMeta highTfs = keeper(6002, 3L, 5);
        bm.setActive(true);

        List<KeeperMeta> keepers = Arrays.asList(bm, lowTfs, highTfs);
        Map<KeeperMeta, KeeperState> roles = KeeperRoleAssigner.assignRoles(bm, keepers, dcMetaCache);

        Assert.assertEquals(KeeperState.ACTIVE, roles.get(bm));
        Assert.assertEquals(KeeperState.BACKUP, roles.get(highTfs));
        Assert.assertEquals(KeeperState.PREPARE, roles.get(lowTfs));
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
