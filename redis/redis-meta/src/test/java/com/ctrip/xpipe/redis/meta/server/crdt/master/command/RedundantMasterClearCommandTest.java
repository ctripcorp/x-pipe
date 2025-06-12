package com.ctrip.xpipe.redis.meta.server.crdt.master.command;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Mockito.*;

/**
 * @author lishanglin
 * date 2023/12/22
 */
@RunWith(MockitoJUnitRunner.class)
public class RedundantMasterClearCommandTest extends AbstractMetaServerTest {

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Before
    public void setupRedundantMasterClearCommandTest() {
        when(currentMetaManager.getUpstreamPeerDcs(getClusterDbId(), getShardDbId()))
                .thenReturn(new HashSet<>(Arrays.asList("jq", "oy")));
    }

    @Test
    public void testClearRedundant() throws Exception {
        RedundantMasterClearCommand command = new RedundantMasterClearCommand(getClusterDbId(), getShardDbId(),
                new HashSet<>(Arrays.asList("jq", "rb")), currentMetaManager);
        Assert.assertEquals(Collections.singleton("oy"), command.execute().get());
        verify(currentMetaManager, times(1)).removePeerMaster("oy", getClusterDbId(), getShardDbId());
    }

    @Test
    public void testNoRedundant() throws Exception {
        RedundantMasterClearCommand command = new RedundantMasterClearCommand(getClusterDbId(), getShardDbId(),
                new HashSet<>(Arrays.asList("jq", "oy")), currentMetaManager);
        Assert.assertEquals(Collections.emptySet(), command.execute().get());
        verify(currentMetaManager, never()).removePeerMaster(anyString(), any(), any());
    }

}
