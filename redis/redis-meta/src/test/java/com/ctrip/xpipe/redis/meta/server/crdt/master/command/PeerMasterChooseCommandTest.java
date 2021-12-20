package com.ctrip.xpipe.redis.meta.server.crdt.master.command;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.times;

@RunWith(MockitoJUnitRunner.class)
public class PeerMasterChooseCommandTest extends AbstractMetaServerTest {

    @Mock
    private MultiDcService multiDcService;

    private PeerMasterChooseCommand chooseCommand;

    private String dcId = "dc1", clusterId = "cluster1", shardId = "shard1";

    private Long clusterDbId = 1L, shardDbId = 1L;

    @Before
    public void setupRemoteDcPeerMasterChooseCommandTest() {
        chooseCommand = new PeerMasterChooseCommand(dcId, clusterDbId, shardDbId, multiDcService);
    }

    @Test
    public void testChoose() {
        RedisMeta redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        Mockito.when(multiDcService.getPeerMaster(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong())).thenReturn(redisMeta);
        RedisMeta result = chooseCommand.choose();
        Assert.assertEquals(redisMeta, result);
        Mockito.verify(multiDcService, times(1)).getPeerMaster(Mockito.anyString(), Mockito.anyLong(), Mockito.anyLong());
    }

}
