package com.ctrip.xpipe.redis.meta.server.crdt.peermaster.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.peermaster.PeerMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.Executor;

@RunWith(MockitoJUnitRunner.class)
public class DefaultPeerMasterChooserTest extends AbstractMetaServerTest {

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private CurrentMetaManager currentMetaManager;

    private String dcId = "dc1", clusterId = "cluster1", shardId = "shard1";

    private int checkIntervalSeconds = 1;

    @Mock
    private MultiDcService multiDcService;

    private DefaultPeerMasterChooser defaultPeerMasterChooser;

    RedisMeta redisMeta = null;

    RedisMeta currentRedisMeta = null;

    private CommandFuture commandFuture = new DefaultCommandFuture();

    @Before
    public void setupDefaultPeerMasterChooserTest() throws Exception {
        defaultPeerMasterChooser = new DefaultPeerMasterChooser(clusterId, shardId, dcMetaCache, currentMetaManager,
                getXpipeNettyClientKeyedObjectPool(), multiDcService, executors, scheduled, checkIntervalSeconds);

        Mockito.when(dcMetaCache.getCurrentDc()).thenReturn(dcId);
        Mockito.doAnswer(invocation -> currentRedisMeta).when(currentMetaManager).getPeerMaster(Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }

    @Test
    public void testCreateMasterChooserCommand() {
        PeerMasterChooseCommand localDcCommand = defaultPeerMasterChooser.createMasterChooserCommand(dcId);
        PeerMasterChooseCommand remoteDcCommand = defaultPeerMasterChooser.createMasterChooserCommand("remote_dc");
        Assert.assertTrue(localDcCommand instanceof DefaultPeerMasterChooseCommand);
        Assert.assertTrue(remoteDcCommand instanceof RemoteDcPeerMasterChooseCommand);
    }

    @Test
    public void testForCommandResultHandle() {
        PeerMasterChooseCommand peerMasterChooser = new TestPeerMasterChooseCommand();
        defaultPeerMasterChooser.wrapMasterChooseCommand(dcId, peerMasterChooser);

        // redis meta null
        redisMeta = null;
        currentRedisMeta = null;
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // redis meta change to null
        redisMeta = null;
        currentRedisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1);
        commandFuture = new DefaultCommandFuture();
        defaultPeerMasterChooser.wrapMasterChooseCommand(dcId, peerMasterChooser);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // redis meta same
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1);
        currentRedisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1);
        commandFuture = new DefaultCommandFuture();
        defaultPeerMasterChooser.wrapMasterChooseCommand(dcId, peerMasterChooser);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // new redis meta
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1);
        currentRedisMeta = null;
        commandFuture = new DefaultCommandFuture();
        defaultPeerMasterChooser.wrapMasterChooseCommand(dcId, peerMasterChooser);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.times(1)).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // redis meta change
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1);
        currentRedisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1);;
        commandFuture = new DefaultCommandFuture();
        defaultPeerMasterChooser.wrapMasterChooseCommand(dcId, peerMasterChooser);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.times(2)).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

    }

    class TestPeerMasterChooseCommand implements PeerMasterChooseCommand {

        public RedisMeta choose() throws Exception {
            return redisMeta;
        }

        public CommandFuture<RedisMeta> future() {
            return commandFuture;
        }

        public CommandFuture<RedisMeta> execute() {
            return null;
        }

        public CommandFuture<RedisMeta> execute(Executor executors) {
            return null;
        }

        public String getName() {
            return null;
        }

        public void reset() {}

    }

}
