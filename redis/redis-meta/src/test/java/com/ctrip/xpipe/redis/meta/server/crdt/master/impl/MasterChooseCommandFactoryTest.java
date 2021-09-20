package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.AbstractMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.CurrentMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.command.PeerMasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.Executor;

@RunWith(MockitoJUnitRunner.class)
public class MasterChooseCommandFactoryTest extends AbstractMetaServerTest {

    private DefaultMasterChooseCommandFactory factory;

    private String dcId = "dc1", clusterId = "cluster1", shardId = "shard1";

    private String upstreamDcId = "dc2";

    @Mock
    protected DcMetaCache dcMetaCache;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    @Mock
    private MultiDcService multiDcService;

    private CommandFuture commandFuture = new DefaultCommandFuture();

    RedisMeta redisMeta = null;

    RedisMeta currentMasterMeta = null;
    RedisMeta peerMasterMeta = null;

    @Before
    public void setupMasterChooseCommandFactoryTest() throws Exception {
        factory = new DefaultMasterChooseCommandFactory(dcMetaCache, currentMetaManager, getXpipeNettyClientKeyedObjectPool(), multiDcService);
        Mockito.doAnswer(invocation -> currentMasterMeta).when(currentMetaManager).getCurrentCRDTMaster(clusterId, shardId);
        Mockito.doAnswer(invocation -> peerMasterMeta).when(currentMetaManager).getPeerMaster(dcId, clusterId, shardId);
    }

    @Test
    public void testCreateMasterChooserCommand() {
        MasterChooseCommand localDcCommand = factory.buildCurrentMasterChooserCommand(clusterId, shardId);
        MasterChooseCommand remoteDcCommand = factory.buildPeerMasterChooserCommand(upstreamDcId, clusterId, shardId);
        Assert.assertTrue(localDcCommand instanceof CurrentMasterChooseCommand);
        Assert.assertTrue(remoteDcCommand instanceof PeerMasterChooseCommand);
    }

    @Test
    public void testForPeerMasterChooseCommandResultHandle() {
        MasterChooseCommand peerMasterCommand = new TestMasterChooseCommand(clusterId, shardId);
        factory.wrapPeerMasterChooseCommand(dcId, clusterId, shardId, peerMasterCommand);

        // redis meta null
        redisMeta = null;
        peerMasterMeta = null;
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // redis meta change to null
        redisMeta = null;
        peerMasterMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        commandFuture = new DefaultCommandFuture();
        factory.wrapPeerMasterChooseCommand(dcId, clusterId, shardId, peerMasterCommand);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // redis meta same
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        peerMasterMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        commandFuture = new DefaultCommandFuture();
        factory.wrapPeerMasterChooseCommand(dcId, clusterId, shardId, peerMasterCommand);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // new redis meta
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        peerMasterMeta = null;
        commandFuture = new DefaultCommandFuture();
        factory.wrapPeerMasterChooseCommand(dcId, clusterId, shardId, peerMasterCommand);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.times(1)).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyInt());

        // redis meta change
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        peerMasterMeta = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1L);;
        commandFuture = new DefaultCommandFuture();
        factory.wrapPeerMasterChooseCommand(dcId, clusterId, shardId, peerMasterCommand);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.times(2)).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyInt());
    }

    @Test
    public void testForCurrentMasterChooseCommandResultHandle() {
        MasterChooseCommand currentMasterCommand = new TestMasterChooseCommand(clusterId, shardId);
        factory.wrapCurrentMasterChooseCommand(clusterId, shardId, currentMasterCommand);

        // redis meta null
        redisMeta = null;
        currentMasterMeta = null;
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setPeerMaster(Mockito.anyString(), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // redis meta change to null
        redisMeta = null;
        currentMasterMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        commandFuture = new DefaultCommandFuture();
        factory.wrapCurrentMasterChooseCommand(clusterId, shardId, currentMasterCommand);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setCurrentCRDTMaster(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // redis meta same
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        currentMasterMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        commandFuture = new DefaultCommandFuture();
        factory.wrapCurrentMasterChooseCommand(clusterId, shardId, currentMasterCommand);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.never()).setCurrentCRDTMaster(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(), Mockito.anyInt());

        // new redis meta
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        currentMasterMeta = null;
        commandFuture = new DefaultCommandFuture();
        factory.wrapCurrentMasterChooseCommand(clusterId, shardId, currentMasterCommand);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.times(1)).setCurrentCRDTMaster(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyInt());

        // redis meta change
        redisMeta = new RedisMeta().setIp("127.0.0.1").setPort(6379).setGid(1L);
        currentMasterMeta = new RedisMeta().setIp("127.0.0.1").setPort(6380).setGid(1L);;
        commandFuture = new DefaultCommandFuture();
        factory.wrapCurrentMasterChooseCommand(clusterId, shardId, currentMasterCommand);
        commandFuture.setSuccess(redisMeta);
        sleep(10);
        Mockito.verify(currentMetaManager, Mockito.times(2)).setCurrentCRDTMaster(Mockito.anyString(),
                Mockito.anyString(), Mockito.anyLong(), Mockito.anyString(), Mockito.anyInt());
    }

    class TestMasterChooseCommand extends AbstractMasterChooseCommand {

        public TestMasterChooseCommand(String clusterId, String shardId) {
            super(clusterId, shardId);
        }

        @Override
        protected RedisMeta choose() throws Exception {
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
