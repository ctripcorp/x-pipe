package com.ctrip.xpipe.redis.meta.server.crdt.master.impl;

import com.ctrip.xpipe.concurrent.KeyedOneThreadTaskExecutor;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommand;
import com.ctrip.xpipe.redis.meta.server.crdt.master.MasterChooseCommandFactory;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultPeerMasterChooseActionTest extends AbstractMetaServerTest {

    @Mock
    private MasterChooseCommandFactory masterChooseCommandFactory;

    @Mock
    private KeyedOneThreadTaskExecutor<Pair<Long, Long>> peerMasterChooseExecutors;

    @Mock
    private MasterChooseCommand command;

    private DefaultPeerMasterChooseAction peerMasterChooseAction;

    @Before
    public void setupDefaultPeerMasterChooseActionTest() {
        peerMasterChooseAction = new DefaultPeerMasterChooseAction(masterChooseCommandFactory, peerMasterChooseExecutors);
        Mockito.when(masterChooseCommandFactory.buildPeerMasterChooserCommand(getDc(), getClusterDbId(), getShardDbId())).thenReturn(command);
    }

    @Test
    public void testChoosePeerMaster() {
        peerMasterChooseAction.choosePeerMaster(getDc(), getClusterDbId(), getShardDbId());
        Mockito.verify(peerMasterChooseExecutors, Mockito.times(1)).execute(Pair.of(getClusterDbId(), getShardDbId()), command);
    }

}
