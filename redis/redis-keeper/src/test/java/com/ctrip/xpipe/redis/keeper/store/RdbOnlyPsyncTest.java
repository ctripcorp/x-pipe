package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.RdbOnlyPsync;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncMasterRdbOffsetNotContinuousRuntimeException;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author lishanglin
 * date 2023/7/31
 */
public class RdbOnlyPsyncTest extends AbstractRedisTest {

    @Test
    public void psyncFail_rdbFileClosed() throws Exception {
        Server redisServer = startServer("-ERR: mock err");
        Endpoint redisEndpoint = new DefaultEndPoint("127.0.0.1", redisServer.getPort());
        DumpedRdbStore rdbStore = Mockito.mock(DumpedRdbStore.class);
        RdbOnlyReplicationStore replicationStore = new RdbOnlyReplicationStore(rdbStore);
        RdbOnlyPsync psync = new RdbOnlyPsync(NettyPoolUtil.createNettyPool(redisEndpoint), replicationStore, scheduled);

        CommandFuture<Object> future = psync.execute();
        waitConditionUntilTimeOut(future::isDone);
        Assert.assertFalse(future.isSuccess());
        Mockito.verify(rdbStore, Mockito.timeout(3000).atLeastOnce()).close();
    }

    @Test
    public void offsetNotContinue_rdbFileClosed() throws Exception {
        Server redisServer = startServer("+FULLRESYNC 2aaecd36e885a0c9079919e2514f4af4f7a5d1bd 999999999999999\r\n");
        Endpoint redisEndpoint = new DefaultEndPoint("127.0.0.1", redisServer.getPort());
        PsyncObserver failObserver = Mockito.mock(PsyncObserver.class);
        DumpedRdbStore rdbStore = Mockito.mock(DumpedRdbStore.class);
        RdbOnlyReplicationStore replicationStore = new RdbOnlyReplicationStore(rdbStore);
        RdbOnlyPsync psync = new RdbOnlyPsync(NettyPoolUtil.createNettyPool(redisEndpoint), replicationStore, scheduled);
        psync.addPsyncObserver(failObserver);
        Mockito.doThrow(new PsyncMasterRdbOffsetNotContinuousRuntimeException(100, 1)).when(failObserver).onFullSync(Mockito.anyLong());

        CommandFuture<Object> future = psync.execute();
        waitConditionUntilTimeOut(future::isDone);
        Assert.assertFalse(future.isSuccess());
        Assert.assertTrue(future.cause() instanceof PsyncMasterRdbOffsetNotContinuousRuntimeException);
        Mockito.verify(rdbStore, Mockito.timeout(3000).atLeastOnce()).close();
    }

}
