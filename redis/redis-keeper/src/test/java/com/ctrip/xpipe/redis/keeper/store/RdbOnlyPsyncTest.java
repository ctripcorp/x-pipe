package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.protocal.cmd.RdbOnlyPsync;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
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
    public void fsyncFail_rdbFileClosed() throws Exception {
        Server redisServer = startServer("-ERR: mock err");
        Endpoint redisEndpoint = new DefaultEndPoint("localhost", redisServer.getPort());
        DumpedRdbStore rdbStore = Mockito.mock(DumpedRdbStore.class);
        RdbOnlyReplicationStore replicationStore = new RdbOnlyReplicationStore(rdbStore);
        RdbOnlyPsync psync = new RdbOnlyPsync(NettyPoolUtil.createNettyPool(redisEndpoint), replicationStore, scheduled);

        CommandFuture<Object> future = psync.execute();
        waitConditionUntilTimeOut(future::isDone);
        Assert.assertFalse(future.isSuccess());
        Mockito.verify(rdbStore, Mockito.times(1)).close();
    }

}
