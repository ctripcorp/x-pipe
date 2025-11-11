package com.ctrip.xpipe.redis.keeper.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.PartialOnlyGapAllowedSync;
import com.ctrip.xpipe.redis.core.store.MetaStore;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
/**
 * @author TB
 * <p>
 * 2025/10/15 17:10
 */
public class PartialOnlyGapAllowedSyncTest extends AbstractRedisKeeperTest {

    private PartialOnlyGapAllowedSync gasync;
    private ReplicationStoreManager replicationStoreManager;
    private ReplicationStore replicationStore;

    @Before
    public void beforePsyncTest() throws Exception{
        replicationStoreManager = createReplicationStoreManager();
        LifecycleHelper.initializeIfPossible(replicationStoreManager);
        replicationStore = replicationStoreManager.create();

        SimpleObjectPool<NettyClient> clientPool = NettyPoolUtil.createNettyPool(new DefaultEndPoint("127.0.0.1", 1234));
        gasync = new PartialOnlyGapAllowedSync(clientPool, new DefaultEndPoint("127.0.0.1", 1234), replicationStoreManager, scheduled);
        gasync.future().addListener(commandFuture -> {
            if(!commandFuture.isSuccess()){
                logger.error("[operationComplete]", commandFuture.cause());
            }
        });
    }


    @Test
    public void testPsyncContinueFromOffsetNoPlusOne() throws XpipeException, IOException, InterruptedException {

        long reploff = 300000000;
        String replId = "replIdxx";
        String reply = "+" + GapAllowedSync.PARTIAL_SYNC + " " + replId + " " + reploff + "\r\n";

        gasync.getRequest();

        runData(new byte[][]{
                reply.getBytes(),
        });

        Assert.assertFalse(gasync.future().isDone());

        replicationStore = replicationStoreManager.getCurrent();

        MetaStore metaStore = replicationStore.getMetaStore();

        ReplStage curReplStage = metaStore.getCurrentReplStage();
        Assert.assertEquals(curReplStage.getProto(), ReplStage.ReplProto.PSYNC);
        Assert.assertEquals(curReplStage.getReplId(), replId);
        Assert.assertEquals(curReplStage.getBegOffsetRepl(), reploff);
    }

    private void runData(byte [][]data) {

        ByteBuf[]byteBufs = new ByteBuf[data.length];

        for(int i=0;i<data.length;i++){

            byte []bdata = data[i];

            byteBufs[i] = directByteBuf(bdata.length);
            byteBufs[i].writeBytes(bdata);
        }

        for(ByteBuf byteBuf : byteBufs){
            gasync.receive(null, byteBuf);
        }
    }
}
