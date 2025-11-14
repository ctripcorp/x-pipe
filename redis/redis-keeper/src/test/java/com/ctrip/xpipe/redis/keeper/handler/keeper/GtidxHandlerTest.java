package com.ctrip.xpipe.redis.keeper.handler.keeper;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.NettyPoolUtil;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.LenEofType;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.AbstractRedisKeeperTest;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.DefaultRedisClient;
import com.ctrip.xpipe.redis.keeper.protocal.cmd.GapAllowedSyncTest;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static com.ctrip.xpipe.redis.core.protocal.GapAllowedSync.DEFAULT_XSYNC_MAXGAP;

/**
 * @author TB
 * <p>
 * 2025/11/10 14:49
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class GtidxHandlerTest extends AbstractRedisKeeperTest {


    private DefaultGapAllowedSync gasync;
    private ReplicationStoreManager replicationStoreManager;
    private ReplicationStore replicationStore;
    private String replIdA = "000000000000000000000000000000000000000A";
    private String replIdB = "000000000000000000000000000000000000000B";
    private String replIdC = "000000000000000000000000000000000000000C";
    private String uuidB = "111111111111111111111111111111111111111B";
    private String uuidC = "111111111111111111111111111111111111111C";

    @Mock
    private RedisKeeperServer redisKeeperServer;

    @Mock
    private RedisClient redisClient;

    @Before
    public void before() throws Exception {
        replicationStoreManager = createReplicationStoreManager();
        LifecycleHelper.initializeIfPossible(replicationStoreManager);
        replicationStore = replicationStoreManager.create();

        Mockito.when(redisClient.getRedisServer()).thenReturn(redisKeeperServer);
        Mockito.when(redisKeeperServer.getReplicationStore()).thenReturn(replicationStore);

        SimpleObjectPool<NettyClient> clientPool = NettyPoolUtil.createNettyPool(new DefaultEndPoint("127.0.0.1", 1234));
        gasync = new DefaultGapAllowedSync(clientPool, new DefaultEndPoint("127.0.0.1", 1234), replicationStoreManager, scheduled, DEFAULT_XSYNC_MAXGAP);
        gasync.future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()){
                    logger.error("[operationComplete]", commandFuture.cause());
                }
            }
        });
    }


    @Test
    public void testRemoveGtidLost() throws Exception{
        int gnoBaseX = 1, gnoCountX = 100;
        setupReplicationStorePX(replIdA, 100000000, 1000,
                uuidB, replIdB, 200000000, 1, 100);

        long replOffC = 300000000;
        String gtidBaseRepr = uuidB + ":" + gnoBaseX + "-" + (gnoBaseX+2*gnoCountX-1);
        String gtidLostRepr = uuidC + ":" + gnoBaseX + "-" + (gnoBaseX+gnoCountX-1);
        String gtidContRepr = gtidBaseRepr + "," + gtidLostRepr;
        String reply = "+" + GapAllowedSync.XPARTIAL_SYNC + " " +
                AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLID + " " + replIdC + " " +
                AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_REPLOFF + " " + replOffC + " " +
                AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_MASTER_UUID + " " + uuidC + " " +
                AbstractGapAllowedSync.SyncReply.XSYNC_REPLY_OPT_GTID_SET + " " + gtidContRepr + " " +
                "\r\n";

        gasync.getRequest();

        byte[] rawCmds = generateGtidCommands(uuidC, gnoBaseX+gnoCountX, gnoCountX);
        runData(new byte[][]{
                reply.getBytes(),
                rawCmds
        });
        replicationStore = replicationStoreManager.getCurrent();
        Assert.assertEquals(replicationStore.getGtidSet().getValue().toString(), gtidLostRepr);
        new GtidxHandler().doHandle(new String[]{"remove","lost",uuidC,"1","100"}, redisClient);
        Assert.assertEquals(replicationStore.getGtidSet().getValue().toString(), "\"\"");
    }

    @Test
    public void testGtidSet(){
        GtidSet gtidSet = new GtidSet(Maps.newLinkedHashMap());
        GtidSet gtidSet1 = new GtidSet(Maps.newLinkedHashMap());
        gtidSet.compensate("a",1,10);
        System.out.println(gtidSet.itemCnt());
        gtidSet1.compensate("a",1,100);
        System.out.println(gtidSet1.itemCnt());
        System.out.println(gtidSet);
        System.out.println(gtidSet1);
        System.out.println(gtidSet.subtract(gtidSet1));
        System.out.println(gtidSet1.subtract(gtidSet));
    }

    private	void setupReplicationStorePX(String replidP, long replOffP, int cmdLenP,
                                            String uuidX, String replidX, long replOffX,
                                            int gnoBaseX, int gnoCountX) throws IOException {
        int gnoCmdX = gnoBaseX + gnoCountX;
        String gtidContRepr =  uuidX + ":" + gnoBaseX + "-" + (gnoBaseX + gnoCountX - 1);

        Assert.assertTrue(replicationStore.isFresh());
        Assert.assertNull(replicationStore.getMetaStore().getPreReplStage());
        Assert.assertNull(replicationStore.getMetaStore().getCurrentReplStage());

        RdbStore rdbStore = replicationStore.prepareRdb(replidP, replOffP, new LenEofType(0),
                ReplStage.ReplProto.PSYNC, null, null);
        rdbStore.updateRdbType(RdbStore.Type.NORMAL);
        rdbStore.updateRdbGtidSet(GtidSet.EMPTY_GTIDSET);
        replicationStore.confirmRdbGapAllowed(rdbStore);

        replicationStore.appendCommands(Unpooled.wrappedBuffer(generateVanillaCommands(cmdLenP)));

        replicationStore.switchToXSync(replidX,replOffX,uuidX,new GtidSet(gtidContRepr), null);

        replicationStore.appendCommands(Unpooled.wrappedBuffer(generateGtidCommands(uuidX, gnoCmdX, gnoCountX)));


    }

    private byte[] generateVanillaCommands(int contentLen) {
        return randomString(contentLen).getBytes();
    }

    private byte[] generateGtidCommands(String uuid, long startGno, int cmdCount) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        for (int i = 0; i < cmdCount; i++) {
            String uuidGno = uuid + ":" + String.valueOf(startGno+i);
            os.write("*6\r\n".getBytes());
            os.write("$4\r\nGTID\r\n".getBytes());
            os.write('$'); os.write(String.valueOf(uuidGno.length()).getBytes()); os.write("\r\n".getBytes()); os.write(uuidGno.getBytes()); os.write("\r\n".getBytes());
            os.write("$1\r\n0\r\n".getBytes());
            os.write("$3\r\nSET\r\n".getBytes());
            os.write("$3\r\nFOO\r\n".getBytes());
            os.write("$3\r\nBAR\r\n".getBytes());
        }
        return os.toByteArray();
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
