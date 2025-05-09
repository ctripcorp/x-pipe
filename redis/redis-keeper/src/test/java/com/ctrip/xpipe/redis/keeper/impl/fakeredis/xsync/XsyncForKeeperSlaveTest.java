package com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryGapAllowedSync;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;


public class XsyncForKeeperSlaveTest extends AbstractFakeRedisTest {


    @Override
    protected String getProto() {
        return "xsync";
    }

    public int writeCommands() {
        int result = 0;
        for(int i = 622000; i < 622010; i++) {
            String cmd = "*6\r\n" +
                    "$4\r\n" +
                    "GTID\r\n" +
                    "$47\r\n" +
                    "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:" + i + "\r\n" +
                    "$1\r\n" +
                    "0\r\n" +
                    "$3\r\n" +
                    "SET\r\n" +
                    "$16\r\n" +
                    "key:__rand_int__\r\n" +
                    "$3\r\n" +
                    "VXK\r\n";
            result += cmd.getBytes().length;
            fakeRedisServer.propagate(cmd);
        }
        return result;
    }



    @Test
    public void testKeeperConnectRedis() throws Exception {

        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);

        Thread.sleep(1000);

        long dataLength = writeCommands();

        Thread.sleep(1000);
        Assert.assertEquals(keeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009");


        InMemoryGapAllowedSync allowedSync = new InMemoryGapAllowedSync("127.0.0.1", keeperServer.getListeningPort(),  true, scheduled);

        allowedSync.setPsyncFull();
        allowedSync.future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()){
                    logger.error("[operationComplete]", commandFuture.cause());
                }
            }
        });

        allowedSync.execute();


        Thread.sleep(3000);
        Assert.assertEquals(1, allowedSync.getFullSyncCnt());

        long replOff = allowedSync.getReplOffset();
        Assert.assertEquals(replOff, dataLength + 1);

    }

    @Test
    public void testKeeperConnectRedisContinue() throws Exception {

        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);

        Thread.sleep(1000);

        long dataLength = writeCommands();

        Thread.sleep(1000);
        Assert.assertEquals(keeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009");


        InMemoryGapAllowedSync allowedSync = new InMemoryGapAllowedSync("127.0.0.1", keeperServer.getListeningPort(),  true, scheduled);

        allowedSync.setXsyncRequest(new GtidSet("7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622001"), () -> 10);
        allowedSync.future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()){
                    logger.error("[operationComplete]", commandFuture.cause());
                }
            }
        });

        allowedSync.execute();


        Thread.sleep(3000);
        Assert.assertEquals(0, allowedSync.getFullSyncCnt());

        long replOff = allowedSync.getReplOffset();
        Assert.assertEquals(replOff, dataLength + 1);

    }

    @Test
    public void testKeeperConnectRedisNotContinue() throws Exception {

        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);

        Thread.sleep(1000);

        long dataLength = writeCommands();

        Thread.sleep(1000);
        Assert.assertEquals(keeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009");


        InMemoryGapAllowedSync allowedSync = new InMemoryGapAllowedSync("127.0.0.1", keeperServer.getListeningPort(),  true, scheduled);

        allowedSync.setXsyncRequest(new GtidSet("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622003"), () -> 1);
        allowedSync.future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if(!commandFuture.isSuccess()){
                    logger.error("[operationComplete]", commandFuture.cause());
                }
            }
        });

        allowedSync.execute();


        Thread.sleep(3000);
        Assert.assertEquals(1, allowedSync.getFullSyncCnt());

        long replOff = allowedSync.getReplOffset();
        Assert.assertEquals(replOff, dataLength + 1);

    }



}
