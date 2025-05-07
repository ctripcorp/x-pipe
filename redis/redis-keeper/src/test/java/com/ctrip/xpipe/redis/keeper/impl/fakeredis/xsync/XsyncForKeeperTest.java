package com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync;

import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class XsyncForKeeperTest extends AbstractFakeRedisTest {

    @Override
    protected String getProto() {
        return "xsync";
    }

    public void writeCommand() throws IOException {
        for(int i = 622000; i < 622100; i++) {
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
            fakeRedisServer.propagate(cmd);
        }
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


        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis();
        waitRedisKeeperServerConnected(keeperServer);

        writeCommand();

        Thread.sleep(3000);

        Assert.assertEquals(keeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622099");

    }

    @Test
    public void testKeeperConnectMaxContinue() throws Exception {


        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis();
        waitRedisKeeperServerConnected(keeperServer);

        Thread.sleep(1000);

        // 异步调用 restartKeeperServer
        CompletableFuture<RedisKeeperServer> futureKeeperServer = CompletableFuture.supplyAsync(() -> {
            try {
                return restartKeeperServer(keeperServer, 1, 10);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        writeCommand();

        RedisKeeperServer newKeeperServer = futureKeeperServer.get(); // 阻塞等待 CompletableFuture 完成
        connectToFakeRedis(newKeeperServer);
        waitRedisKeeperServerConnected(newKeeperServer);

        int length =  writeCommands();

        Thread.sleep(1000);


        System.out.println(newKeeperServer.getReplicationStore().getGtidSet().getValue().toString());
        Assert.assertEquals(newKeeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009");

        Assert.assertEquals(newKeeperServer.getReplicationStore().getGtidSet().getValue().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:3-21,1777955e932bed5eb321a58fbc2132cba48f026f:1-2");
        Assert.assertEquals(length + 589, newKeeperServer.getReplicationStore().getCurReplStageReplOff());
    }
}
