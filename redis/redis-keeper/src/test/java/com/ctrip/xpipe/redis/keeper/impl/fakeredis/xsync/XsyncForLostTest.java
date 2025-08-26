package com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.cmd.InMemoryGapAllowedSync;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class XsyncForLostTest extends AbstractFakeRedisTest {

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

    public int writeCommand2() {
        int result = 0;
        for(int i = 622000; i < 622610; i++) {
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
    public void testLost() throws Exception {
        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);

        Thread.sleep(1000);
        Assert.assertEquals(keeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2");

        System.out.println("KEEPER LOST: " + keeperServer.getReplicationStore().getGtidSet().getValue());

        InMemoryGapAllowedSync allowedSync = new InMemoryGapAllowedSync("127.0.0.1", keeperServer.getListeningPort(),  true, scheduled);

        allowedSync.setXsyncRequest(new GtidSet("7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009"), () -> 100);

        allowedSync.execute();

        Thread.sleep(2000);

        writeCommands();

        Thread.sleep(2000);

        Assert.assertFalse(keeperServer.getReplicationStore().getGtidSet().getKey().contains("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0"));
        Assert.assertTrue(keeperServer.getReplicationStore().getGtidSet().getValue().contains("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0"));

    }

    @Test
    public void testLostConflict() throws Exception {
        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);

        Thread.sleep(1000);
        Assert.assertEquals(keeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2");

        InMemoryGapAllowedSync allowedSync = new InMemoryGapAllowedSync("127.0.0.1", keeperServer.getListeningPort(),  true, scheduled);

        allowedSync.setXsyncRequest(new GtidSet("7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622509"), () -> 100000);


        CountDownLatch countDownLatch = new CountDownLatch(2);

        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            allowedSync.execute();
            countDownLatch.countDown();
        }).start();

        new Thread(() -> {
            logger.info("start write commands");
            writeCommand2();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            logger.info("end write commands");
            countDownLatch.countDown();
        }).start();

        countDownLatch.await();

        System.out.println("KEEPER LOST: " + keeperServer.getReplicationStore().getGtidSet().getKey());
        System.out.println("KEEPER LOST: " + keeperServer.getReplicationStore().getGtidSet().getValue());

        Assert.assertTrue(keeperServer.getReplicationStore().getGtidSet().getKey().contains("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0"));
        Assert.assertFalse(keeperServer.getReplicationStore().getGtidSet().getValue().contains("a50c0ac6608a3351a6ed0c6a92d93ec736b390a0"));

    }
}
