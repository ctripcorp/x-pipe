package com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;
import org.junit.Assert;
import org.junit.Test;

public class XsyncForKeeperAndKeeperTest extends AbstractFakeRedisTest {

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

    public int writeCommands2() {
        int result = 0;
        for(int i = 622000; i < 622010; i++) {
            String cmd = "*6\r\n" +
                    "$4\r\n" +
                    "GTID\r\n" +
                    "$47\r\n" +
                    "b50c0ac6608a3351a6ed0c6a92d93ec736b390a1:" + i + "\r\n" +
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
    public void testKeeperConnect() throws Exception {

        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);

        Thread.sleep(1000);
        Assert.assertEquals(keeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2");

        RedisKeeperServer backUp = startRedisKeeperServer(100, 5000, 1000);
        backUp.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", keeperServer.getListeningPort()));

        sleep(2000);

        Assert.assertEquals(backUp.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2");
        Assert.assertEquals(backUp.getReplicationStore().getGtidSet().getValue().toString(), "\"\"");

        writeCommands();

        Thread.sleep(1000);

        Assert.assertEquals(keeperServer.getKeeperMonitor().getKeeperStats().getFullSyncCount(), 1);

        Assert.assertEquals(backUp.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009");
        Assert.assertEquals(backUp.getReplicationStore().getGtidSet().getValue().toString(), "\"\"");

    }

    @Test
    public void testKeeperContinueGap() throws Exception {

        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);

        Thread.sleep(1000);
        Assert.assertEquals(keeperServer.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2");

        RedisKeeperServer backUp = startRedisKeeperServer(100, 5000, 1000000000);
        backUp.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", keeperServer.getListeningPort()));
        writeCommands();
        sleep(1000);

        keeperServer.stop();

        Assert.assertEquals(backUp.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009");

        RedisKeeperServer keeperServer2 = startRedisKeeperServerAndConnectToFakeRedis(100, 5000, 1000000000);
        waitRedisKeeperServerConnected(keeperServer2);

        writeCommands2();
        Thread.sleep(1000);
        logger.info("wait data send writeCommands2");
        Assert.assertEquals(keeperServer2.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,b50c0ac6608a3351a6ed0c6a92d93ec736b390a1:622000-622009");
        writeCommands();
        logger.info("wait data send writeCommands");
        Thread.sleep(1000);
        Assert.assertEquals(keeperServer2.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,b50c0ac6608a3351a6ed0c6a92d93ec736b390a1:622000-622009,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009");
        backUp.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", keeperServer2.getListeningPort()));

        Thread.sleep(1000);
        Assert.assertEquals(keeperServer2.getKeeperMonitor().getKeeperStats().getFullSyncCount(), 0);
        Assert.assertEquals(backUp.getReplicationStore().getGtidSet().getValue().toString(), "b50c0ac6608a3351a6ed0c6a92d93ec736b390a1:622000-622009");
    }

    @Test
    public void testFull() throws Exception {

        TestKeeperConfig keeperConfig = new TestKeeperConfig(
                commandFileSize,
                1000,
                1000000, 1000000, 1000000);

        keeperConfig.setXsyncMaxGap(3);


        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);

        RedisKeeperServer backUp = startRedisKeeperServer(keeperConfig);
        backUp.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", keeperServer.getListeningPort()));
        writeCommands();

        sleep(2000);

        keeperServer.stop();

        RedisKeeperServer keeperServer2 = startRedisKeeperServerAndConnectToFakeRedis(100, 5000, 1000000000);
        waitRedisKeeperServerConnected(keeperServer2);

        writeCommands2();
        Thread.sleep(1000);
        logger.info("wait data send writeCommands2");
        Assert.assertEquals(keeperServer2.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,b50c0ac6608a3351a6ed0c6a92d93ec736b390a1:622000-622009");
        writeCommands();
        logger.info("wait data send writeCommands");
        Thread.sleep(1000);
        Assert.assertEquals(keeperServer2.getReplicationStore().getGtidSet().getKey().toString(), "7ca392ffb0fa8415cbf6a88bb7937f323c7367ac:1-2,b50c0ac6608a3351a6ed0c6a92d93ec736b390a1:622000-622009,a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622000-622009");
        backUp.getRedisKeeperServerState().becomeActive(new DefaultEndPoint("localhost", keeperServer2.getListeningPort()));

        Thread.sleep(1000);

        Assert.assertEquals(keeperServer2.getKeeperMonitor().getKeeperStats().getFullSyncCount(), 1);
        Assert.assertEquals(backUp.getReplicationStore().getGtidSet().getValue().toString(), "\"\"");



    }


}
