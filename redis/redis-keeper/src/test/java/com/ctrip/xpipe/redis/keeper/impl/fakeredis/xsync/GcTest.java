package com.ctrip.xpipe.redis.keeper.impl.fakeredis.xsync;

import com.ctrip.xpipe.redis.keeper.AbstractFakeRedisTest;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import org.junit.Test;

public class GcTest extends AbstractFakeRedisTest {

    @Override
    protected String getProto() {
        return "xsync";
    }

    @Test
    public void testGc() throws Exception {

        RedisKeeperServer keeperServer = startRedisKeeperServerAndConnectToFakeRedis(100, 5000);
        waitRedisKeeperServerConnected(keeperServer);
        Thread.sleep(1000);

        String cmd = "*6\r\n" +
                "$4\r\n" +
                "GTID\r\n" +
                "$47\r\n" +
                "a50c0ac6608a3351a6ed0c6a92d93ec736b390a0:622100\r\n" +
                "$1\r\n" +
                "0\r\n" +
                "$3\r\n" +
                "SET\r\n" +
                "$16\r\n" +
                "key:__rand_int__\r\n" +
                "$3\r\n" +
                "VXK\r\n";
        fakeRedisServer.propagate(cmd);

        Thread.sleep(10000000);
    }
}
