package com.ctrip.xpipe.redis.keeper.applier;

import com.ctrip.xpipe.api.cluster.LeaderElector;
import com.ctrip.xpipe.api.cluster.LeaderElectorManager;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.redis.parser.AbstractRedisOpParserTest;
import com.ctrip.xpipe.redis.core.server.FakeXsyncServer;
import com.ctrip.xpipe.redis.core.store.ClusterId;
import com.ctrip.xpipe.redis.core.store.ShardId;
import com.ctrip.xpipe.redis.keeper.config.TestKeeperConfig;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Set;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import redis.embedded.RedisServer;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 21:05
 */
@RunWith(MockitoJUnitRunner.class)
public class ApplierServerToFakeXsyncServerTest extends AbstractRedisOpParserTest {

    private FakeXsyncServer server;

    private DefaultApplierServer applier;

    private ApplierMeta applierMeta;

    @Mock
    private LeaderElectorManager leaderElectorManager;

    @Mock
    private LeaderElector leaderElector;

    private Jedis jedis;

    private RedisServer redisServer;

    @Before
    public void setUp() throws Exception {
//        executeScript("kill_server.sh", String.valueOf(6379));
//        executeScript("start_redis.sh");
        redisServer = new RedisServer(6379);
        redisServer.start();
        jedis = new Jedis("127.0.0.1",6379);
        jedis.flushAll();

        server = startFakeXsyncServer(randomPort(), null);
        applierMeta = new ApplierMeta();
        applierMeta.setPort(randomPort());
        leaderElectorManager = Mockito.mock(LeaderElectorManager.class);
        leaderElector = Mockito.mock(LeaderElector.class);
        when(leaderElectorManager.createLeaderElector(any())).thenReturn(leaderElector);

        applier = new DefaultApplierServer(
                "ApplierTest",
                ClusterId.from(1L), ShardId.from(1L),
                applierMeta, leaderElectorManager, parser, new TestKeeperConfig(),1,2,
                50000l, 167772160l, 10l, 10000l, null,2);
        applier.initialize();
        applier.start();

        ApplierConfig config = new ApplierConfig();
        config.setDropAllowKeys(-1);
        config.setDropAllowRation(-1);
        config.setUseXsync(true);
        applier.setStateActive(new DefaultEndPoint("127.0.0.1", server.getPort()), new GtidSet("a1:1-10:15-20,b1:1-8"), config);
    }

    @After
    public void stopRedis() throws IOException {
        redisServer.stop();
    }

    @Test
    public void test() throws Exception {
        waitConditionUntilTimeOut(() -> 1 == server.slaveCount());

        server.propagate("hset h1 f1 v1 f2 v2");
        server.propagate("zadd z1 1 v1 2 v2");

        server.propagate("multi");
        server.propagate("incr in");
        server.propagate("exec");

        server.propagate("gtid a1:21 set k1 v1");
        server.propagate("gtid a1:22 mset k1 v1 k2 v2");
        server.propagate("gtid a1:23 del k1 k2");

        server.propagate("gtid a1:24 set k3 v3");
        server.propagate("gtid a1:25 set k4 v4");
        server.propagate("gtid a1:26 set k1 v6");

        server.propagate("MULTI");
        server.propagate("set k13 v13");
        server.propagate("set k14 v14");
        server.propagate("set k15 v15");
        server.propagate("GTID a1:28");

        server.propagate("gtid a1:27 set k1 v7");

        sleep(2000);


        Set<String> keys = jedis.keys("*");
        Assert.assertEquals(11,keys.size());
        long len;
        len = jedis.llen("biglist");
        Assert.assertEquals(3,len);
        len = jedis.hlen("bighash");
        Assert.assertEquals(3,len);
        len = jedis.scard("bigset");
        Assert.assertEquals(3,len);
        len = jedis.scard("bignormalset1");
        Assert.assertEquals(3,len);
        len = jedis.llen("bignormallist");
        Assert.assertEquals(3,len);
        len = jedis.hlen("bignormalhash");
        Assert.assertEquals(3,len);
        len = jedis.zcard("bigzset");
        Assert.assertEquals(3,len);
        len = jedis.zcard("bignormalset");
        Assert.assertEquals(3,len);
        len = jedis.hlen("h1");
        Assert.assertEquals(2,len);
        len = jedis.zcard("z1");
        Assert.assertEquals(2,len);
        String count = jedis.get("in");
        Assert.assertEquals("1",count);

        applier.stop();
    }


    @Test
    public void testMulti() throws Exception {
        waitConditionUntilTimeOut(() -> 1 == server.slaveCount());

        server.propagate("hset h1 f1 v1 f2 v2");
        server.propagate("zadd z1 1 v1 2 v2");

        server.propagate("multi");
        server.propagate("del h1");
        server.propagate("hset h1 f1 v11 f2 v22");
        server.propagate("expire h1 300");
        server.propagate("exec");

        server.propagate("multi");
        server.propagate("zadd z1 3 v1 4 v2");
        server.propagate("exec");

        server.propagate("multi");
        server.propagate("del {tag}htag1");
        server.propagate("hset {tag}htag2 f1 v11 f2 v22");
        server.propagate("exec");

        server.propagate("multi");
        server.propagate("incr in");
        server.propagate("exec");

        sleep(3000);

        Set<String> keys = jedis.keys("*");
        Assert.assertEquals(12,keys.size());
        long len;
        len = jedis.llen("biglist");
        Assert.assertEquals(3,len);
        len = jedis.hlen("bighash");
        Assert.assertEquals(3,len);
        len = jedis.scard("bigset");
        Assert.assertEquals(3,len);
        len = jedis.scard("bignormalset1");
        Assert.assertEquals(3,len);
        len = jedis.llen("bignormallist");
        Assert.assertEquals(3,len);
        len = jedis.hlen("bignormalhash");
        Assert.assertEquals(3,len);
        len = jedis.zcard("bigzset");
        Assert.assertEquals(3,len);
        len = jedis.zcard("bignormalset");
        Assert.assertEquals(3,len);

        len = jedis.hlen("h1");
        Assert.assertEquals(2,len);
        String f1 = jedis.hget("h1","f1");
        Assert.assertEquals("v11",f1);
        String f2 = jedis.hget("h1","f2");
        Assert.assertEquals("v22",f2);

        len = jedis.hlen("{tag}htag2");
        Assert.assertEquals(2,len);
        String f11 = jedis.hget("{tag}htag2","f1");
        Assert.assertEquals("v11",f11);
        String f22 = jedis.hget("{tag}htag2","f2");
        Assert.assertEquals("v22",f22);

        long ttl = jedis.ttl("h1");
        Assert.assertNotEquals(-1,ttl);

        len = jedis.zcard("z1");
        Assert.assertEquals(2,len);
        double score1 = jedis.zscore("z1","v1");
        Assert.assertEquals(3.0,score1,1e-6);
        double score2 = jedis.zscore("z1","v2");
        Assert.assertEquals(4.0,score2,1e-6);

        String count = jedis.get("in");
        Assert.assertEquals("1",count);

        applier.stop();
    }



    @Test
    public void testRedis8() throws Exception {
        waitConditionUntilTimeOut(() -> 1 == server.slaveCount());

        sleep(3000);

        Set<String> keys = jedis.keys("*");
        Assert.assertEquals(8,keys.size());
        long len;
        len = jedis.llen("biglist");
        Assert.assertEquals(3,len);
        len = jedis.hlen("bighash");
        Assert.assertEquals(3,len);
        len = jedis.scard("bigset");
        Assert.assertEquals(3,len);
        len = jedis.scard("bignormalset1");
        Assert.assertEquals(3,len);
        len = jedis.llen("bignormallist");
        Assert.assertEquals(3,len);
        len = jedis.hlen("bignormalhash");
        Assert.assertEquals(3,len);
        len = jedis.zcard("bigzset");
        Assert.assertEquals(3,len);
        len = jedis.zcard("bignormalset");
        Assert.assertEquals(3,len);

        applier.stop();
    }

}
