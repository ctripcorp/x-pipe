package com.ctrip.xpipe.redis.integratedtest.applier;


import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.applier.DefaultApplierServer;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Set;

import static com.ctrip.xpipe.redis.core.protocal.MASTER_STATE.REDIS_REPL_CONNECTED;


/**
 * @author TB
 * @date 2026/2/26 15:10
 */
public class ApplierServerToKeeperToFakeXsyncServerTest extends AbstractApplierIntegratedSingleDc {

    @Test
    public void testKeeperApplier2Redis() throws Exception {
        waitConditionUntilTimeOut(() -> 1 == server.slaveCount());
        for(KeeperMeta keeperMeta : getDcKeepers(dc, getClusterId(), getShardId())) {
            waitConditionUntilTimeOut(() -> getRedisKeeperServer(keeperMeta).getRedisMaster().getMasterState().equals(REDIS_REPL_CONNECTED));
        }
        server.propagate("multi");
        server.propagate("incr in");
        server.propagate("GTID f32e4ba72875a76bfd1c92ea1857c4755ea0d680:1 0 exec");

        server.propagate("GTID f32e4ba72875a76bfd1c92ea1857c4755ea0d680:2 0 hset h1 f1 v1 f2 v2");
        server.propagate("GTID f32e4ba72875a76bfd1c92ea1857c4755ea0d680:3 0 zadd z1 1 v1 2 v2");


        sleep(10000);
        List<RedisMeta>  redisMetas = getDcRedises(dc,getClusterId(),getShardId());
        int keySize=0;
        int bigListLen = 0;
        int bigHashLen = 0;
        int bigSetLen = 0;
        int bigNormalSet1Len = 0;
        int bigNormalListLen = 0;
        int bigNormalHashLen = 0;
        int bigZsetLen = 0;
        int bigNormalSetLen = 0;
        int h1Len = 0;
        int z1Len = 0;
        String count ="";
        for(RedisMeta redisMeta:redisMetas){
            Jedis jedis = new Jedis(redisMeta.getIp(),redisMeta.getPort());
            Set<String> keys = jedis.keys("*");
            keySize += keys.size();
            bigListLen += jedis.llen("biglist");
            bigHashLen += jedis.hlen("bighash");
            bigSetLen += jedis.scard("bigset");
            bigNormalSet1Len += jedis.scard("bignormalset1");
            bigNormalListLen += jedis.llen("bignormallist");
            bigNormalHashLen += jedis.hlen("bignormalhash");
            bigZsetLen += jedis.zcard("bigzset");
            bigNormalSetLen += jedis.zcard("bignormalset");
            h1Len += jedis.hlen("h1");
            z1Len += jedis.zcard("z1");
            String in = jedis.get("in");
            if(in != null){
                count = in;
            }
        }
        Assert.assertEquals(11,keySize);
        Assert.assertEquals(3,bigListLen);
        Assert.assertEquals(3,bigHashLen);
        Assert.assertEquals(3,bigSetLen);
        Assert.assertEquals(3,bigNormalSet1Len);
        Assert.assertEquals(3,bigNormalListLen);
        Assert.assertEquals(3,bigNormalHashLen);
        Assert.assertEquals(3,bigZsetLen);
        Assert.assertEquals(3,bigNormalSetLen);
        Assert.assertEquals(2,h1Len);
        Assert.assertEquals(2,z1Len);
        Assert.assertEquals("1",count);

        applier.stop();
    }

}
