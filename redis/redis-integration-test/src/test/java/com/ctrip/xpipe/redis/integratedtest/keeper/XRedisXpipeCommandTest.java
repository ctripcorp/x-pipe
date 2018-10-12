package com.ctrip.xpipe.redis.integratedtest.keeper;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 22, 2018
 * test for xredis, not for keeper
 */
public class XRedisXpipeCommandTest extends AbstractKeeperIntegratedSingleDc {

    @Override
    protected boolean startServers() {
        return false;
    }

    @Test
    public void testRefullsync() throws Exception {

        RedisMeta redis = new RedisMeta().setIp("127.0.0.1").setPort(6379);
        RedisMeta redisSlave = new RedisMeta().setIp("127.0.0.1").setPort(6479);

        startRedis(redis);
        startRedis(redisSlave, redis);

        Integer beforeFullSync = getSyncFull(redis);
        String result = new RefullSync(redis.getIp(), redis.getPort(), scheduled).execute().get(1, TimeUnit.SECONDS);
        sleep(1500);
        Integer afterFullSync = getSyncFull(redis);

        Assert.assertEquals(beforeFullSync + 1, afterFullSync.intValue());
        sendMesssageToMasterAndTest(redis, Lists.newArrayList(redisSlave));

    }

    @Test
    public void testReplall() throws IOException {

        RedisMeta redis = new RedisMeta().setIp("127.0.0.1").setPort(6379);
        RedisMeta redisSlave = new RedisMeta().setIp("127.0.0.1").setPort(6479);
        RedisMeta redisSlaveSlave = new RedisMeta().setIp("127.0.0.1").setPort(6579);

        startRedis(redis);
        startRedis(redisSlave, redis);
        startRedis(redisSlaveSlave, redisSlave);

        Jedis jedisSlave = createJedis(redisSlave);
        Jedis jedisSlaveSlave = createJedis(redisSlaveSlave);

        jedisSlave.configSet("slave-read-only", "no");


        assertReplicate(jedisSlave, false, jedisSlaveSlave);

        jedisSlave.configSet("slave-repl-all", "yes");
        assertReplicate(jedisSlave, true, jedisSlaveSlave);

        jedisSlave.configSet("slave-repl-all", "no");
        assertReplicate(jedisSlave, false, jedisSlaveSlave);
    }

    private void assertReplicate(Jedis master, boolean replicate, Jedis... slaves) {

        String key = randomString(10), value = randomString();
        master.set(key, value);

        for (Jedis slave : slaves) {
            String valueSlave = slave.get(key);
            if (!replicate) {
                Assert.assertNull(valueSlave);
            } else {
                Assert.assertEquals(value, valueSlave);
            }
        }
    }

    private Integer getSyncFull(RedisMeta redis) throws Exception {

        SimpleObjectPool<NettyClient> keyPool = getXpipeNettyClientKeyedObjectPool().getKeyPool(new DefaultEndPoint(redis.getIp(), redis.getPort()));
        InfoCommand infoCommand = new InfoCommand(keyPool, InfoCommand.INFO_TYPE.STATS, scheduled);
        String value = infoCommand.execute().get();
        Integer sync_full = new InfoResultExtractor(value).extractAsInteger("sync_full");
        return sync_full;

    }

    public static class RefullSync extends AbstractRedisCommand<String> {

        public RefullSync(String host, int port, ScheduledExecutorService scheduled) {
            super(host, port, scheduled);
        }

        @Override
        public ByteBuf getRequest() {
            return new RequestStringParser("refullsync").format();
        }

        @Override
        protected String format(Object payload) {
            return payloadToString(payload);
        }
    }


}
