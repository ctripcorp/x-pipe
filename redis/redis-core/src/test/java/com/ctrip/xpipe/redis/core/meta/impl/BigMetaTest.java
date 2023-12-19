package com.ctrip.xpipe.redis.core.meta.impl;

import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * @author wenchao.meng
 *         <p>
 *         Dec 29, 2017
 */
public class BigMetaTest extends AbstractRedisTest {

    private int dcCount = 2;
    private int clusterCount = 500;
    private int eachShardCount = 10;

    private int port = 0;

    @Test
    public void testCloneTime() throws InterruptedException {

        XpipeMeta meta = genXpipeMeta(dcCount, clusterCount, eachShardCount);

        ExecutorService executors = DefaultExecutorFactory.createAllowCoreTimeout(getTestName()).createExecutorService();

        int count = 400;
        long start = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            executors.execute(new Runnable() {
                @Override
                public void run() {
                    try{
                        XpipeMeta clone = MetaCloneFacade.INSTANCE.clone(meta);
                    }finally {
                        latch.countDown();
                    }

                }
            });
        }

        latch.await();
        long end = System.currentTimeMillis();

        logger.info("{} us", (end - start) * 1000 / count);

    }

    private XpipeMeta genXpipeMeta(int dcCount, int clusterCount, int eachShardCount) {

        XpipeMeta xpipeMeta = new XpipeMeta();

        for (int i = 0; i < dcCount; i++) {

            DcMeta dcMeta = new DcMeta().setId("dc" + i);
            xpipeMeta.addDc(dcMeta);
            for (int j = 0; j < clusterCount; j++) {

                ClusterMeta clusterMeta = new ClusterMeta().setId("cluster:" + j);
                dcMeta.addCluster(clusterMeta);
                for (int k = 0; k < eachShardCount; k++) {

                    ShardMeta shardMeta = new ShardMeta().setId("shard:" + k);
                    clusterMeta.addShard(shardMeta);
                    for (int r = 0; r < 4; r++) {
                        shardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(port++));
                    }
                }

            }
        }
        return xpipeMeta;
    }
}
