package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.healthcheck.HealthChecker;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class DefaultHealthCheckerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private DefaultHealthChecker healthChecker;

    @Autowired
    private MetaCache metaCache;

    @BeforeClass
    public static void setUpDefaultHealthCheckerTest() {
        System.setProperty(HealthChecker.ENABLED, "true");
    }

    @Test
    public void testDefaultHealthCheckerTest() throws Exception {
        XpipeMeta meta = new XpipeMeta();
        DcMeta local = newDcMeta(FoundationService.DEFAULT.getDataCenter());
        meta.addDc(local);
        DcMeta target = newDcMeta("target");
        meta.addDc(target);

        Thread.sleep(1000 * 60);
    }

    private DcMeta newDcMeta(String dcId) {
        DcMeta dcMeta = new DcMeta().setId(dcId);
        ClusterMeta clusterMeta = new ClusterMeta().setId("cluster"+randomString(3)).setParent(dcMeta);
        dcMeta.addCluster(clusterMeta);
        ShardMeta shardMeta = new ShardMeta().setParent(clusterMeta).setId("shard" + randomString(3));
        clusterMeta.addShard(shardMeta);
        RedisMeta redisMeta = new RedisMeta().setParent(shardMeta).setIp("localhost").setPort(randomPort());
        shardMeta.addRedis(redisMeta);
        return dcMeta;
    }
}