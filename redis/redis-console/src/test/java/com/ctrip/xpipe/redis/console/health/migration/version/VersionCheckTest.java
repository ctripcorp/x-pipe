package com.ctrip.xpipe.redis.console.health.migration.version;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 * <p>
 * Sep 22, 2017
 */
@RunWith(SpringJUnit4ClassRunner.class)
public class VersionCheckTest extends AbstractConsoleIntegrationTest {

    @Autowired
    VersionCheck versionCheck;

    @Autowired
    MetaCache metaCache;

    private static final String HOST = "10.3.2.23";
    private static final int PORT = 6379;

    @Before
    public void beforeVersionCheckTest() {
        DcMeta dcMeta = new DcMeta();
        ClusterMeta clusterMeta = new ClusterMeta();
        ShardMeta shardMeta = new ShardMeta();
        shardMeta.addRedis(new RedisMeta().setIp(HOST).setPort(PORT).setParent(shardMeta));
        clusterMeta.addShard(shardMeta);
        dcMeta.addCluster(clusterMeta);
//        xpipeMeta.addDc(dcMeta);
        System.out.println(metaCache.getXpipeMeta());
        System.out.println(dcMeta);
        metaCache.getXpipeMeta().addDc(dcMeta);
    }

    @Test
    public void doCheck() throws Exception {
        System.out.println(metaCache.getXpipeMeta().getDcs());
//        metaCache.getXpipeMeta().addDc(new DcMeta());

    }

    @After
    public void afterVersionCheckTest() throws IOException {
        waitForAnyKey();
    }

}