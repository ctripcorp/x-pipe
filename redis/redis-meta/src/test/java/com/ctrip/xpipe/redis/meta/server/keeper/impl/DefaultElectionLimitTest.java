package com.ctrip.xpipe.redis.meta.server.keeper.impl;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerContextTest;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.keeper.ElectionLimit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author yu
 *
 * 2023/9/7
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultElectionLimitTest extends AbstractMetaServerContextTest {

    @Mock
    private ElectionLimit electionLimit;

    @Mock
    private MetaServerConfig config;

    @Before
    public void beforeDefaultElectionLimitTest() {
        MockitoAnnotations.initMocks(this);
        electionLimit = getBean(DefaultElectionLimit.class);
        logger.info("{}", electionLimit);

    }

    @Test
    public void testTryAcquire() {
        boolean res = electionLimit.tryAcquire("cluster_1,shard_1");
        Assert.assertEquals(true, res);
        res = electionLimit.tryAcquire("cluster_1,shard_1");
        Assert.assertEquals(false, res);
        res = electionLimit.tryAcquire("cluster_1,shard_1");
        Assert.assertEquals(false, res);
        res = electionLimit.tryAcquire("cluster_1,shard_2");
        Assert.assertEquals(true, res);
        sleep(1000);
        res = electionLimit.tryAcquire("cluster_1,shard_1");
        Assert.assertEquals(true, res);
    }

}