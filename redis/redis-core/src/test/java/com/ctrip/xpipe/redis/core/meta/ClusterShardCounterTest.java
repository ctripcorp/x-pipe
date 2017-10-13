package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 21, 2017
 */
public class ClusterShardCounterTest extends AbstractRedisTest {

    @Test
    public void testCount(){
        XpipeMeta xpipeMeta = getXpipeMeta();

        ClusterShardCounter counter = new ClusterShardCounter();
        xpipeMeta.accept(counter);

        logger.info("{}", counter.getClusters());

        Assert.assertEquals(2, counter.getClusterCount());
        Assert.assertEquals(2, counter.getShardCount());
    }


    @Override
    protected String getXpipeMetaConfigFile() {
        return "cluster_shard_counter.xml";
    }
}
