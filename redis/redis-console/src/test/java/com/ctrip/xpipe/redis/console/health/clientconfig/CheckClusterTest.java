package com.ctrip.xpipe.redis.console.health.clientconfig;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public class CheckClusterTest extends AbstractConsoleTest {

    private String clusterName = randomString(10);

    @Test
    public void testEquals() {

        CheckCluster cluster1 = new CheckCluster(clusterName);
        CheckCluster cluster2 = new CheckCluster(clusterName);

        cluster1.equals(cluster2);

        String[] shards = new String[]{"shard1", "shard2"};

        CheckShard shard1 = cluster1.getOrCreate(shards[0]);
        shard1.addRedis(new CheckRedis("127.0.0.1", 6379, "jq"));

        try {
            cluster1.equals(cluster2);
            Assert.fail();
        }catch (EqualsException e){
            logger.info("{}", e.getMessage());
        }

        CheckShard shard2 = cluster2.getOrCreate(shards[0]);
        try {
            cluster1.equals(cluster2);
            Assert.fail();
        }catch (EqualsException e){
            logger.info("{}", e.getMessage());
        }

        shard2.addRedis(new CheckRedis("127.0.0.1", 6379, "jq"));
        cluster1.equals(cluster2);

        shard2.clearRedises();
        shard2.addRedis(new CheckRedis("127.0.0.1", 6379, "oy"));
        try {
            cluster1.equals(cluster2);
            Assert.fail();
        }catch (EqualsException e){
            logger.info("{}", e.getMessage());
        }

    }

    @Test
    public void testEquals2() {

        CheckCluster cluster1 = new CheckCluster(clusterName);
        CheckCluster cluster2 = new CheckCluster(clusterName);

        String[] shards = new String[]{"shard1", "shard2"};

        cluster1.getOrCreate(shards[0]);
        cluster2.getOrCreate(shards[1]);

        try {
            cluster1.equals(cluster2);
            Assert.fail();
        }catch (EqualsException e){
            logger.info("{}", e.getMessage());
        }
    }

}
