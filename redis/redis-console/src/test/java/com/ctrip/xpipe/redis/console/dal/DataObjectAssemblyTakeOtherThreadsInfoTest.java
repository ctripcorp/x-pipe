package com.ctrip.xpipe.redis.console.dal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * @author chen.zhu
 * <p>
 * Nov 20, 2017
 */
public class DataObjectAssemblyTakeOtherThreadsInfoTest extends AbstractConsoleIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DataObjectAssemblyTakeOtherThreadsInfoTest.class);

    private XpipeMeta meta1;

    private XpipeMeta meta2;

    @Before
    public void beforeDoaTakeOtherThreadsInfoTest() throws Exception {
        meta1 = new XpipeMetaGenerator(2000).generateXpipeMeta();
        meta2 = new XpipeMetaGenerator(2000).generateXpipeMeta();
        startH2Server();
    }

    @Test
    public void testDOATakeOtherThreadsObject() {
        XPipeMetaVisitor visitor = new XPipeMetaVisitor(meta1);
//        visitor.visitXpipe(meta1);
        logger.info("[test] \n {}", meta1.toString());
    }


    class XpipeMetaGenerator {

        int clusterNum;

        Random random;

        XpipeMetaGenerator(int n) {
            clusterNum = n;
            random = new Random();
        }

        XpipeMeta generateXpipeMeta() {
            XpipeMeta result = new XpipeMeta();
            DcMeta dc1 = new DcMeta("1"), dc2 = new DcMeta("2");
            result.addDc(dc1);
            result.addDc(dc2);
            for(int i = 0; i < clusterNum; i++) {
                ClusterMeta cluster = new ClusterMeta("" + i);
                String activeDc = (i & 1) == 0 ? "1" : "2";
                String backupDcs = (i & 1) == 0 ? "2" : "1";
                cluster.setActiveDc(activeDc);
                cluster.setBackupDcs(backupDcs);
                dc1.addCluster(cluster);
                dc2.addCluster(cluster);
                generateShard(cluster);
            }
            return result;
        }

        void generateShard(ClusterMeta cluster) {
            ShardMeta shard = new ShardMeta(cluster.getId() + "1");
            for(int i = 0; i < 2; i ++)
                shard.addRedis(generateRedis(shard));
            shard.setParent(cluster);
            cluster.addShard(shard);
        }

        RedisMeta generateRedis(ShardMeta shard) {
            RedisMeta redis = new RedisMeta();
            redis.setIp(randomIP());
            redis.setPort(random.nextInt(10000));
            redis.setParent(shard);
            redis.setId(shard.getId() + random.nextInt(10));
            return redis;
        }

        String randomIP() {
            StringBuilder sb = new StringBuilder();
            sb.append(random.nextInt(255)).append(".")
                    .append(random.nextInt(255)).append(".")
                    .append(random.nextInt(255)).append(".")
                    .append(random.nextInt(255));
            return sb.toString();
        }
    }
}
