package com.ctrip.xpipe.redis.console.dal;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author chen.zhu
 * <p>
 * Nov 20, 2017
 */
public class DataObjectAssemblyTakeOtherThreadsInfoTest extends AbstractConsoleIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(DataObjectAssemblyTakeOtherThreadsInfoTest.class);

    private XpipeMeta meta1;

    private static final int N = 20;

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private DcService dcService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private RedisService redisService;


    ExecutorService executor = Executors.newFixedThreadPool(N);


    @Before
    public void beforeDoaTakeOtherThreadsInfoTest() throws Exception {
        meta1 = new XpipeMetaGenerator(2000).generateXpipeMeta();
    }

    @Test
    public void testDOATakeOtherThreadsObject() throws Exception {
        List<Future> futures2 = new LinkedList<>();
        insertXPipeMetaIntoDataBase(meta1, futures2);
        DcMeta dcMeta = dcMetaService.getDcMeta(dcNames[0]);
        logger.info("[test] \n {}", dcMeta.toString());

        CyclicBarrier barrier = new CyclicBarrier(N);
        List<Future<DcMeta>> futures = new LinkedList<>();
        for(int i = 0; i < N; i ++) {
            futures.add(getDcMetas(barrier));
        }

        for(Future<DcMeta> future : futures) {
            DcMeta meta = future.get();
            logger.info("{}", meta);
            Assert.assertTrue(meta.equals(dcMeta));
            Assert.assertEquals(meta.getClusters().size(), dcMeta.getClusters().size());

            for(ClusterMeta cluster : meta.getClusters().values()) {
                Assert.assertTrue(cluster.equals(dcMeta.findCluster(cluster.getId())));
                Assert.assertEquals(cluster.getShards().size(), dcMeta.findCluster(cluster.getId()).getShards().size());

                for(ShardMeta shard : cluster.getShards().values()) {
                    ShardMeta sampleShard = dcMeta.findCluster(cluster.getId()).findShard(shard.getId());

                    Assert.assertTrue(shard.equals(sampleShard));
                    Assert.assertEquals(sampleShard.getRedises().size(), shard.getRedises().size());

                    List<RedisMeta> sampleRedises = sampleShard.getRedises();

                    for(RedisMeta redisMeta : shard.getRedises()) {
                        if(!isIn(redisMeta, sampleRedises)) {
                            logger.info("{}", shard);
                            logger.info("{}", sampleShard);
                        }
                        Assert.assertTrue(isIn(redisMeta, sampleRedises));
                    }
                }
            }
        }
    }

    boolean isIn(RedisMeta redisMeta, List<RedisMeta> redisMetas) {
        for(RedisMeta meta : redisMetas) {
            if(redisMeta.getIp().equalsIgnoreCase(meta.getIp()) && redisMeta.getPort().equals(meta.getPort()))
                return true;
        }
        return false;
    }


    private Future<DcMeta> getDcMetas(CyclicBarrier barrier) {
        Future<DcMeta> future_allDetails = executor.submit(new Callable<DcMeta>() {
            @Override
            public DcMeta call() throws Exception {
                barrier.await();
                return dcMetaService.getDcMeta(dcNames[0]);
            }
        });
        return future_allDetails;
    }


    private void insertXPipeMetaIntoDataBase(XpipeMeta xpipeMeta, List<Future> futures2) throws Exception {
        for(Map.Entry<String, DcMeta> dcEntry : xpipeMeta.getDcs().entrySet()) {
            DcMeta dcMeta = dcEntry.getValue();
            for(Map.Entry<String, ClusterMeta> clusterMetaEntry : dcMeta.getClusters().entrySet()) {
                futures2.add(executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        doExecute(clusterMetaEntry.getValue(), dcMeta.getId());
                    }
                }));
            }
        }
        for(Future future : futures2) {
            future.get();
        }
    }

    private void doExecute(ClusterMeta clusterMeta, String dcId) {
        ClusterTbl clusterTbl1 = clusterService.find(clusterMeta.getId());
        if(clusterTbl1 != null) {
            return;
        }
        ClusterTbl clusterTbl = clusterService.createCluster(buildClusterModel(clusterMeta));
        for(Map.Entry<String, ShardMeta> shardMetaEntry : clusterMeta.getShards().entrySet()) {
            ShardMeta shardMeta = shardMetaEntry.getValue();
            shardService.createShard(clusterMeta.getId(), buildShardTbl(shardMeta, clusterTbl), Collections.emptyMap());
            List<Pair<String, Integer>> pairs = new ArrayList<>();
            for(RedisMeta redisMeta : shardMeta.getRedises()) {
                pairs.add(new Pair<> (redisMeta.getIp(), redisMeta.getPort()));
            }


            try {
                redisService.insertRedises(dcId, clusterMeta.getId(), shardMeta.getId(), pairs);
            } catch (DalException e) {
                e.printStackTrace();
            } catch (ResourceNotFoundException e) {
                e.printStackTrace();
            }

        }

    }

    private ClusterModel buildClusterModel(ClusterMeta clusterMeta) {
        ClusterModel clusterModel = new ClusterModel();
        DcTbl dcTbl0 = dcService.find(dcNames[0]);
        DcTbl dcTbl1 = dcService.find(dcNames[1]);
        clusterModel.setClusterTbl(new ClusterTbl()
                .setActivedcId(clusterMeta.getActiveDc().equalsIgnoreCase(dcNames[0]) ? dcTbl0.getId() : dcTbl1.getId())
                .setClusterName(clusterMeta.getId())
                .setClusterType(ClusterType.ONE_WAY.toString())
                .setClusterDescription("test")
                .setClusterAdminEmails("admin@ctrip.com")
                .setOrganizationInfo(new OrganizationTbl().setId(2L).setOrgId(3))
                .setClusterOrgName("org-2"));
        clusterModel.setDcs(Arrays.asList(dcTbl0, dcTbl1));
        return clusterModel;
    }

    private ShardTbl buildShardTbl(ShardMeta shardMeta, ClusterTbl clusterTbl) {
        ShardTbl shardTbl = new ShardTbl();
        shardTbl.setClusterId(clusterTbl.getId())
                .setClusterName(clusterTbl.getClusterName())
                .setShardName(shardMeta.getId())
                .setDeleted(false)
                .setSetinelMonitorName(clusterTbl.getClusterName() + shardMeta.getId());
        return shardTbl;
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
            DcMeta dc1 = new DcMeta(dcNames[0]), dc2 = new DcMeta(dcNames[1]);
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
