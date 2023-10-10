package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RegionShardsCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ShardCreateInfo;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author chen.zhu
 * <p>
 * Jan 30, 2018
 */
public class ShardUpdateControllerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MetaUpdate metaUpdate;

    @Autowired
    private ClusterUpdateController clusterUpdateController;

    @Autowired
    private ShardUpdateController shardUpdateController;

    @Autowired
    private ShardService shardService;

    @Autowired
    private RedisService redisService;

    @Autowired
    private ClusterService clusterService;

    private String clusterName = "cluster-create-shard-test";

    private String activeDC = "jq", backupDC = "oy";

    private String shardName = "shard";

    private String shardName1 = "shard1";
    private String shardName2 = "shard2";

    @Before
    public void beforeMetaUpdateTest3() throws Exception {
        clusterUpdateController.deleteCluster(clusterName, false);
        createCluster();
    }

    @Test
    public void createShard() throws Exception {
        List<RedisCreateInfo> createInfo = createInfo(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
            Lists.newArrayList("192.168.0.2:6379", "192.168.0.2:6380"));
        metaUpdate.createShard(clusterName, shardName, createInfo);

        ShardTbl shardTbl = shardService.find(clusterName, shardName);

        List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(activeDC, clusterName, shardName);

        List<RedisTbl> redisTbls = redisService.findRedisesByDcClusterShard(activeDC, clusterName, shardName);

        logger.info("{}", shardTbl);

        logger.info("{}", keepers);

        logger.info("{}", redisTbls);

        List<String> redisIpPorts = redisTbls.stream()
            .map(redisTbl-> redisTbl.getRedisIp() + ":" + redisTbl.getRedisPort())
            .collect(Collectors.toList());
        Assert.assertEquals(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"), redisIpPorts);

        Assert.assertEquals(2, keepers.size());
    }

    @Test
    public void createShard1() throws Exception {
        ShardCreateInfo shardCreateInfo = new ShardCreateInfo();
        shardCreateInfo.setShardMonitorName(shardName);
        shardCreateInfo.setShardName(shardName);
        metaUpdate.createShards(clusterName, Lists.newArrayList(new ShardCreateInfo()));

        List<RedisCreateInfo> createInfo = createInfo(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
                Lists.newArrayList("192.168.0.2:6379", "192.168.0.2:6380"));
        metaUpdate.createShard(clusterName, shardName, createInfo);

        ShardTbl shardTbl = shardService.find(clusterName, shardName);

        List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(activeDC, clusterName, shardName);

        List<RedisTbl> redisTbls = redisService.findRedisesByDcClusterShard(activeDC, clusterName, shardName);

        logger.info("{}", shardTbl);

        logger.info("{}", keepers);

        logger.info("{}", redisTbls);

        Assert.assertTrue(listEquals(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
                redisTbls.stream().map((redisTbl)-> { return redisTbl.getRedisIp() + ":" + redisTbl.getRedisPort();})
                        .collect(Collectors.toList())));

        Assert.assertEquals(2, keepers.size());

    }

    @Test
    public void createShard2() throws Exception {
        ShardCreateInfo shardCreateInfo = new ShardCreateInfo();
        shardCreateInfo.setShardMonitorName(shardName);
        shardCreateInfo.setShardName(shardName);
        metaUpdate.createShards(clusterName, Lists.newArrayList(shardCreateInfo));

        List<RedisCreateInfo> createInfo = createInfo(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
                Lists.newArrayList("192.168.0.2:6379", "192.168.0.2:6380"));

        RetMessage result = metaUpdate.createShard(clusterName, shardName, clusterName + "-" + shardName, createInfo);
        Assert.assertEquals(RetMessage.FAIL_STATE, result.getState());
        Assert.assertEquals(String.format("Post shard monitor name %s diff from previous %s",
                    clusterName + "-" + shardName, shardName), result.getMessage());
    }

    @Test
    public void createShard4() throws Exception {
        ShardCreateInfo shardCreateInfo1 = new ShardCreateInfo();
        shardCreateInfo1.setShardMonitorName(shardName);
        shardCreateInfo1.setShardName(shardName);

        metaUpdate.createShards(clusterName, Lists.newArrayList(shardCreateInfo1));

        List<RedisCreateInfo> createInfo = createInfo(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
                Lists.newArrayList("192.168.0.2:6379", "192.168.0.2:6380"));

        RetMessage result = metaUpdate.createShard(clusterName, shardName, createInfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, result.getState());
    }

    @Test
    public void createShard5() throws Exception {
        int taskNum = 10;
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(taskNum, taskNum, 1L, TimeUnit.SECONDS,
                new SynchronousQueue<>());
        executorService.allowsCoreThreadTimeOut();

        for(int i = 0; i < taskNum; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    List<RedisCreateInfo> createInfo = createInfo(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
                            Lists.newArrayList("192.168.0.2:6379", "192.168.0.2:6380"));

                    RetMessage result = metaUpdate.createShard(clusterName, shardName, createInfo);
                    logger.info("{}", result);
                }
            });
        }
        waitConditionUntilTimeOut(()->executorService.getCompletedTaskCount() == taskNum, 5000);

        System.out.println("=========================");
        List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);
        logger.info("{}", shards);
        Assert.assertEquals(1, shards.size());

        List<RedisTbl> redisTbls = redisService.findRedisesByDcClusterShard(activeDC, clusterName, shardName);
        logger.info("{}", redisTbls);
        Assert.assertEquals(2, redisTbls.size());

        List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(activeDC, clusterName, shardName);
        logger.info("{}", keepers);
        Assert.assertEquals(2, keepers.size());
    }

    @Test
    public void createShard6() throws Exception {
        int taskNum = 3;
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(taskNum, taskNum, 1L, TimeUnit.SECONDS,
                new SynchronousQueue<>());
        executorService.allowsCoreThreadTimeOut();

        for(int i = 0; i < taskNum; i++) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    ShardCreateInfo shardCreateInfo = new ShardCreateInfo();
                    shardCreateInfo.setShardMonitorName(shardName);
                    shardCreateInfo.setShardName(shardName);

                    RetMessage result = metaUpdate.createShards(clusterName, Lists.newArrayList(shardCreateInfo));
                    logger.info("{}", result);
                }
            });
        }
        waitConditionUntilTimeOut(()->executorService.getCompletedTaskCount() == taskNum, 2000);

        System.out.println("=========================");
        List<ShardTbl> shards = shardService.findAllByClusterName(clusterName);
        logger.info("{}", shards);
        Assert.assertEquals(1, shards.size());
    }

    @Test
    public void testGetCluster() throws Exception {
        createCluster();
        ClusterCreateInfo clusterCreateInfo = clusterUpdateController.getCluster(clusterName);
        Assert.assertEquals(clusterName, clusterCreateInfo.getClusterName());
        Assert.assertEquals(0L, (long)clusterCreateInfo.getOrganizationId());
        Assert.assertEquals(2, clusterCreateInfo.getDcs().size());
        Assert.assertEquals(activeDC, clusterCreateInfo.getDcs().get(0));
        Assert.assertEquals(backupDC, clusterCreateInfo.getDcs().get(1));
    }

    @Test
    public void testCreateClusterWithDcSequence() {
        String clusterName = "testCreateClusterWithDcSequence";
        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
        clusterCreateInfo.setClusterAdminEmails("xpipe@ctrip.com");
        clusterCreateInfo.setClusterName(clusterName);
        clusterCreateInfo.setClusterType(ClusterType.ONE_WAY.toString());
        clusterCreateInfo.setDcs(Lists.newArrayList("jq", "oy"));
        clusterCreateInfo.setOrganizationId(3L);
        clusterCreateInfo.setDesc("test cluster");
        clusterUpdateController.createCluster(clusterCreateInfo);

        ClusterCreateInfo current = clusterUpdateController.getCluster(clusterName);
        Assert.assertEquals("jq", current.getDcs().get(0));
        Assert.assertEquals("oy", current.getDcs().get(1));
    }

    @Test
    public void testCreateClusterWithDcSequence1() {
        String clusterName = "testCreateClusterWithDcSequence1";
        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
        clusterCreateInfo.setClusterAdminEmails("xpipe@ctrip.com");
        clusterCreateInfo.setClusterName(clusterName);
        clusterCreateInfo.setClusterType(ClusterType.ONE_WAY.toString());
        clusterCreateInfo.setDcs(Lists.newArrayList("oy", "jq"));
        clusterCreateInfo.setOrganizationId(3L);
        clusterCreateInfo.setDesc("test cluster");
        clusterUpdateController.createCluster(clusterCreateInfo);

        ClusterCreateInfo current = clusterUpdateController.getCluster(clusterName);
        Assert.assertEquals("oy", current.getDcs().get(0));
        Assert.assertEquals("jq", current.getDcs().get(1));
    }

    private List<RedisCreateInfo> createInfo(List<String> activeDcRedis, List<String> backupDcRedis) {
        return Lists.newArrayList(new RedisCreateInfo().setDcId(activeDC).setRedises(StringUtil.join(", ", activeDcRedis.toArray())),
                new RedisCreateInfo().setDcId(backupDC).setRedises(StringUtil.join(", ", backupDcRedis.toArray())));
    }


    private void createCluster() {
        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
        clusterCreateInfo.setClusterAdminEmails("xpipe@ctrip.com");
        clusterCreateInfo.setClusterName(clusterName);
        clusterCreateInfo.setClusterType(ClusterType.ONE_WAY.toString());
        clusterCreateInfo.setDcs(Lists.newArrayList(activeDC, backupDC));
        clusterCreateInfo.setOrganizationId(0L);
        clusterCreateInfo.setDesc("test cluster");
        clusterUpdateController.createCluster(clusterCreateInfo);

        logger.info("[Cluster] {}", clusterService.find(clusterName));
    }

    private <T, E> boolean listEquals(List<T> list1, List<E> list2) {
        if(list1 == null && list2 == null)
            return true;
        if(list1 == null || list2 == null)
            return false;
        if(list1.size() != list2.size())
            return false;

        for(T t : list1) {
            E e = (E) t;
            if(!list2.contains(e)) {
                return false;
            }
        }
        return true;
    }


    @Test
    public void syncBatchDeleteShardsWhenNullShard() {
        shardService.deleteShard(clusterName, shardName1);
        shardService.deleteShard(clusterName, shardName2);

        metaUpdate.syncBatchDeleteShards(clusterName, Lists.newArrayList(shardName1, shardName2));
    }

    @Test
    public void getClusters() throws CheckFailException {
        List<ClusterCreateInfo> clusters = clusterUpdateController.getClusters(ClusterType.ONE_WAY.toString());
        Assert.assertNotNull(clusters.get(0).getRegions());
        logger.info("{}", clusters);
    }

    @Test
    public void testCreateRegionShardsFail() {
        RegionShardsCreateInfo createInfo = new RegionShardsCreateInfo();
        createInfo.setShardNames(Arrays.asList("shard1", "shard2"));
        RetMessage ret = shardUpdateController.createRegionShards(clusterName, "SHA", createInfo);

        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());
    }

    @Test
    public void testCreateRegionShards() {
        ClusterCreateInfo createInfo = new ClusterCreateInfo();
        createInfo.setClusterName("cluster2");
        createInfo.setClusterType(ClusterType.ONE_WAY.toString());
        createInfo.setDcs(Lists.newArrayList(activeDC, backupDC, "fra"));
        createInfo.setOrganizationId(0L);
        createInfo.setDesc("test cluster2");
        createInfo.setClusterAdminEmails("xpipe@ctrip.com");

        RegionInfo regionInfo1 = new RegionInfo("SHA", "ONE_WAY", "jq", Arrays.asList("jq", "oy"));
        RegionInfo regionInfo2 = new RegionInfo("FRA", "SINGLE_DC", "fra", Collections.singletonList("fra"));
        createInfo.setRegions(Arrays.asList(regionInfo1, regionInfo2));
        clusterUpdateController.createCluster(createInfo);

        RegionShardsCreateInfo shardsCreateInfo = new RegionShardsCreateInfo();
        shardsCreateInfo.setShardNames(Arrays.asList("shard1", "shard2"));
        RetMessage ret = shardUpdateController.createRegionShards("cluster2", "SHA", shardsCreateInfo);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());
    }
}