package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.CheckFailException;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.RedisCreateInfo;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ShardCreateInfo;
import com.ctrip.xpipe.redis.console.model.DcTbl;
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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @author chen.zhu
 * <p>
 * Jan 30, 2018
 */
public class MetaUpdateTest3 extends AbstractConsoleIntegrationTest {

    @Autowired
    private MetaUpdate metaUpdate;

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

        Assert.assertTrue(listEquals(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
                redisTbls.stream().map((redisTbl)-> { return redisTbl.getRedisIp() + ":" + redisTbl.getRedisPort();})
            .collect(Collectors.toList())));

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
        ClusterCreateInfo clusterCreateInfo = metaUpdate.getCluster(clusterName);
        Assert.assertEquals(clusterName, clusterCreateInfo.getClusterName());
        Assert.assertEquals(3L, (long)clusterCreateInfo.getOrganizationId());
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
        metaUpdate.createCluster(clusterCreateInfo);

        ClusterCreateInfo current = metaUpdate.getCluster(clusterName);
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
        metaUpdate.createCluster(clusterCreateInfo);

        ClusterCreateInfo current = metaUpdate.getCluster(clusterName);
        Assert.assertEquals("oy", current.getDcs().get(0));
        Assert.assertEquals("jq", current.getDcs().get(1));
    }

    private List<RedisCreateInfo> createInfo(List<String> activeDcRedis, List<String> backupDcRedis) {
        return Lists.newArrayList(new RedisCreateInfo().setDcId(activeDC).setRedises(StringUtil.join(", ", activeDcRedis.toArray())),
                new RedisCreateInfo().setDcId(backupDC).setRedises(StringUtil.join(", ", backupDcRedis.toArray())));
    }


    private void createCluster() throws Exception {
        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
        clusterCreateInfo.setClusterAdminEmails("xpipe@ctrip.com");
        clusterCreateInfo.setClusterName(clusterName);
        clusterCreateInfo.setClusterType(ClusterType.ONE_WAY.toString());
        clusterCreateInfo.setDcs(Lists.newArrayList(activeDC, backupDC));
        clusterCreateInfo.setOrganizationId(3L);
        clusterCreateInfo.setDesc("test cluster");
        metaUpdate.createCluster(clusterCreateInfo);

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
    public void testBindDc() {
        RetMessage ret = metaUpdate.bindDc(clusterName, "fra", Optional.ofNullable(null));
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());

        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs(clusterName);
        Assert.assertEquals(3, dcTbls.size());
        Assert.assertTrue(dcTbls.stream().anyMatch(dc -> dc.getDcName().equals("fra")));
    }

    @Test
    public void testBindDuplicatedDc() {
        RetMessage ret = metaUpdate.bindDc(clusterName, "jq", null);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());

        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs(clusterName);
        Assert.assertEquals(2, dcTbls.size());
    }

    @Test
    public void testUnbindActiveDc() {
        RetMessage ret = metaUpdate.unbindDc(clusterName, activeDC);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());

        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs(clusterName);
        Assert.assertEquals(2, dcTbls.size());
    }

    @Test
    public void testUnbindBackupDc() {
        RetMessage ret = metaUpdate.unbindDc(clusterName, backupDC);
        Assert.assertEquals(RetMessage.SUCCESS_STATE, ret.getState());

        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs(clusterName);
        Assert.assertEquals(1, dcTbls.size());
        Assert.assertTrue(dcTbls.stream().noneMatch(dc -> dc.getDcName().equals(backupDC)));
    }

    @Test
    public void testUnbindNotEmptyDc() {
        List<RedisCreateInfo> createInfo = createInfo(Lists.newArrayList("192.168.0.1:6379", "192.168.0.1:6380"),
                Lists.newArrayList("192.168.0.2:6379", "192.168.0.2:6380"));
        metaUpdate.createShard(clusterName, shardName, createInfo);

        RetMessage ret = metaUpdate.unbindDc(clusterName, backupDC);
        Assert.assertEquals(RetMessage.FAIL_STATE, ret.getState());

        List<DcTbl> dcTbls = clusterService.getClusterRelatedDcs(clusterName);
        Assert.assertEquals(2, dcTbls.size());
    }

    @Test
    public void getClusters() throws CheckFailException {
        List<ClusterCreateInfo> clusters = metaUpdate.getClusters(ClusterType.ONE_WAY.toString());
        Assert.assertNotNull(clusters.get(0).getDcDetails());
        logger.info("{}", clusters);
    }
}