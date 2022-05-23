package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.dao.RedisDao;
import com.ctrip.xpipe.redis.console.exception.BadRequestException;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.notifier.ClusterMetaModifiedNotifier;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.exception.ResourceNotFoundException;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.FileUtils;
import com.google.common.collect.Lists;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.*;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public class RedisServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    private RedisServiceImpl redisService;

    @Autowired
    private KeeperAdvancedService keeperAdvancedService;

    @Autowired
    private RedisDao redisDao;

    private String dcName;

    private String shardName;

    private ClusterMetaModifiedNotifier tmpNotifier;

    private List<String> keeperContainers = Arrays.asList("127.1.1.1", "127.1.1.2");

    @Before
    public void beforeRedisServiceImplTest() {
        dcName = dcNames[0];
        shardName = shardNames[0];
        tmpNotifier = redisService.notifier;
    }

    {
        shardNames = new String[]{"shard1", "shard2", "shard3", "shard4"};
    }

    @After
    public void afterRedisServiceImplTest() {
        if (null != tmpNotifier) redisService.notifier = tmpNotifier;
    }

    @Test
    public void testFindAllRedisesByDcClusterName(){

        List<RedisTbl> dc1 = redisService.findAllRedisesByDcClusterName(dcNames[0], clusterName);

        Assert.assertTrue(dc1.size() > 0);

        Set<Long> dc1Ids = new HashSet<>();
        dc1.forEach(redisTbl -> dc1Ids.add(redisTbl.getId()));

        List<RedisTbl> dc2 = redisService.findAllRedisesByDcClusterName(dcNames[1], clusterName);
        Assert.assertTrue(dc2.size() > 0);

        dc2.forEach(redisTbl -> Assert.assertFalse(dc1Ids.contains(redisTbl.getId())));
    }

    @Test
    public void testInsertRedises() throws ResourceNotFoundException, DalException {

        List<RedisTbl> redises = redisService.findRedisesByDcClusterShard(dcName, clusterName, shardName);

        redisService.insertInstancesToDb(redises.get(0).getDcClusterShardId(), XPipeConsoleConstant.ROLE_REDIS, new Pair<>("127.0.0.1", randomInt()), new Pair<>("127.0.0.1", randomInt()));

        List<RedisTbl> newRedises = redisService.findRedisesByDcClusterShard(dcName, clusterName, shardName);

        Assert.assertEquals(redises.size() + 2, newRedises.size());

    }

    @Test
    public void testDeleteRedises() throws DalException, ResourceNotFoundException{
        List<RedisTbl> redises = redisService.findRedisesByDcClusterShard(dcName, clusterName, shardName);
        int expect_length = redises.size() - 1;
        List<Pair<String, Integer>> redisAddresses = new LinkedList<>();
        redisAddresses.add(new Pair<>(redises.get(0).getRedisIp(), redises.get(0).getRedisPort()));
        redisService.deleteRedises(dcName, clusterName, shardName, redisAddresses);
        redises = redisService.findRedisesByDcClusterShard(dcName, clusterName, shardName);
        Assert.assertEquals(expect_length, redises.size());
    }

    @Test
    public void testInsertKeepers() throws ResourceNotFoundException, DalException {

        redisService.deleteKeepers(dcName, clusterName, shardName);

        List<KeeperBasicInfo> newKeepers = keeperAdvancedService.findBestKeepers(dcName, clusterName);

        Assert.assertEquals(2, newKeepers.size());

        redisService.insertKeepers(dcName, clusterName, shardName, newKeepers);

        List<RedisTbl> result = redisService.findKeepersByDcClusterShard(dcName, clusterName, shardName);

        result.forEach(redisTbl -> Assert.assertTrue(redisTbl.getKeepercontainerId() != 0));

        Assert.assertEquals(newKeepers.size(), result.size());

    }

    @Test
    public void testDeleteKeepers() throws ResourceNotFoundException, DalException {

        List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(dcName, clusterName, shardName);
        Assert.assertTrue(keepers.size() > 0);

        redisService.deleteKeepers(dcName, clusterName, shardName);

        keepers = redisService.findKeepersByDcClusterShard(dcName, clusterName, shardName);
        Assert.assertEquals(0, keepers.size());
    }

    @Test
    public void testFindByRole() throws ResourceNotFoundException {

        List<RedisTbl> allByDcClusterShard = redisService.findAllByDcClusterShard(dcName, clusterName, shardName);
        checkAllInstances(allByDcClusterShard);

        List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(dcName, clusterName, shardName);
        List<RedisTbl> redises = redisService.findRedisesByDcClusterShard(dcName, clusterName, shardName);

        Assert.assertEquals(allByDcClusterShard.size(), keepers.size() + redises.size());

        keepers.forEach(new Consumer<RedisTbl>() {
            @Override
            public void accept(RedisTbl redisTbl) {
                logger.debug("[keeper]{}", redisTbl);
                Assert.assertEquals(XPipeConsoleConstant.ROLE_KEEPER, redisTbl.getRedisRole());
            }
        });

        redises.forEach(new Consumer<RedisTbl>() {
            @Override
            public void accept(RedisTbl redisTbl) {
                logger.debug("[redis]{}", redisTbl);
                Assert.assertEquals(XPipeConsoleConstant.ROLE_REDIS, redisTbl.getRedisRole());
            }
        });

    }

    @Test
    public void testUpdateRedises() throws IOException, ResourceNotFoundException {

        List<RedisTbl> allByDcClusterShard = redisService.findAllByDcClusterShard(dcName, clusterName, shardName);
        checkAllInstances(allByDcClusterShard);
        boolean firstSlave = true;
        RedisTbl newMaster = null;

        for (RedisTbl redisTbl : allByDcClusterShard) {
            if (redisTbl.getRedisRole().equals(XPipeConsoleConstant.ROLE_REDIS)) {

                if (redisTbl.isMaster()) {
                    redisTbl.setMaster(false);
                } else if (!redisTbl.isMaster() && firstSlave) {
                    redisTbl.setMaster(true);
                    newMaster = redisTbl;
                    firstSlave = false;
                }

            }
        }

        checkAllInstances(allByDcClusterShard);

        ShardModel shardModel = new ShardModel(allByDcClusterShard);
        redisService.updateRedises(dcName, clusterName, shardName, shardModel);

        allByDcClusterShard = redisService.findAllByDcClusterShard(dcName, clusterName, shardName);
        checkAllInstances(allByDcClusterShard);

        Stream<RedisTbl> redisTblStream = allByDcClusterShard.stream().filter(instance -> instance.isMaster());

        RedisTbl currentMaster = redisTblStream.findFirst().get();
        Assert.assertEquals(newMaster.getId(), currentMaster.getId());

    }

    private void checkAllInstances(List<RedisTbl> allByDcClusterShard) {

        Assert.assertEquals(4, allByDcClusterShard.size());

        int masterCount = 0;

        for (RedisTbl redisTbl : allByDcClusterShard) {
            logger.debug("{}", redisTbl);
            if (redisTbl.isMaster()) {
                masterCount++;
            }
        }
        Assert.assertEquals(1, masterCount);

    }

    @Test
    public void testSub() {

        List<Pair<String, Integer>> first = Lists.newArrayList(
                Pair.from("127.0.0.1", 1111),
                Pair.from("127.0.0.1", 1112),
                Pair.from("127.0.0.1", 1113)
        );

        List<RedisTbl> second = Lists.newArrayList(
                new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(1111),
                new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(1112),
                new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(9999)
        );

        List<Pair<String, Integer>> sub = redisService.sub(first, second);
        Assert.assertEquals(1, sub.size());
    }

    @Test
    public void testInter() {

        List<Pair<String, Integer>> first = Lists.newArrayList(
                Pair.from("127.0.0.1", 1111),
                Pair.from("127.0.0.1", 1112),
                Pair.from("127.0.0.1", 1113)
        );

        List<RedisTbl> second = Lists.newArrayList(
                new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(1111),
                new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(1112),
                new RedisTbl().setRedisIp("127.0.0.1").setRedisPort(9999)
        );

        List<RedisTbl> inter = redisService.inter(first, second);
        Assert.assertEquals(2, inter.size());
    }

    @Test
    public void testValidateKeepersWithNothingChanged() throws ResourceNotFoundException {
        List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(dcName, clusterName, shardName);
        redisService.validateKeepers(keepers);
    }

    @Test(expected = BadRequestException.class)
    public void testValidateKeepersWithKeeperPortChange() throws ResourceNotFoundException {
        List<RedisTbl> originKeepers = redisService.findKeepersByDcClusterShard(dcName, clusterName, shardName);
        List<RedisTbl> targetKeepers = new ArrayList<>(originKeepers);
        // A front end port change, leads to an id change in backend
        // Due to the function logic, we change the id only
        targetKeepers.get(0).setId(11111L);
        redisService.validateKeepers(targetKeepers);
    }

    @Test
    public void testValidateKeepersInTheSameAvailableZone() throws ResourceNotFoundException, IOException, ComponentLookupException, SQLException {

        executeSqlScript(FileUtils.readFileAsString("src/test/resources/keeper-in-same-avaialable-zone.sql"));
        List<RedisTbl> originKeepers = redisService.findKeepersByDcClusterShard(dcName, clusterName, shardName);
        List<RedisTbl> targetKeepers = new ArrayList<>(originKeepers);

        targetKeepers.get(0).setRedisIp(keeperContainers.get(0)).setKeepercontainerId(30);
        targetKeepers.get(1).setRedisIp(keeperContainers.get(1)).setKeepercontainerId(31);
        redisService.validateKeepers(targetKeepers);
    }

    @Test
    public void testAddDuplicateRedis() throws ResourceNotFoundException, DalException {
        List<RedisTbl> redisTbls = redisService.findAllByDcClusterShard(dcName, clusterName, shardName);
        logger.info("[Redis-Tbls]{}", redisTbls);
        try {
            redisService.insertRedises(dcName, clusterName, shardNames[1], Lists.transform(redisTbls,
                    (redisTbl) -> new Pair<String, Integer>(redisTbl.getRedisIp(), redisTbl.getRedisPort())));
            Assert.fail();
        } catch (Exception e) {
//            Assert.assertEquals("Redis already exists, localhost:6379", e.getMessage());
            logger.error("", e);
        }
    }

    @Test
    public void testUpdateWithDuplicateRedis() throws ResourceNotFoundException, DalException {
        List<RedisTbl> redisTbls = redisService.findAllByDcClusterShard(dcName, clusterName, shardName);
        logger.info("[Redis-Tbls]{}", redisTbls);
        RedisTbl redis = new RedisTbl().setRedisIp("localhost").setRedisPort(6379);
        try {
            redisService.insertRedises(dcName, clusterName, shardNames[1], Lists.newArrayList(new Pair<>(redis.getRedisIp(), redis.getRedisPort())));
            redisService.updateRedises(dcName, clusterName, shardName, new ShardModel().addRedis(redis));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertEquals("Redis already exists, localhost:6379", e.getMessage());
            logger.error("", e);
        }
    }

    @Test
    public void testCountContainerKeeperAndClusterAndShard() {
        List<RedisTbl> redisTbls = redisService.findAllKeeperContainerCountInfo();
        Assert.assertEquals(4, redisTbls.size());

        RedisTbl redis = redisTbls.get(0);
        Assert.assertTrue(redis.getKeepercontainerId() > 0);
        Assert.assertEquals(shardNames.length, redis.getCount());
        Assert.assertEquals(1, redis.getDcClusterShardInfo().getClusterCount());
        Assert.assertEquals(shardNames.length, redis.getDcClusterShardInfo().getShardCount());
    }

    @Test
    public void addKeeperConcurrentlyTest() throws Exception {
        int threadCnt = shardNames.length;
        CyclicBarrier barrier = new CyclicBarrier(threadCnt);
        CountDownLatch latch = new CountDownLatch(threadCnt);
        List<KeeperBasicInfo> bestKeepers = keeperAdvancedService.findBestKeepers(dcName, clusterName);
        logger.info("[addKeeperConcurrentlyTest] addKeepers {}", bestKeepers);

        IntStream.range(0, threadCnt).forEach(i -> {
            new Thread(() -> {
                RedisServiceImpl localRedisService = buildLocalRedisService();
                try {
                    redisService.deleteKeepers(dcName, clusterName, shardNames[i]);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    barrier.await(2, TimeUnit.SECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    localRedisService.insertKeepers(dcName, clusterName, shardNames[i], bestKeepers);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        });

        latch.await(5, TimeUnit.SECONDS);

        AtomicInteger noKeeperShardCnt = new AtomicInteger();
        AtomicInteger twoKeeperShardCnt = new AtomicInteger();
        AtomicInteger otherShardCnt = new AtomicInteger();

        IntStream.range(0, threadCnt).forEach(i -> {
            try {
                List<RedisTbl> keepers = redisService.findKeepersByDcClusterShard(dcName, clusterName, shardNames[i]);
                if (keepers.isEmpty()) noKeeperShardCnt.incrementAndGet();
                else if (keepers.size() == 2) twoKeeperShardCnt.incrementAndGet();
                else otherShardCnt.incrementAndGet();
            } catch (ResourceNotFoundException e) {
                Assert.fail();
            }
        });

        logger.info("[addKeeperConcurrentlyTest] twoKeeperShardCnt {} noKeeperShardCnt {}, otherShardCnt {}",
                twoKeeperShardCnt.get(), noKeeperShardCnt.get(), otherShardCnt.get());
        Assert.assertEquals(1, twoKeeperShardCnt.get());
        Assert.assertEquals(shardNames.length - 1, noKeeperShardCnt.get());
        Assert.assertEquals(0, otherShardCnt.get());
    }

    @Test
    public void testNotifyBiDirectionClusterUpdate() {
        ClusterModel clusterModel = new ClusterModel();
        clusterModel.setClusterTbl(new ClusterTbl().setClusterName("bi-test")
                .setClusterType(ClusterType.BI_DIRECTION.toString())
                .setActivedcId(1)
                .setClusterDescription("desc")
                .setClusterAdminEmails("test@ctrip.com"));
        clusterModel.setDcs(Arrays.asList(new DcTbl().setDcName("jq"), new DcTbl().setDcName("oy")));
        redisService.clusterService.createCluster(clusterModel);

        ClusterMetaModifiedNotifier notifier = Mockito.mock(ClusterMetaModifiedNotifier.class);
        redisService.notifier = notifier;

        Mockito.doAnswer(invocation -> {
            String clusterName = invocation.getArgument(0, String.class);
            List<String> dcs = invocation.getArgument(1, List.class);
            Assert.assertEquals("bi-test", clusterName);
            Assert.assertEquals(2, dcs.size());
            Assert.assertTrue(dcs.contains("jq") && dcs.contains("oy"));
            return null;
        }).when(notifier).notifyClusterUpdate(Mockito.anyString(), Mockito.anyList());
        redisService.notifyClusterUpdate("jq", "bi-test");
        Mockito.verify(notifier, Mockito.times(1)).notifyClusterUpdate(Mockito.anyString(), Mockito.anyList());
    }

    @Test
    public void testNotifyOneWayClusterUpdate() {
        ClusterMetaModifiedNotifier notifier = Mockito.mock(ClusterMetaModifiedNotifier.class);
        redisService.notifier = notifier;

        Mockito.doAnswer(invocation -> {
            String paramClusterName = invocation.getArgument(0, String.class);
            List<String> dcs = invocation.getArgument(1, List.class);
            Assert.assertEquals(clusterName, paramClusterName);
            Assert.assertEquals(1, dcs.size());
            Assert.assertTrue(dcs.contains("jq"));
            return null;
        }).when(notifier).notifyClusterUpdate(Mockito.anyString(), Mockito.anyList());
        redisService.notifyClusterUpdate("jq", clusterName);
        Mockito.verify(notifier, Mockito.times(1)).notifyClusterUpdate(Mockito.anyString(), Mockito.anyList());
    }

    private RedisServiceImpl buildLocalRedisService() {
        RedisServiceImpl localRedisService = new RedisServiceImpl();
        localRedisService.clusterService = redisService.clusterService;
        localRedisService.dcClusterShardService = redisService.dcClusterShardService;
        localRedisService.keeperContainerService = redisService.keeperContainerService;
        localRedisService.notifier = redisService.notifier;
        localRedisService.redisDao = redisService.redisDao;

        try {
            Field daoField = AbstractConsoleService.class.getDeclaredField("dao");
            daoField.setAccessible(true);
            daoField.set(localRedisService, daoField.get(redisService));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return localRedisService;
    }

}
