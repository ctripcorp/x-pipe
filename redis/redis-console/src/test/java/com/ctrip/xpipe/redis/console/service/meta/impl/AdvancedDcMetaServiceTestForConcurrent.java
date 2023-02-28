package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.meta.comparator.AbstractMetaComparator;
import com.ctrip.xpipe.tuple.Pair;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.ContainerLoader;
import org.unidal.tuple.Triple;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @author chen.zhu
 * <p>
 * Apr 03, 2018
 */
public class AdvancedDcMetaServiceTestForConcurrent extends AbstractConsoleIntegrationTest {

    @Autowired
    private DcMetaService service;

    @Autowired
    private ShardService shardService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private RedisService redisService;

    @Test
    public void testGetDcMeta() throws Exception {
        DcMeta jqFuture = service.getDcMeta("NTGXH");
        DcMeta oyFuture = service.getDcMeta("UAT");

        DcComparator dcComparator = new DcComparator(getDcMeta("NTGXH"), jqFuture);
        Assert.assertEquals(0, dcComparator.getAdded().size());
        Assert.assertEquals(0, dcComparator.getRemoved().size());
        Assert.assertEquals(0, dcComparator.getMofified().size());

        dcComparator = new DcComparator(getDcMeta("UAT"), oyFuture);
        Assert.assertEquals(0, dcComparator.getAdded().size());
        Assert.assertEquals(0, dcComparator.getRemoved().size());
        Assert.assertEquals(0, dcComparator.getMofified().size());
    }

    @Test
    public void testGetDcMeta2() throws TimeoutException {
        String dcJQ = "NTGXH", dcOY = "UAT";
        DcMeta currentJQ = getDcMeta(dcJQ), currentOY = getDcMeta(dcOY);
        ExecutorService executors = Executors.newCachedThreadPool();
        final int task = 50;
        int index = 0;
        AtomicInteger counter = new AtomicInteger(0);
        while(task > index++) {
            TestDcMetaCommand jqCommand = new TestDcMetaCommand(dcJQ);
            jqCommand.future().addListener(commandFuture -> {
                DcMeta jqFuture = commandFuture.getNow();
                DcComparator dcComparator = new DcComparator(currentJQ, jqFuture);
                Assert.assertEquals(0, dcComparator.getAdded().size());
                Assert.assertEquals(0, dcComparator.getRemoved().size());
                Assert.assertEquals(0, dcComparator.getMofified().size());
                counter.getAndIncrement();
            });

            TestDcMetaCommand oyCommand = new TestDcMetaCommand(dcOY);
            oyCommand.future().addListener(commandFuture -> {
//                counter.getAndIncrement();
                DcMeta oyFuture = commandFuture.getNow();
                DcComparator dcComparator = new DcComparator(currentOY, oyFuture);
                Assert.assertEquals(0, dcComparator.getAdded().size());
                Assert.assertEquals(0, dcComparator.getRemoved().size());
                Assert.assertEquals(0, dcComparator.getMofified().size());
            });

            jqCommand.execute(executors);
            oyCommand.execute(executors);
        }


        waitConditionUntilTimeOut(() -> counter.get() >= task , 60 * 1000);

    }

    @Test
    public void testDcComparator() throws Exception {
        DcMeta jqFuture = service.getDcMeta("NTGXH");
        jqFuture.addCluster(new ClusterMeta("add"));

        DcComparator dcComparator = new DcComparator(getDcMeta("NTGXH"), jqFuture);

        Assert.assertNotEquals(0, dcComparator.getAdded().size());
        Assert.assertEquals(0, dcComparator.getRemoved().size());
        Assert.assertEquals(0, dcComparator.getMofified().size());
    }

    @Test
    public void testDcComparator2() throws Exception {
        DcMeta jqFuture = service.getDcMeta("NTGXH");

        jqFuture.removeCluster(jqFuture.getClusters().keySet().iterator().next());

        DcComparator dcComparator = new DcComparator(getDcMeta("NTGXH"), jqFuture);

        Assert.assertNotEquals(0, dcComparator.getRemoved().size());

        Assert.assertEquals(0, dcComparator.getMofified().size());
    }

    @Test
    public void testDcComparator3() throws Exception {
        DcMeta jqFuture = service.getDcMeta("NTGXH");

        ClusterMeta clusterMeta = jqFuture.getClusters().values().iterator().next();

        clusterMeta.addShard(new ShardMeta("differ-shard"));

        DcComparator dcComparator = new DcComparator(getDcMeta("NTGXH"), jqFuture);

        Assert.assertEquals(0, dcComparator.getRemoved().size());

        Assert.assertNotEquals(0, dcComparator.getMofified().size());
    }

    class TestDcMetaCommand extends AbstractCommand<DcMeta> {

        private String dcName;

        public TestDcMetaCommand(String dcName) {
            this.dcName = dcName;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                DcMeta dcMeta = service.getDcMeta(dcName);
                future().setSuccess(dcMeta);
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return TestDcMetaCommand.class.getSimpleName();
        }
    }

    class DcComparator extends AbstractMetaComparator<ClusterMeta> {
        DcMeta current, future;

        public DcComparator(DcMeta current, DcMeta future) {
            this.current = current;
            this.future = future;
            compare();
        }


        @Override
        public void compare() {
            Triple<Set<String>, Set<String>, Set<String>> result = getDiff(current.getClusters().keySet(), future.getClusters().keySet());

            Set<String> addedClusterIds = result.getFirst();
            Set<String> intersectionClusterIds = result.getMiddle();
            Set<String> deletedClusterIds = result.getLast();

            for(String clusterId : addedClusterIds){
                added.add(future.findCluster(clusterId));
            }

            for(String clusterId : deletedClusterIds){
                removed.add(current.findCluster(clusterId));
            }

            for(String clusterId : intersectionClusterIds) {
                ClusterComparator clusterMetaComparator = new ClusterComparator(
                        current.findCluster(clusterId), future.findCluster(clusterId));
                if(clusterMetaComparator.getAdded().size() != 0 || clusterMetaComparator.getRemoved().size() != 0
                        || clusterMetaComparator.getMofified().size() != 0) {
                    modified.add(clusterMetaComparator);
                }
            }
        }

        @Override
        public String idDesc() {
            return null;
        }
    }

    class ClusterComparator extends AbstractMetaComparator<ShardMeta> {

        private ClusterMeta current, future;

        public ClusterComparator(ClusterMeta current, ClusterMeta future) {
            this.current = current;
            this.future = future;
            compare();
        }

        @Override
        public void compare() {
            Triple<Set<String>, Set<String>, Set<String>> result = getDiff(current.getShards().keySet(), future.getShards().keySet());

            for(String shardId : result.getFirst()){
                added.add(future.findShard(shardId));
            }

            for(String shardId : result.getLast()){
                removed.add(current.findShard(shardId));
            }

            for(String shardId : result.getMiddle()) {
                ShardMeta currentMeta = current.findShard(shardId);
                ShardMeta futureMeta = future.findShard(shardId);
                ShardComparator comparator = new ShardComparator(currentMeta, futureMeta);
                comparator.compare();
                if(comparator.getAdded().size() != 0 || comparator.getRemoved().size() != 0
                        || comparator.getMofified().size() != 0) {
                    modified.add(comparator);
                }
            }
        }

        @Override
        public String idDesc() {
            return null;
        }
    }


    public class ShardComparator extends AbstractMetaComparator<Redis>{

        private ShardMeta current, future;

        public ShardComparator(ShardMeta current, ShardMeta future){
            this.current = current;
            this.future = future;
        }

        @Override
        public void compare() {
            List<Redis> currentAll =  getAll(current);
            List<Redis> futureAll =  getAll(future);


            Pair<List<Redis>, List<Pair<Redis, Redis>>> subResult = sub(futureAll, currentAll);
            List<Redis> tAdded = subResult.getKey();
            added.addAll(tAdded);

            List<Pair<Redis, Redis>> modifiedAll = subResult.getValue();

            List<Redis> tRemoved = sub(currentAll, futureAll).getKey();
            removed.addAll(tRemoved);
        }


        private Pair<List<Redis>, List<Pair<Redis, Redis>>> sub(List<Redis> allRedis1, List<Redis> allRedis2) {

            List<Redis> subResult = new LinkedList<>();
            List<Pair<Redis, Redis>> intersectResult = new LinkedList<>();

            for(Redis redis1 : allRedis1){

                Redis redis2Equal = null;
                for(Redis redis2 : allRedis2) {
                    if (!MetaUtils.theSame(redis2, redis1)) {
                        continue;
                    }
                    redis2Equal = redis2;
                    break;
                }
                if(redis2Equal == null){
                    subResult.add(redis1);
                }else{
                    intersectResult.add(new Pair<>(redis1, redis2Equal));
                }
            }
            return new Pair<List<Redis>, List<Pair<Redis, Redis>>>(subResult, intersectResult);
        }

        private List<Redis> getAll(ShardMeta shardMeta) {

            List<Redis> result = new LinkedList<>();
            result.addAll(shardMeta.getRedises());
            result.addAll(shardMeta.getKeepers());
            return result;
        }

        @Override
        public String idDesc() {
            return current.getId();
        }
    }


    @Override
    protected String getXpipeMetaConfigFile() {
        return "src/test/resources/dc-meta-info.xml";
    }

    @Override
    protected String prepareDatas() throws IOException {
        // empty the database
        String sql = prepareDatasFromFile("src/main/resources/sql/h2/xpipedemodbtables.sql");
        sql += "insert into dc_tbl(zone_id, id, dc_name, dc_active, dc_description) values (1, 1, 'NTGXH', 1, 'NTGXH'), (1, 2, 'UAT', 2, 'UAT');";
        return sql;
    }

    @Before
    public void beforeOptimizedDcMetaServiceTestForConcurrent() throws Exception {
        // put xpipe meta into database
        Set<String> clusters = new HashSet<>();
        Map<String, Map<Long, SentinelGroupModel>> shardMap = Maps.newHashMap();
        long dcId;
        for(DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            dcId = dcMeta.getId().equalsIgnoreCase("NTGXH") ? 1L : 2L;
            for(ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if(clusters.add(clusterMeta.getId())) {
                    long activeDcId = clusterMeta.getActiveDc().equalsIgnoreCase("NTGXH") ? 1L : 2L;
                    ClusterTbl clusterTbl = new ClusterTbl().setActivedcId(activeDcId)
                            .setClusterName(clusterMeta.getId()).setClusterLastModifiedTime(clusterMeta.getLastModifiedTime())
                            .setClusterType(ClusterType.ONE_WAY.toString())
                            .setClusterAdminEmails("test@ctrip.com").setClusterDescription(clusterMeta.getId());
                    ClusterModel clusterModel = new ClusterModel();
                    clusterModel.setClusterTbl(clusterTbl);
                    clusterModel.setDcs(Lists.newArrayList(new DcTbl().setDcName("NTGXH"), new DcTbl().setDcName("UAT")));
                    clusterService.createCluster(clusterModel);
                }
                for(ShardMeta shardMeta : clusterMeta.getShards().values()) {
                    String shardName = shardMeta.getId();
                    Map<Long, SentinelGroupModel> sentinelMap = shardMap.get(shardName);
                    if(sentinelMap == null) {
                        sentinelMap = Maps.newHashMap();
                        sentinelMap.put(dcId, new SentinelGroupModel().setSentinelGroupId(shardMeta.getSentinelId()));
                        shardMap.put(shardName, sentinelMap);
                    } else {
                        sentinelMap.put(dcId, new SentinelGroupModel().setSentinelGroupId(shardMeta.getSentinelId()));
                        ShardTbl shardTbl = new ShardTbl().setShardName(shardName)
                                .setSetinelMonitorName(shardMeta.getSentinelMonitorName());

                        shardService.createShard(clusterMeta.getId(), shardTbl, sentinelMap);
                    }

                }
            }
        }

        KeepercontainerTblDao dao = ContainerLoader.getDefaultContainer().lookup(KeepercontainerTblDao.class);
        for(DcMeta dcMeta : getXpipeMeta().getDcs().values()) {
            dcMeta.getKeeperContainers().forEach(keeperContainerMeta -> {
                try {
                    KeepercontainerTbl proto = new KeepercontainerTbl().setKeepercontainerId(keeperContainerMeta.getId())
                            .setKeepercontainerIp(keeperContainerMeta.getIp()).setKeepercontainerPort(keeperContainerMeta.getPort());
                    dao.insert(proto);
                } catch (DalException e) {
                    e.printStackTrace();
                }
            });
            for(ClusterMeta clusterMeta : dcMeta.getClusters().values()) {

                for(ShardMeta shardMeta : clusterMeta.getShards().values()) {

                    List<Pair<String, Integer>> redises = new ArrayList<>();
                    for(RedisMeta redisMeta : shardMeta.getRedises()) {
                        redises.add(new Pair<>(redisMeta.getIp(), redisMeta.getPort()));
                    }
                    redisService.insertRedises(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), redises);

                    List<KeeperBasicInfo> keepers = new ArrayList<>();
                    for(KeeperMeta keeperMeta : shardMeta.getKeepers()) {
                        keepers.add(new KeeperBasicInfo(keeperMeta.getKeeperContainerId(), keeperMeta.getIp(), keeperMeta.getPort()));
                    }
                    redisService.insertKeepers(dcMeta.getId(), clusterMeta.getId(), shardMeta.getId(), keepers);
                }
            }
        }
    }
}
