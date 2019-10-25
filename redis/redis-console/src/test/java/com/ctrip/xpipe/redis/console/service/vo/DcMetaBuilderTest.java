package com.ctrip.xpipe.redis.console.service.vo;

import com.ctrip.xpipe.command.DefaultRetryCommandFactory;
import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.RedisMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Apr 03, 2018
 */
public class DcMetaBuilderTest extends AbstractConsoleIntegrationTest {

    private DcMeta dcMeta = new DcMeta();

    private Map<Long, String> dcNameMap;

    @Autowired
    private RedisMetaService redisMetaService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private DcClusterShardService dcClusterShardService;

    @Autowired
    private ClusterMetaService clusterMetaService;

    @Autowired
    private ClusterService clusterService;

    private List<DcClusterShardTbl> dcClusterShards;

    private DcMetaBuilder builder;

    @Before
    public void beforeDcMetaBuilderTest() {
        dcNameMap = dcService.dcNameMap();
        long dcId = dcNameMap.keySet().iterator().next();
        builder = new DcMetaBuilder(dcMeta, dcId, executors, redisMetaService, dcClusterService,
                clusterMetaService, dcClusterShardService, dcService, new DefaultRetryCommandFactory());
        builder.execute();

        logger.info("[beforeDcMetaBuilderTest] dcId: {}", dcId);
        dcClusterShards = dcClusterShardService.findAllByDcId(dcId);
        logger.info("[beforeDcMetaBuilderTest] dcClusterShards: {}", dcClusterShards);
    }

    @Test
    public void getOrCreateClusterMeta() throws Exception {
        builder.getOrCreateClusterMeta(dcClusterShards.get(0).getClusterInfo());
        ClusterMeta clusterMeta = dcMeta.getClusters().get(dcClusterShards.get(0).getClusterInfo().getClusterName());
        Assert.assertNotNull(clusterMeta);
    }

    @Test
    public void getOrCreateShardMeta() throws Exception {
        DcClusterShardTbl dcClusterShard = dcClusterShards.get(0);
        builder.getOrCreateClusterMeta(dcClusterShards.get(0).getClusterInfo());
        ClusterMeta clusterMeta = dcMeta.getClusters().get(dcClusterShard.getClusterInfo().getClusterName());

        logger.info("{}", dcClusterShard.getShardInfo());

        builder.getOrCreateShardMeta(clusterMeta.getId(), dcClusterShard.getShardInfo(), dcClusterShard.getSetinelId());


        String clusterId = dcClusterShard.getClusterInfo().getClusterName(), shardId = dcClusterShard.getShardInfo().getShardName();

        ShardMeta shardMeta = dcMeta.findCluster(clusterId).findShard(shardId);
        Assert.assertNotNull(shardMeta);
        logger.info("{}", shardMeta);
    }


    @Test
    public void testClusterBondToOnlyOneIDC() throws Exception {

        ClusterModel clusterModel = new ClusterModel();
        ClusterTbl clusterTbl = new ClusterTbl();
        clusterTbl.setActivedcId(1).setClusterAdminEmails("test@test.com").setClusterName("test-one-dc-cluster")
                .setClusterOrgId(1).setClusterDescription("not null").setStatus("Normal");
        clusterModel.setClusterTbl(clusterTbl);
        clusterModel.setShards(Lists.newArrayList());
        //empty slave idc
        clusterModel.setSlaveDcs(Lists.newArrayList());
        clusterTbl = clusterService.createCluster(clusterModel);

        dcClusterService.addDcCluster(dcService.getDcName(1), "test-one-dc-cluster");

        Map<Long, List<DcClusterTbl>> map = Maps.newHashMap();
        map.put(clusterTbl.getId(), Lists.newArrayList(new DcClusterTbl().setClusterId(clusterTbl.getId()).setDcId(1)));
        builder.setCluster2DcClusterMap(map);
        builder.getBackupDcs(clusterTbl, 1);
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql");
    }

}