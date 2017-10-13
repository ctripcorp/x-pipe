package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.*;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public abstract class AbstractServiceImplTest extends AbstractConsoleIntegrationTest{

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private KeepercontainerService keepercontainerService;

    @Autowired
    private ShardService shardService;

    @Autowired
    private RedisService redisService;

    private Long []dcIds = new Long[]{1L, 2L};
    protected String clusterName = "cluster1";
    protected String []shardNames = new String[]{"shard1", "shard2"};

    private final int initRedisPort = 6379;
    private final int initKeeperPort = 8000;

    //TODO auto generate by h2
    private AtomicLong redisId = new AtomicLong(1);

    @Before
    public void beforeAbstractServiceImpl(){

        ClusterModel clusterModel = new ClusterModel();

        clusterModel.setClusterTbl(new ClusterTbl().
                setClusterName(clusterName)
                .setActivedcId(dcIds[0])
                .setClusterDescription("desc")
                .setClusterAdminEmails("test@ctrip.com"));


        clusterModel.setShards(createShards(shardNames));

        DcTbl slaveDc = new DcTbl().setDcName(dcNames[1]);
        clusterModel.setSlaveDcs(Lists.newArrayList(slaveDc));

        clusterService.createCluster(clusterModel);

        createRedisAndKeepers();
    }

    private void createRedisAndKeepers() {

        int redisPort = initRedisPort;
        int keeperPort = initKeeperPort;

        for(String dcName : dcNames){

            List<KeepercontainerTbl> keepercontainerTbls = keepercontainerService.findAllActiveByDcName(dcName);

            Assert.assertTrue(keepercontainerTbls.size() >= 2);
            long keepercontainerId1 = keepercontainerTbls.get(0).getKeepercontainerId();
            long keepercontainerId2 = keepercontainerTbls.get(1).getKeepercontainerId();

            String clusterName = this.clusterName;
            for(String shardName : shardNames){

                ShardModel shardModel = new ShardModel();
                shardModel.addRedis(new RedisTbl().setId(redisId.incrementAndGet()).setRedisIp("127.0.0.1").setRedisPort(redisPort++).setMaster(true));
                shardModel.addRedis(new RedisTbl().setId(redisId.incrementAndGet()).setRedisIp("127.0.0.1").setRedisPort(redisPort++));
                shardModel.addKeeper(new RedisTbl().setId(redisId.incrementAndGet()).setKeepercontainerId(keepercontainerId1).setRedisIp("127.0.0.1").setRedisPort(keeperPort++));
                shardModel.addKeeper(new RedisTbl().setId(redisId.incrementAndGet()).setKeepercontainerId(keepercontainerId2).setRedisIp("127.0.0.1").setRedisPort(keeperPort++));
                redisService.updateRedises(dcName, clusterName, shardName, shardModel);

            }
        }

    }

    private List<ShardModel> createShards(String[] shardNames) {

        List<ShardModel> shards = new LinkedList<>();

        for(String shardName : shardNames){
            ShardModel shardModel = new ShardModel();
            shardModel.setShardTbl(new ShardTbl().setShardName(shardName).setSetinelMonitorName(shardName));
            shards.add(shardModel);
        }

        return shards;
    }
}
