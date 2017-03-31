package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.constant.XpipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.ClusterModel;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.console.model.ShardModel;
import com.ctrip.xpipe.redis.console.service.RedisService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author wenchao.meng
 *         <p>
 *         Mar 31, 2017
 */
public class RedisServiceImplTest extends AbstractServiceImplTest{


    @Autowired
    private RedisService redisService;


    @Test
    public void testUpdateRedises() throws IOException {

        String dcName = dcNames[0];
        String shardName = shardNames[0];

        List<RedisTbl> allByDcClusterShard = redisService.findAllByDcClusterShard(dcName, clusterName, shardName);

        checkAllInstances(allByDcClusterShard);

        boolean firstSlave = true;
        RedisTbl newMaster = null;

        for(RedisTbl redisTbl : allByDcClusterShard){
            if(redisTbl.getRedisRole().equals(XpipeConsoleConstant.ROLE_REDIS)){

                if(redisTbl.isMaster()){
                    redisTbl.setMaster(false);
                }else if(!redisTbl.isMaster() && firstSlave){
                    redisTbl.setMaster(true);
                    newMaster = redisTbl;
                    firstSlave = false;
                }

            }
        }

        checkAllInstances(allByDcClusterShard);

        ShardModel shardModel = new ShardModel(allByDcClusterShard);
        redisService.updateRedises(clusterName, dcName, shardName, shardModel);

        allByDcClusterShard = redisService.findAllByDcClusterShard(dcName, clusterName, shardName);
        checkAllInstances(allByDcClusterShard);

        Stream<RedisTbl> redisTblStream = allByDcClusterShard.stream().filter(instance -> instance.isMaster());

        RedisTbl  currentMaster = redisTblStream.findFirst().get();

        Assert.assertEquals(newMaster.getId(), currentMaster.getId());

    }

    private void checkAllInstances(List<RedisTbl> allByDcClusterShard) {

        Assert.assertEquals(4, allByDcClusterShard.size());

        int masterCount = 0;

        for(RedisTbl redisTbl : allByDcClusterShard){
            logger.debug("{}", redisTbl);
            if(redisTbl.isMaster()){
                masterCount++;
            }
        }
        Assert.assertEquals(1, masterCount);

    }

}
