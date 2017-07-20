package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.console.model.RedisTbl;
import com.ctrip.xpipe.redis.core.redis.RunidGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.LinkedList;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 20, 2017
 */
public class RedisDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private RedisDao redisDao;

    private int count = 5;

    private List<RedisTbl> redises;

    private long dcClusterShardId = Integer.MAX_VALUE;

    @Before
    public void beforeRedisDaoTest() throws DalException {
        redises = createRedises(count);
        redisDao.createRedisesBatch(redises);
    }

    @Test
    public void testUpdateBatchMaster() throws DalException {

        for(RedisTbl redisTbl : redises){
            redisTbl.setMaster(!redisTbl.isMaster());
        }

        redisDao.updateBatchMaster(redises);

        for(RedisTbl redisTbl : redises){
            RedisTbl byPK = redisDao.findByPK(redisTbl.getId());
            Assert.assertEquals(redisTbl.isMaster(), byPK.isMaster());
        }

    }

    @Test
    public void testUpdateBatchKeeperActive() throws DalException {

        for(RedisTbl redisTbl : redises){
            redisTbl.setKeeperActive(!redisTbl.isKeeperActive());
        }

        redisDao.updateBatchKeeperActive(redises);

        for(RedisTbl redisTbl : redises){
            RedisTbl byPK = redisDao.findByPK(redisTbl.getId());
            Assert.assertEquals(redisTbl.isKeeperActive(), byPK.isKeeperActive());
        }

    }

    private List<RedisTbl> createRedises(int count) {

        List<RedisTbl> result = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            result.add(createRedis(XPipeConsoleConstant.ROLE_REDIS));
        }
        return result;
    }

    private RedisTbl createRedis(String role) {
        return new RedisTbl()
                .setKeepercontainerId(1)
                .setDcClusterShardId(dcClusterShardId)
                .setRedisIp("127.0.0.1")
                .setRedisPort(randomPort())
                .setRunId(RunidGenerator.DEFAULT.generateRunid())
                .setRedisRole(role)
                .setMaster((randomInt() & 1) == 0);
    }
}
