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

import java.util.Collections;
import java.util.Calendar;
import java.util.Date;
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

    @Test
    public void testUpdateBatchPreservesCreateTime() throws Exception {
        RedisTbl redis = redises.get(0);
        executeSqlScript(String.format(
                "UPDATE REDIS_TBL SET create_time = '2023-11-14 22:13:20' WHERE id = %d", redis.getId()));
        Date expectedCreateTime = redisDao.findByPK(redis.getId()).getCreateTime();

        RedisTbl updateProto = new RedisTbl()
                .setId(redis.getId())
                .setRedisIp(redis.getRedisIp())
                .setRedisPort(redis.getRedisPort())
                .setRunId(redis.getRunId())
                .setKeeperActive(redis.isKeeperActive())
                .setMaster(!redis.isMaster())
                .setKeepercontainerId(redis.getKeepercontainerId())
                .setRedisRole(redis.getRedisRole())
                .setDcClusterShardId(redis.getDcClusterShardId())
                .setAzId(redis.getAzId());

        redisDao.updateBatch(Collections.singletonList(updateProto));

        RedisTbl fromDb = redisDao.findByPK(redis.getId());
        Assert.assertEquals(expectedCreateTime, fromDb.getCreateTime());
        Assert.assertEquals(updateProto.isMaster(), fromDb.isMaster());
    }

    @Test
    public void testCreateRedisesBatchPersistsExplicitCreateTime() throws DalException {
        Date expectedCreateTime = dateTime(2024, 6, 18, 14, 30, 0);
        RedisTbl redis = createRedis(XPipeConsoleConstant.ROLE_REDIS)
                .setCreateTime(expectedCreateTime);

        redisDao.createRedisesBatch(Collections.singletonList(redis));

        Assert.assertEquals(expectedCreateTime, redisDao.findByPK(redis.getId()).getCreateTime());
    }

    @Test
    public void testCreateTimeBeyond2038RoundTrip() throws Exception {
        Date expectedCreateTime = dateTime(2040, 1, 15, 10, 20, 30);

        RedisTbl redis = createRedis(XPipeConsoleConstant.ROLE_REDIS)
                .setCreateTime(expectedCreateTime);
        redisDao.createRedisesBatch(Collections.singletonList(redis));
        Assert.assertEquals(expectedCreateTime, redisDao.findByPK(redis.getId()).getCreateTime());

        executeSqlScript(String.format(
                "UPDATE REDIS_TBL SET create_time = '2040-01-15 10:20:30' WHERE id = %d", redis.getId()));
        Assert.assertEquals(expectedCreateTime, redisDao.findByPK(redis.getId()).getCreateTime());
    }

    private static Date dateTime(int year, int month, int day, int hour, int minute, int second) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(year, month - 1, day, hour, minute, second);
        return calendar.getTime();
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
                .setMaster((randomInt() & 1) == 0)
                .setAzId(0L);
    }
}
