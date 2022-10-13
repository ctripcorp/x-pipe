package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2017
 */
public class ShardDaoTest extends AbstractConsoleIntegrationTest {

    @Autowired
    ShardDao shardDao;

    @Test
    public void testDeleteShardsBatch() throws Exception {
        shardDao.deleteShardsBatch(new LinkedList<ShardTbl>());
    }

    @Test
    public void testQueryAllShardMonitors() throws Exception {
        Set<String> expectedMonitors = Sets.newHashSet("shard1", "shard2", "cluster2shard1",
                "cluster2shard2", "cluster3shard1", "cluster3shard2");
        Set<String> monitors = shardDao.queryAllShardMonitorNames();
        Assert.assertEquals(expectedMonitors, monitors);
    }

    @Test
    public void testAddShardThenSentinelNameShouldEqShardName() throws DalException {
        shardDao.createShard("cluster1", new ShardTbl().setShardName("cluster-test-1_1"));
    }

    @Override
    protected String prepareDatas() throws IOException {
        String path = "src/test/resources/shard-dao-test.sql";
        return prepareDatasFromFile(path);
    }
}
