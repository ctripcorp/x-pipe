package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;

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
}
