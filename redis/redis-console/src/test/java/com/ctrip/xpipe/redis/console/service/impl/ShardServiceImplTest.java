package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.ShardTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public class ShardServiceImplTest extends AbstractServiceImplTest{

    @Autowired
    private ShardServiceImpl shardService;

    @Test
    public void testFindAllByClusterName(){

        List<ShardTbl> allByClusterName = shardService.findAllByClusterName(clusterName);
        Assert.assertEquals(shardNames.length, allByClusterName.size());

        String invalid = randomString(10);
        allByClusterName = shardService.findAllByClusterName(invalid);
        Assert.assertEquals(0, allByClusterName.size());
    }

}
