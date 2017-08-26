package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

/**
 * Created by zhuchen on 2017/8/26 0026.
 */
public class ClusterDaoTest  extends AbstractConsoleIntegrationTest {
    @Autowired
    ClusterDao clusterDao;

    ClusterTbl clusterTbl;

    @Before
    public void beforeClusterDaoTest() {
        clusterTbl = new ClusterTbl()
                .setClusterDescription("ut-cluster")
                .setActivedcId(1)
                .setClusterName("ut-cluster")
                .setCount(12)
                .setIsXpipeInterested(true)
                .setClusterLastModifiedTime("test-last-modified")
                .setStatus("normal");
    }


    @Test
    public void testCreateCluster() throws DalException {
        ClusterTbl newCluster = clusterDao.createCluster(clusterTbl);
        Assert.assertEquals(clusterTbl.getId(), newCluster.getId());
    }
}
