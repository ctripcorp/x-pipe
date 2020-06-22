package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Jun 17, 2020
 */
public class DcClusterServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    private DcClusterServiceImpl dcClusterService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/cluster-service-impl-test2.sql");
    }

    @Test
    public void testFind() {
        DcClusterTbl dcClusterTbl = dcClusterService.find(1L, 101L);
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(101L, dcClusterTbl.getDcClusterId());
    }

    @Test
    public void testTestFind() {
        DcClusterTbl dcClusterTbl = dcClusterService.find("jq", "cluster101");
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(101L, dcClusterTbl.getDcClusterId());
    }

    @Test
    public void testAddDcCluster() {
        dcClusterService.addDcCluster("fra", "cluster101");
        DcClusterTbl dcClusterTbl = dcClusterService.find("fra", "cluster101");
        Assert.assertNotNull(dcClusterTbl);
        Assert.assertEquals(101L, dcClusterTbl.getClusterId());
    }

    @Test
    public void testFindAllDcClusters() {
        dcClusterService.findAllDcClusters();
    }

    @Test
    public void testFindByClusterIds() {
        List<DcClusterTbl> result = dcClusterService.findByClusterIds(Lists.newArrayList(101L));
        Assert.assertNotNull(result);
        Assert.assertEquals(101L, result.get(0).getClusterId());
    }

    @Test
    public void testFindAllByDcId() {
    }

    @Test
    public void testFindClusterRelated() {
        List<DcClusterTbl> dcClusterTbls = dcClusterService.findClusterRelated(101L);
        Assert.assertEquals(2, dcClusterTbls.size());
    }
}