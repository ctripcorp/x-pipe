package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;


public class ClusterServiceImplTest5 extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterServiceImpl clusterService;

    @Override
    public String prepareDatas() {
        try {
            return prepareDatasFromFile("src/test/resources/cluster-service-impl-test5.sql");
        } catch (Exception e) {
            logger.error("[ClusterServiceImplTest5]prepare data error for path", e);
        }
        return "";
    }

    @Test
    public void testFindAllClustersByDcNameBind() {
        List<ClusterTbl> result = clusterService.findAllClusterByDcNameBind("jq");
        Assert.assertEquals(5, result.size());
        result = clusterService.findAllClusterByDcNameBind("oy");
        Assert.assertEquals(5, result.size());
        result = clusterService.findAllClusterByDcNameBind("fra");
        Assert.assertEquals(3, result.size());
    }

    @Test
    public void testFindAllClusterByDcNameBindAndType() {
        List<ClusterTbl> result = clusterService.findAllClusterByDcNameBindAndType("jq", "one_way", true);
        Assert.assertEquals(3, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("oy", "one_way", true);
        Assert.assertEquals(2, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("fra", "one_way", true);
        Assert.assertEquals(0, result.size());

        result = clusterService.findAllClusterByDcNameBindAndType("jq", "one_way", false);
        Assert.assertEquals(1, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("oy", "ONE_WAY", false);
        Assert.assertEquals(1, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("fra", "one_way", false);
        Assert.assertEquals(0, result.size());

        result = clusterService.findAllClusterByDcNameBindAndType("jq", "single_dc", true);
        Assert.assertEquals(1, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("oy", "SINGLE_DC", true);
        Assert.assertEquals(2, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("fra", "SINGLE_DC", true);
        Assert.assertEquals(3, result.size());

        result = clusterService.findAllClusterByDcNameBindAndType("jq", "single_dc", false);
        Assert.assertEquals(1, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("oy", "SINGLE_DC", false);
        Assert.assertEquals(1, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("fra", "SINGLE_DC", false);
        Assert.assertEquals(0, result.size());

        result = clusterService.findAllClusterByDcNameBindAndType("jq", "hetero", false);
        Assert.assertEquals(2, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("oy", "HETERO", false);
        Assert.assertEquals(2, result.size());
        result = clusterService.findAllClusterByDcNameBindAndType("fra", "hetero", false);
        Assert.assertEquals(3, result.size());
    }

    @Test
    public void testFindActiveClustersByDcName() {
        List<ClusterTbl> result = clusterService.findActiveClustersByDcName("jq");
        Assert.assertEquals(5, result.size());
        result = clusterService.findActiveClustersByDcName("oy");
        Assert.assertEquals(5, result.size());
        result = clusterService.findActiveClustersByDcName("fra");
        Assert.assertEquals(3, result.size());
    }

    @Test
    public void testFindActiveClustersByDcNameAndType() {
        List<ClusterTbl> result = clusterService.findActiveClustersByDcNameAndType("jq", "one_way", true);
        Assert.assertEquals(3, result.size());
        result = clusterService.findActiveClustersByDcNameAndType("oy", "one_way", true);
        Assert.assertEquals(2, result.size());
        result = clusterService.findActiveClustersByDcNameAndType("fra", "one_way", true);
        Assert.assertEquals(0, result.size());

        result = clusterService.findActiveClustersByDcNameAndType("jq", "single_dc", true);
        Assert.assertEquals(1, result.size());
        result = clusterService.findActiveClustersByDcNameAndType("oy", "single_dc", true);
        Assert.assertEquals(2, result.size());
        result = clusterService.findActiveClustersByDcNameAndType("fra", "single_dc", true);
        Assert.assertEquals(3, result.size());

        result = clusterService.findActiveClustersByDcNameAndType("jq", "hetero", true);
        Assert.assertEquals(2, result.size());
        result = clusterService.findActiveClustersByDcNameAndType("oy", "hetero", true);
        Assert.assertEquals(2, result.size());
        result = clusterService.findActiveClustersByDcNameAndType("fra", "hetero", true);
        Assert.assertEquals(3, result.size());

        result = clusterService.findActiveClustersByDcNameAndType("jq", "single_dc", false);
        Assert.assertEquals(1, result.size());
        result = clusterService.findActiveClustersByDcNameAndType("oy", "single_dc", false);
        Assert.assertEquals(1, result.size());
        result = clusterService.findActiveClustersByDcNameAndType("fra", "single_dc", false);
        Assert.assertEquals(0, result.size());
    }

}