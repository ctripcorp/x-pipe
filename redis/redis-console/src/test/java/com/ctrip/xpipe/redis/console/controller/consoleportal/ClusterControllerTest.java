package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.ReplDirectionInfoModel;
import com.ctrip.xpipe.redis.console.model.ReplDirectionTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ReplDirectionService;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterControllerTest extends AbstractConsoleIntegrationTest {
    @Autowired
    ClusterController controller;

    @Autowired
    ReplDirectionService replDirectionService;

    @Autowired
    ClusterService clusterService;

    @Test
    public void testUpdateReplDirection() {
        String heteroClusterName = "hetero-cluster";
        String wrongClusterName = "hetero-cluster2";
        long heteroClusterId = 7;
        ClusterTbl clusterTbl = clusterService.find(heteroClusterName);

        ReplDirectionTbl replDirectionTbl = replDirectionService.findReplDirectionTblById(1L);
        Assert.assertEquals(2, replDirectionTbl.getToDcId());
        replDirectionTbl = replDirectionService.findReplDirectionTblById(2L);
        Assert.assertEquals(3, replDirectionTbl.getToDcId());

        ReplDirectionInfoModel replDirectionInfoModel1 = new ReplDirectionInfoModel().setClusterName(heteroClusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("oy").setId(2L);
        ReplDirectionInfoModel replDirectionInfoModel2 = new ReplDirectionInfoModel().setClusterName(heteroClusterName)
                .setSrcDcName("jq").setFromDcName("jq").setToDcName("fra").setId(1L);

        controller.updateClusterReplDirections(clusterTbl, Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));

        replDirectionTbl = replDirectionService.findReplDirectionTblById(1L);
        Assert.assertEquals(3, replDirectionTbl.getToDcId());
        replDirectionTbl = replDirectionService.findReplDirectionTblById(2L);
        Assert.assertEquals(2, replDirectionTbl.getToDcId());

        try {
            controller.updateClusterReplDirections(null, Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));
        } catch (Exception e) {
            Assert.assertEquals("[updateClusterReplDirections] cluster can not be null!", e.getMessage());
        }

        replDirectionInfoModel1.setClusterName(wrongClusterName);
        try {
            controller.updateClusterReplDirections(clusterTbl, Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));
        } catch (Exception e) {
            Assert.assertEquals("[updateClusterReplDirections] repl direction should belong to cluster:7, but belong to cluster:8", e.getMessage());
        }

        replDirectionInfoModel1.setClusterName(heteroClusterName).setSrcDcName("oy");
        try {
            controller.updateClusterReplDirections(clusterTbl, Lists.newArrayList(replDirectionInfoModel1, replDirectionInfoModel2));
        } catch (Exception e) {
            Assert.assertEquals("[updateClusterReplDirections] repl direction should copy from src dc:1, but from 2", e.getMessage());
        }
    }

    @Override
    protected String prepareDatas() throws IOException {
        return  prepareDatasFromFile("src/test/resources/hetero-cluster-test.sql");
    }
}