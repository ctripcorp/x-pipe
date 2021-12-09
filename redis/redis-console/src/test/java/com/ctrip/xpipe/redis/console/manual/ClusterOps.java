package com.ctrip.xpipe.redis.console.manual;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.console.controller.api.data.meta.ClusterCreateInfo;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.util.ArrayList;

/**
 * @author wenchao.meng
 * Dec 04, 2021
 */
public class AddCluster extends AbstractTest {


    private String clusterName = System.getProperty("clusterName", "cluster_three_dc");
    private int shardCount = Integer.parseInt(System.getProperty("shardCount", "10"));
    private String url = System.getProperty("url", "http://127.0.0.1");

    private String dcs[] = new String[]{"SHAJQ", "SHAOY", "PTJQ"};
    private String redisHosts[] = new String[]{
            "127.0.0.1", "127.0.0.2", "127.0.0.3"
    };
    private int startPort = 6379;
    private RestOperations restOperations;

    @Before
    public void beforeAddCluster() {
        restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();
    }

    @Test
    public void addCluster() {

        ClusterCreateInfo clusterCreateInfo = new ClusterCreateInfo();
        clusterCreateInfo.setClusterName(clusterName);
        clusterCreateInfo.setDcs(Lists.newArrayList(dcs));
        //create cluster
        RetMessage response = restOperations.postForObject(url + "/api/clusters", clusterCreateInfo, RetMessage.class);

    }

    @Test
    public void testRemoveCluster(){

    }

}
