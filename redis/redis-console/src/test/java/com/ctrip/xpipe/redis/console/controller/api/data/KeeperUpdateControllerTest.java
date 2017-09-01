package com.ctrip.xpipe.redis.console.controller.api.data;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * Created by zhuchen on 2017/8/29.
 */
public class KeeperUpdateControllerTest extends AbstractConsoleIntegrationTest {

    @Autowired
    KeeperUpdateController keeperUpdateController;

    @Test
    public void testAddKeepersWithNonKCForOrg() {
        String dcName = "oy";
        String clusterName = "cluster3";
        String shardName = "cluster3shard1";
        keeperUpdateController.addKeepers(dcName, clusterName, shardName);
        for (String s : keeperUpdateController.getKeepers(dcName, clusterName, shardName)) {
            logger.info(s);
        }
    }

    @Test
    public void testAddKeepersWithKCForOrg() {
        String dcName = "jq";
        String clusterName = "cluster2";
        String shardName = "cluster2shard1";
        keeperUpdateController.addKeepers(dcName, clusterName, shardName);
        for (String s : keeperUpdateController.getKeepers(dcName, clusterName, shardName)) {
            logger.info(s);
        }
    }

    @Test
    public void testAddKeepersWithDefaultOrg() {
        String dcName = "jq";
        String clusterName = "cluster1";
        String shardName = "shard1";
        keeperUpdateController.addKeepers(dcName, clusterName, shardName);
        for (String s : keeperUpdateController.getKeepers(dcName, clusterName, shardName)) {
            logger.info(s);
        }
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/keeper-update-controller-test.sql");
    }
}
