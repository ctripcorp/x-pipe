package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.DcClusterDelayMarkDown;
import com.ctrip.xpipe.redis.console.config.DcsRelationsInfo;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 15, 2017
 */
public class DefaultConsoleConfigTest extends AbstractConsoleTest{

    private ConsoleConfig consoleConfig;

    @Before
    public void beforeDefaultConsoleConfigTest(){
        consoleConfig = new DefaultConsoleConfig();
    }


    @Test
    public void testWhiteList(){

        System.setProperty(DefaultConsoleConfig.KEY_ALERT_WHITE_LIST, "cluster1, cluster2 ; cluster3");

        Set<String> whiteList = consoleConfig.getAlertWhileList();

        Set<String> result = Sets.newHashSet("cluster1", "cluster2", "cluster3");

        Assert.assertEquals(result, whiteList);

    }

    @Test
    public void testGetDcRelations1() {
        String dcsRelations = "{\"healthyDelayPerDistance\":2000, \"relations\":[{\"dcs\": \"jq,oy\",\"distance\":1},{\"dcs\": \"jq,fra\",\"distance\":15},{\"dcs\": \"fra,oy\",\"distance\":15}]}";
        System.setProperty(DefaultConsoleConfig.KEY_DCS_RELATIONS, dcsRelations);

        DcsRelationsInfo dcsRelationsInfo = consoleConfig.getDcsRelationsInfo();
        Assert.assertEquals(2000, dcsRelationsInfo.getHealthyDelayPerDistance());
        Assert.assertEquals(3, dcsRelationsInfo.getRelations().size());
    }

    @Test
    public void testGetDcRelations2() {
        String dcsRelations = "{\"relations\":[{\"dcs\": \"jq,oy\",\"distance\":1},{\"dcs\": \"jq,fra\",\"distance\":15}]}";
        System.setProperty(DefaultConsoleConfig.KEY_DCS_RELATIONS, dcsRelations);

        DcsRelationsInfo dcsRelationsInfo = consoleConfig.getDcsRelationsInfo();
        Assert.assertEquals(0, dcsRelationsInfo.getHealthyDelayPerDistance());
        Assert.assertEquals(2, dcsRelationsInfo.getRelations().size());
    }

    @Test
    public void testGetDcRelations3() {
        String dcsRelations = "{\"healthyDelayPerDistance\":2000 }";
        System.setProperty(DefaultConsoleConfig.KEY_DCS_RELATIONS, dcsRelations);

        DcsRelationsInfo dcsRelationsInfo = consoleConfig.getDcsRelationsInfo();
        Assert.assertEquals(2000, dcsRelationsInfo.getHealthyDelayPerDistance());
        Assert.assertNull(dcsRelationsInfo.getRelations());
    }

    @Test
    public void testGetDcRelations4() {
        DcsRelationsInfo dcsRelationsInfo = consoleConfig.getDcsRelationsInfo();
        Assert.assertEquals(2000, dcsRelationsInfo.getHealthyDelayPerDistance());
        Assert.assertEquals(1, dcsRelationsInfo.getRelations().size());
    }

    @Test
    public void testDcClusterWontMarkDown(){

        System.setProperty(DefaultConsoleConfig.KEY_DC_CLUSTER_WONT_MARK_DOWN, "FAT-AWS:cluster_shyin, FAT:cluster_shyin:300");

        Set<DcClusterDelayMarkDown> result = consoleConfig.getDelayedMarkDownDcClusters();

        Set<DcClusterDelayMarkDown> expected = Sets.newHashSet(
                new DcClusterDelayMarkDown().setDcId("FAT-AWS").setClusterId("cluster_shyin").setDelaySecond(3600),
                new DcClusterDelayMarkDown().setDcId("FAT").setClusterId("cluster_shyin").setDelaySecond(300));

        Assert.assertEquals(result, expected);

    }

}
