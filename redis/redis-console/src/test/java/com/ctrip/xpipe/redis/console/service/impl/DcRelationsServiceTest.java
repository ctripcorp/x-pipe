package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.model.ClusterDcRelations;
import com.ctrip.xpipe.redis.checker.model.DcRelation;
import com.ctrip.xpipe.redis.checker.model.DcsPriority;
import com.ctrip.xpipe.redis.checker.model.DcsRelations;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class DcRelationsServiceTest {

    @InjectMocks
    @Spy
    private final DefaultDcRelationsService dcRelationsService=new DefaultDcRelationsService();

    @Mock
    private ConsoleConfig config;

    private String configStr = "{\n" +
            "    \"delayPerDistance\":3000,"+
            "    \"dcLevel\":[\n" +
            "        {\n" +
            "            \"dcs\":\"sharb,shaxy\",\n" +
            "            \"distance\":1\n" +
            "        },\n" +
            "        {\n" +
            "            \"dcs\":\"SHARB,SHA-ALI\",\n" +
            "            \"distance\":15\n" +
            "        },\n" +
            "        {\n" +
            "            \"dcs\":\"SHAXY,SHA-ALI\",\n" +
            "            \"distance\":15\n" +
            "        }\n" +
            "    ],\n" +
            "    \"clusterLevel\":[\n" +
            "        {\n" +
            "            \"clusterName\":\"Cluster1\", \n" +
            "            \"relations\":[\n" +
            "                {\n" +
            "                    \"dcs\":\"SHARB,SHAXY\",\n" +
            "                    \"distance\":1\n" +
            "                },\n" +
            "                {\n" +
            "                    \"dcs\":\"sha-ali,shaxy\",\n" +
            "                    \"distance\":-1\n" +
            "                },\n" +
            "                {\n" +
            "                    \"dcs\":\"SHA-ALI,SHARB\",\n" +
            "                    \"distance\":-1\n" +
            "                }\n" +
            "            ]\n" +
            "        },\n" +
            "        {\n" +
            "            \"clusterName\":\"Cluster2\",   \n" +
            "            \"relations\":[\n" +
            "                {\n" +
            "                    \"dcs\":\"sharb,shaxy\",\n" +
            "                    \"distance\":2\n" +
            "                },\n" +
            "                {\n" +
            "                    \"dcs\":\"SHA-ALI,SHAXY\",\n" +
            "                    \"distance\":15\n" +
            "                },\n" +
            "                {\n" +
            "                    \"dcs\":\"SHA-ALI,SHARB\",\n" +
            "                    \"distance\":30\n" +
            "                }\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    @Test
    public void jsonTest() {
        DcsRelations dcsRelations = JsonCodec.INSTANCE.decode(configStr, DcsRelations.class);
        List<DcRelation> dcRelations = dcsRelations.getDcLevel();
        List<ClusterDcRelations> clusterDcRelations = dcsRelations.getClusterLevel();
        Assert.assertEquals(3, dcRelations.size());
        Assert.assertEquals(2, clusterDcRelations.size());
    }

    @Test
    public void refreshTest() throws Exception {
        Mockito.when(config.getDcsRelations()).thenReturn(configStr);
        dcRelationsService.refresh();

        Map<String, DcsPriority> clusterLevelDcPriority = dcRelationsService.getClusterLevelDcPriority();
        DcsPriority dcsPriority = dcRelationsService.getDcLevelPriority();
        Map<Set<String>, Integer> dcDistances = dcRelationsService.getDcsDistance();
        Map<String, Map<Set<String>, Integer>> clusterDcsDistance = dcRelationsService.getClusterDcsDistance();

        //check dcsPriority
        Assert.assertNotNull(dcsPriority);
        Map<Integer, List<String>> aliPriority = dcsPriority.getDcPriority("SHA-ALI").getPriority2Dcs();
        Assert.assertEquals(1, aliPriority.size());
        Assert.assertEquals(2, aliPriority.get(15).size());
        Assert.assertTrue(aliPriority.get(15).contains("SHARB"));
        Assert.assertTrue(aliPriority.get(15).contains("SHAXY"));

        Map<Integer, List<String>> rbPriority = dcsPriority.getDcPriority("SHARB").getPriority2Dcs();
        Assert.assertEquals(2, rbPriority.size());
        Assert.assertEquals(1, rbPriority.get(1).size());
        Assert.assertEquals(1, rbPriority.get(15).size());
        Assert.assertTrue(rbPriority.get(1).contains("SHAXY"));
        Assert.assertTrue(rbPriority.get(15).contains("SHA-ALI"));

        Map<Integer, List<String>> xyPriority = dcsPriority.getDcPriority("SHAXY").getPriority2Dcs();
        Assert.assertEquals(2, xyPriority.size());
        Assert.assertEquals(1, xyPriority.get(1).size());
        Assert.assertEquals(1, xyPriority.get(15).size());
        Assert.assertTrue(xyPriority.get(1).contains("SHARB"));
        Assert.assertTrue(xyPriority.get(15).contains("SHA-ALI"));

        //check clusterLevelDcPriority
        Assert.assertNotNull(clusterLevelDcPriority);
        Assert.assertEquals(2, clusterLevelDcPriority.size());
        DcsPriority cluster1DcPriority = clusterLevelDcPriority.get("cluster1");
        aliPriority = cluster1DcPriority.getDcPriority("SHA-ALI").getPriority2Dcs();
        Assert.assertEquals(1, aliPriority.size());
        Assert.assertTrue(aliPriority.get(-1).contains("SHAXY"));
        Assert.assertTrue(aliPriority.get(-1).contains("SHARB"));

        rbPriority = cluster1DcPriority.getDcPriority("SHARB").getPriority2Dcs();
        Assert.assertEquals(2, rbPriority.size());
        Assert.assertEquals(1, rbPriority.get(-1).size());
        Assert.assertEquals(1, rbPriority.get(1).size());
        Assert.assertTrue(rbPriority.get(1).contains("SHAXY"));
        Assert.assertTrue(rbPriority.get(-1).contains("SHA-ALI"));

        xyPriority = cluster1DcPriority.getDcPriority("SHAXY").getPriority2Dcs();
        Assert.assertEquals(2, xyPriority.size());
        Assert.assertEquals(1, xyPriority.get(-1).size());
        Assert.assertEquals(1, xyPriority.get(1).size());
        Assert.assertTrue(xyPriority.get(1).contains("SHARB"));
        Assert.assertTrue(xyPriority.get(-1).contains("SHA-ALI"));


        DcsPriority cluster2DcPriority = clusterLevelDcPriority.get("cluster2");
        aliPriority = cluster2DcPriority.getDcPriority("SHA-ALI").getPriority2Dcs();
        Assert.assertEquals(2, aliPriority.size());
        Assert.assertEquals(1, aliPriority.get(15).size());
        Assert.assertTrue(aliPriority.get(15).contains("SHAXY"));
        Assert.assertEquals(1, aliPriority.get(30).size());
        Assert.assertTrue(aliPriority.get(30).contains("SHARB"));


        rbPriority = cluster2DcPriority.getDcPriority("SHARB").getPriority2Dcs();
        Assert.assertEquals(2, rbPriority.size());
        Assert.assertEquals(1, rbPriority.get(2).size());
        Assert.assertTrue(rbPriority.get(2).contains("SHAXY"));
        Assert.assertEquals(1, rbPriority.get(30).size());
        Assert.assertTrue(rbPriority.get(30).contains("SHA-ALI"));

        xyPriority = cluster2DcPriority.getDcPriority("SHAXY").getPriority2Dcs();
        Assert.assertEquals(2, xyPriority.size());
        Assert.assertEquals(1, xyPriority.get(2).size());
        Assert.assertTrue(xyPriority.get(2).contains("SHARB"));
        Assert.assertEquals(1, xyPriority.get(15).size());
        Assert.assertTrue(xyPriority.get(15).contains("SHA-ALI"));

        //check delay per distantce
        Assert.assertEquals(3000, dcRelationsService.getDelayPerDistance().intValue());

        //check dcs distance
        Assert.assertEquals(15, dcDistances.get(Sets.newHashSet("SHA-ALI", "SHARB")).intValue());
        Assert.assertEquals(15, dcDistances.get(Sets.newHashSet("SHA-ALI", "SHAXY")).intValue());
        Assert.assertEquals(1, dcDistances.get(Sets.newHashSet("SHAXY", "SHARB")).intValue());
        Assert.assertNull(dcDistances.get(Sets.newHashSet("SHAXY", "SHAFQ")));

        //check cluster dcs distance
        Map<Set<String>, Integer> cluster1 = clusterDcsDistance.get("cluster1");
        Assert.assertEquals(-1, cluster1.get(Sets.newHashSet("SHA-ALI", "SHARB")).intValue());
        Assert.assertEquals(-1, cluster1.get(Sets.newHashSet("SHA-ALI", "SHAXY")).intValue());
        Assert.assertEquals(1, cluster1.get(Sets.newHashSet("SHAXY", "SHARB")).intValue());

        Map<Set<String>, Integer> cluster2 = clusterDcsDistance.get("cluster2");
        Assert.assertEquals(30, cluster2.get(Sets.newHashSet("SHA-ALI", "SHARB")).intValue());
        Assert.assertEquals(15, cluster2.get(Sets.newHashSet("SHA-ALI", "SHAXY")).intValue());
        Assert.assertEquals(2, cluster2.get(Sets.newHashSet("SHAXY", "SHARB")).intValue());

        Assert.assertNull(clusterDcsDistance.get("cluster3"));
    }

    @Test
    public void getClusterTargetDcByPriorityTest() throws Exception {
        Mockito.when(config.getDcsRelations()).thenReturn(configStr);
        dcRelationsService.refresh();

        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals("SHARB", dcRelationsService.getClusterTargetDcByPriority(234, "clustEr3", "sha-ALi", Lists.newArrayList("shaRB", "Shaxy")));
        }

        Mockito.verify(dcRelationsService, Mockito.times(1)).getTargetDcs(Mockito.any(), Mockito.any());

        Assert.assertNull(dcRelationsService.getClusterTargetDcByPriority(234, "clustEr3", "sha-ALi", null));
        Assert.assertNull(dcRelationsService.getClusterTargetDcByPriority(234, "clustEr3", "sha-ALi", Lists.newArrayList()));
        Assert.assertNull(dcRelationsService.getClusterTargetDcByPriority(234, "clustEr3", "sha-ALi", Lists.newArrayList("SHAFQ")));
    }

    @Test
    public void getClusterLevelTargetDcsTest() throws Exception {
        Mockito.when(config.getDcsRelations()).thenReturn(configStr);
        dcRelationsService.refresh();

        List<String> targetDcs = dcRelationsService.getTargetDcsByPriority("cluSter2", "sha-ali", Lists.newArrayList("sharb", "shaxy"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertEquals("SHAXY", targetDcs.get(0));

        targetDcs = dcRelationsService.getTargetDcsByPriority("clusTer2", "sharb", Lists.newArrayList("sha-ali", "SHAXY"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertEquals("SHAXY", targetDcs.get(0));

        targetDcs = dcRelationsService.getTargetDcsByPriority("clusTer1", "shaxy", Lists.newArrayList("SHA-ALI", "sharb"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHARB"));

        targetDcs = dcRelationsService.getTargetDcsByPriority("clusTer1", "sha-ali", Lists.newArrayList("shaxy", "SHARB"));
        Assert.assertEquals(0, targetDcs.size());

        targetDcs = dcRelationsService.getTargetDcsByPriority("clUster1", "CFTRB", Lists.newArrayList("CFTXY"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("CFTXY"));
    }

    @Test
    public void getDcLevelTargetDcsTest() throws Exception {
        Mockito.when(config.getDcsRelations()).thenReturn(configStr);
        dcRelationsService.refresh();

        List<String> targetDcs = dcRelationsService.getTargetDcsByPriority("cluster3", "SHA-ALI", Lists.newArrayList("SHARB", "SHAXY"));
        Assert.assertEquals(2, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHARB"));
        Assert.assertTrue(targetDcs.contains("SHAXY"));

        targetDcs = dcRelationsService.getTargetDcsByPriority("cluster3", "SHARB", Lists.newArrayList("SHA-ALI", "SHAXY"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHAXY"));

        targetDcs = dcRelationsService.getTargetDcsByPriority("cluster3", "SHAXY", Lists.newArrayList("SHA-ALI", "SHARB"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHARB"));

        targetDcs = dcRelationsService.getTargetDcsByPriority("cluster3", "SHAFQ", Lists.newArrayList("SHAXY", "SHARB"));
        Assert.assertEquals(2, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHARB"));
        Assert.assertTrue(targetDcs.contains("SHAXY"));

        targetDcs = dcRelationsService.getTargetDcsByPriority("cluster3", "CFTRB", Lists.newArrayList("CFTXY"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("CFTXY"));
    }

    @Test
    public void getClusterDcsDelayTest() throws Exception {
        Mockito.when(config.getDcsRelations()).thenReturn(configStr);
        dcRelationsService.refresh();

        Assert.assertEquals(-3000, dcRelationsService.getClusterDcsDelay("clUster1", "SHA-ALI", "sharb").intValue());
        Assert.assertEquals(-3000, dcRelationsService.getClusterDcsDelay("clUster1", "SHA-Ali", "shaxy").intValue());
        Assert.assertEquals(3000, dcRelationsService.getClusterDcsDelay("clusTer1", "SHAxy", "sharb").intValue());
        Assert.assertNull(dcRelationsService.getClusterDcsDelay("clusTer1", "shaxy", "shafq"));

        Assert.assertNull(dcRelationsService.getClusterDcsDelay("clusTer3", "SHAxy", "sharb"));

        Assert.assertEquals(90000, dcRelationsService.getClusterDcsDelay("clUster2", "SHA-ALI", "sharb").intValue());
        Assert.assertEquals(45000, dcRelationsService.getClusterDcsDelay("clUster2", "SHA-Ali", "shaxy").intValue());
        Assert.assertEquals(6000, dcRelationsService.getClusterDcsDelay("clusTer2", "SHAxy", "sharb").intValue());
    }

    @Test
    public void getDcsDelayTest() throws Exception {
        Mockito.when(config.getDcsRelations()).thenReturn(configStr);
        dcRelationsService.refresh();

        Assert.assertEquals(45000, dcRelationsService.getDcsDelay("SHA-ALI", "sharb").intValue());
        Assert.assertEquals(45000, dcRelationsService.getDcsDelay("SHA-Ali", "shaxy").intValue());
        Assert.assertEquals(3000, dcRelationsService.getDcsDelay("SHAxy", "sharb").intValue());
        Assert.assertNull(dcRelationsService.getDcsDelay("shaxy", "shafq"));
    }

    @Test
    public void getExcludeDcsForBiClusterTest() throws Exception {
        Mockito.when(config.getDcsRelations()).thenReturn(configStr);

        //not initialized
        Set<String> excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB"), Sets.newHashSet("shaXy", "shA-ali"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        dcRelationsService.refresh();

        //empty available dcs
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB"), Sets.newHashSet());
        Assert.assertEquals(Sets.newHashSet(), excludedDcs);

        //not existed cluster, use dc priority
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster3", Sets.newHashSet("sharB"), Sets.newHashSet("shaXy", "shA-ali"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        //sharb down, ignore ali
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB"), Sets.newHashSet("shaXy", "shA-ali"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        //shaxy down, ignore ali
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("shaXy"), Sets.newHashSet("shaRB", "shA-ali"));
        Assert.assertEquals(Sets.newHashSet("SHAXY"), excludedDcs);

        //ali down, no available dc
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sha-ALI"), Sets.newHashSet("shaRB", "shaXY"));
        Assert.assertEquals(Sets.newHashSet(), excludedDcs);

        //shaxy and sharb down
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharb","shaxy"), Sets.newHashSet("sha-ali"));
        Assert.assertEquals(Sets.newHashSet(), excludedDcs);

        //multi target dcs
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB"), Sets.newHashSet("sha-ali", "shaXY","shafq"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        // shaali and sharb down
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster3", Sets.newHashSet("sharB", "shA-ali"), Sets.newHashSet("shaXy"));
        Assert.assertEquals(Sets.newHashSet("SHARB", "SHA-ALI"), excludedDcs);

        // shaali and sharb down, no downgrade for shaali
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB", "shA-ali"), Sets.newHashSet("shaXy"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        //no ignore dcs
        excludedDcs = dcRelationsService.getExcludedDcsForBiCluster("clUster2", Sets.newHashSet("sharB"), Sets.newHashSet("sha-ali", "shaXY"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

    }

    private DcsRelations buildDcsDistances() {
        List<DcRelation> dcRelations = Lists.newArrayList(
                new DcRelation().setDcs("sharb,shaxy").setDistance(1),
                new DcRelation().setDcs("SHA-ALI,SHAXY").setDistance(15),
                new DcRelation().setDcs("SHA-ALI,SHARB").setDistance(15));

        List<ClusterDcRelations> clusterDcRelations = Lists.newArrayList(
                new ClusterDcRelations().setClusterName("Cluster1").setRelations(Lists.newArrayList(
                        new DcRelation().setDcs("SHARB,SHAXY").setDistance(1),
                        new DcRelation().setDcs("sha-ali,shaxy").setDistance(-1),
                        new DcRelation().setDcs("SHA-ALI,SHARB").setDistance(-1))),
                new ClusterDcRelations().setClusterName("Cluster2").setRelations(Lists.newArrayList(
                        new DcRelation().setDcs("sharb,shaxy").setDistance(2),
                        new DcRelation().setDcs("SHA-ALI,SHAXY").setDistance(15),
                        new DcRelation().setDcs("SHA-ALI,SHARB").setDistance(30))));

        return new DcsRelations().setDcLevel(dcRelations).setClusterLevel(clusterDcRelations).setDelayPerDistance(3000);
    }

}
