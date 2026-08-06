package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.model.ClusterRelations;
import com.ctrip.xpipe.redis.checker.model.DcsPriority;
import com.ctrip.xpipe.redis.checker.model.Relation;
import com.ctrip.xpipe.redis.checker.model.Relations;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.tuple.Pair;
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
public class RelationsServiceTest {

    @InjectMocks
    @Spy
    private final DefaultRelationsService relationsService=new DefaultRelationsService();

    @Mock
    private ConsoleConfig config;

    @Mock
    private MetaCache metaCache;

    private String configStr = "{\n" +
            "    \"delayPerDistance\":3000,"+
            "    \"dcLevel\":[\n" +
            "        {\n" +
            "            \"src\":\"sharb\",\n" +
            "            \"dst\":\"shaxy\",\n" +
            "            \"distance\":1\n" +
            "        },\n" +
            "        {\n" +
            "            \"src\":\"SHARB\",\n" +
            "            \"dst\":\"SHA-ALI\",\n" +
            "            \"distance\":15\n" +
            "        },\n" +
            "        {\n" +
            "            \"src\":\"SHAXY\",\n" +
            "            \"dst\":\"SHA-ALI\",\n" +
            "            \"distance\":15\n" +
            "        }\n" +
            "    ],\n" +
            "    \"clusterLevel\":[\n" +
            "        {\n" +
            "            \"clusterName\":\"Cluster1\", \n" +
            "            \"relations\":[\n" +
            "                {\n" +
            "                    \"src\":\"SHARB\",\n" +
            "                    \"dst\":\"SHAXY\",\n" +
            "                    \"distance\":1\n" +
            "                },\n" +
            "                {\n" +
            "                    \"src\":\"sha-ali\",\n" +
            "                    \"dst\":\"shaxy\",\n" +
            "                    \"distance\":-1\n" +
            "                },\n" +
            "                {\n" +
            "                    \"src\":\"SHA-ALI\",\n" +
            "                    \"dst\":\"SHARB\",\n" +
            "                    \"distance\":-1\n" +
            "                }\n" +
            "            ]\n" +
            "        },\n" +
            "        {\n" +
            "            \"clusterName\":\"Cluster2\",   \n" +
            "            \"relations\":[\n" +
            "                {\n" +
            "                    \"src\":\"sharb\",\n" +
            "                    \"dst\":\"shaxy\",\n" +
            "                    \"distance\":2\n" +
            "                },\n" +
            "                {\n" +
            "                    \"src\":\"SHA-ALI\",\n" +
            "                    \"dst\":\"SHAXY\",\n" +
            "                    \"distance\":15\n" +
            "                },\n" +
            "                {\n" +
            "                    \"src\":\"SHA-ALI\",\n" +
            "                    \"dst\":\"SHARB\",\n" +
            "                    \"distance\":30\n" +
            "                }\n" +
            "            ]\n" +
            "        }\n" +
            "    ]\n" +
            "}";

    private String regionConfigStr = "{\n" +
            "    \"delayPerDistance\":3000,"+
            "    \"regionLevel\":[\n" +
            "        {\"src\":\"SHA\",\"dst\":\"XREG\",\"distance\":1},\n" +
            "        {\"src\":\"SHA\",\"dst\":\"FRA\",\"distance\":-1},\n" +
            "        {\"src\":\"SHA\",\"dst\":\"JP\",\"distance\":0,\"biDirection\":false}\n" +
            "    ]\n" +
            "}";

    @Test
    public void jsonTest() {
        Relations relations = JsonCodec.INSTANCE.decode(configStr, Relations.class);
        List<Relation> dcLevel = relations.getDcLevel();
        List<ClusterRelations> clusterLevel = relations.getClusterLevel();
        Assert.assertEquals(3, dcLevel.size());
        Assert.assertEquals(2, clusterLevel.size());
        Assert.assertEquals("sharb", dcLevel.get(0).getSrc());
        Assert.assertEquals("shaxy", dcLevel.get(0).getDst());
        Assert.assertTrue(dcLevel.get(0).isBiDirection());
    }

    @Test
    public void regionJsonTest() {
        Relations relations = JsonCodec.INSTANCE.decode(regionConfigStr, Relations.class);
        List<Relation> regionLevel = relations.getRegionLevel();
        Assert.assertEquals(3, regionLevel.size());
        Assert.assertTrue(regionLevel.get(0).isBiDirection());
        Assert.assertFalse(regionLevel.get(2).isBiDirection());
    }

    @Test
    public void refreshTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(configStr);
        relationsService.refresh();

        Map<String, DcsPriority> clusterLevelDcPriority = relationsService.getClusterLevelDcPriority();
        DcsPriority dcsPriority = relationsService.getDcLevelPriority();
        Map<Pair<String, String>, Integer> dcDistances = relationsService.getDcsDistance();
        Map<String, Map<Pair<String, String>, Integer>> clusterDcsDistance = relationsService.getClusterDcsDistance();

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
        Assert.assertEquals(3000, relationsService.getDelayPerDistance().intValue());

        //check dcs distance (biDirection 默认 true，双向均有值)
        Assert.assertEquals(15, dcDistances.get(new Pair<>("SHA-ALI", "SHARB")).intValue());
        Assert.assertEquals(15, dcDistances.get(new Pair<>("SHARB", "SHA-ALI")).intValue());
        Assert.assertEquals(15, dcDistances.get(new Pair<>("SHA-ALI", "SHAXY")).intValue());
        Assert.assertEquals(1, dcDistances.get(new Pair<>("SHAXY", "SHARB")).intValue());
        Assert.assertNull(dcDistances.get(new Pair<>("SHAXY", "SHAFQ")));

        //check cluster dcs distance
        Map<Pair<String, String>, Integer> cluster1 = clusterDcsDistance.get("cluster1");
        Assert.assertEquals(-1, cluster1.get(new Pair<>("SHA-ALI", "SHARB")).intValue());
        Assert.assertEquals(-1, cluster1.get(new Pair<>("SHA-ALI", "SHAXY")).intValue());
        Assert.assertEquals(1, cluster1.get(new Pair<>("SHAXY", "SHARB")).intValue());

        Map<Pair<String, String>, Integer> cluster2 = clusterDcsDistance.get("cluster2");
        Assert.assertEquals(30, cluster2.get(new Pair<>("SHA-ALI", "SHARB")).intValue());
        Assert.assertEquals(15, cluster2.get(new Pair<>("SHA-ALI", "SHAXY")).intValue());
        Assert.assertEquals(2, cluster2.get(new Pair<>("SHAXY", "SHARB")).intValue());

        Assert.assertNull(clusterDcsDistance.get("cluster3"));
    }

    @Test
    public void getClusterTargetDcByPriorityTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(configStr);
        relationsService.refresh();

        for (int i = 0; i < 1000; i++) {
            Assert.assertEquals("SHARB", relationsService.getClusterTargetDcByPriority(234, "clustEr3", "sha-ALi", Lists.newArrayList("shaRB", "Shaxy")));
        }

        Mockito.verify(relationsService, Mockito.times(1)).getTargetDcs(Mockito.any(), Mockito.any());

        Assert.assertNull(relationsService.getClusterTargetDcByPriority(234, "clustEr3", "sha-ALi", null));
        Assert.assertNull(relationsService.getClusterTargetDcByPriority(234, "clustEr3", "sha-ALi", Lists.newArrayList()));
        Assert.assertNull(relationsService.getClusterTargetDcByPriority(234, "clustEr3", "sha-ALi", Lists.newArrayList("SHAFQ")));
    }

    @Test
    public void getClusterLevelTargetDcsTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(configStr);
        relationsService.refresh();

        List<String> targetDcs = relationsService.getTargetDcsByPriority("cluSter2", "sha-ali", Lists.newArrayList("sharb", "shaxy"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertEquals("SHAXY", targetDcs.get(0));

        targetDcs = relationsService.getTargetDcsByPriority("clusTer2", "sharb", Lists.newArrayList("sha-ali", "SHAXY"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertEquals("SHAXY", targetDcs.get(0));

        targetDcs = relationsService.getTargetDcsByPriority("clusTer1", "shaxy", Lists.newArrayList("SHA-ALI", "sharb"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHARB"));

        targetDcs = relationsService.getTargetDcsByPriority("clusTer1", "sha-ali", Lists.newArrayList("shaxy", "SHARB"));
        Assert.assertEquals(0, targetDcs.size());

        targetDcs = relationsService.getTargetDcsByPriority("clUster1", "CFTRB", Lists.newArrayList("CFTXY"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("CFTXY"));
    }

    @Test
    public void getDcLevelTargetDcsTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(configStr);
        relationsService.refresh();

        List<String> targetDcs = relationsService.getTargetDcsByPriority("cluster3", "SHA-ALI", Lists.newArrayList("SHARB", "SHAXY"));
        Assert.assertEquals(2, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHARB"));
        Assert.assertTrue(targetDcs.contains("SHAXY"));

        targetDcs = relationsService.getTargetDcsByPriority("cluster3", "SHARB", Lists.newArrayList("SHA-ALI", "SHAXY"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHAXY"));

        targetDcs = relationsService.getTargetDcsByPriority("cluster3", "SHAXY", Lists.newArrayList("SHA-ALI", "SHARB"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHARB"));

        targetDcs = relationsService.getTargetDcsByPriority("cluster3", "SHAFQ", Lists.newArrayList("SHAXY", "SHARB"));
        Assert.assertEquals(2, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("SHARB"));
        Assert.assertTrue(targetDcs.contains("SHAXY"));

        targetDcs = relationsService.getTargetDcsByPriority("cluster3", "CFTRB", Lists.newArrayList("CFTXY"));
        Assert.assertEquals(1, targetDcs.size());
        Assert.assertTrue(targetDcs.contains("CFTXY"));
    }

    @Test
    public void getClusterDcsDelayTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(configStr);
        relationsService.refresh();

        Assert.assertEquals(-3000, relationsService.getClusterDcsDelay("clUster1", "SHA-ALI", "sharb").intValue());
        Assert.assertEquals(-3000, relationsService.getClusterDcsDelay("clUster1", "SHA-Ali", "shaxy").intValue());
        Assert.assertEquals(3000, relationsService.getClusterDcsDelay("clusTer1", "SHAxy", "sharb").intValue());
        Assert.assertNull(relationsService.getClusterDcsDelay("clusTer1", "shaxy", "shafq"));

        Assert.assertNull(relationsService.getClusterDcsDelay("clusTer3", "SHAxy", "sharb"));

        Assert.assertEquals(90000, relationsService.getClusterDcsDelay("clUster2", "SHA-ALI", "sharb").intValue());
        Assert.assertEquals(45000, relationsService.getClusterDcsDelay("clUster2", "SHA-Ali", "shaxy").intValue());
        Assert.assertEquals(6000, relationsService.getClusterDcsDelay("clusTer2", "SHAxy", "sharb").intValue());
    }

    @Test
    public void getDcsDelayTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(configStr);
        relationsService.refresh();

        Assert.assertEquals(45000, relationsService.getDcsDelay("SHA-ALI", "sharb").intValue());
        Assert.assertEquals(45000, relationsService.getDcsDelay("SHA-Ali", "shaxy").intValue());
        Assert.assertEquals(3000, relationsService.getDcsDelay("SHAxy", "sharb").intValue());
        Assert.assertNull(relationsService.getDcsDelay("shaxy", "shafq"));
    }

    @Test
    public void getExcludeDcsForBiClusterTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(configStr);

        //not initialized
        Set<String> excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB"), Sets.newHashSet("shaXy", "shA-ali"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        relationsService.refresh();

        //empty available dcs
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB"), Sets.newHashSet());
        Assert.assertEquals(Sets.newHashSet(), excludedDcs);

        //not existed cluster, use dc priority
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster3", Sets.newHashSet("sharB"), Sets.newHashSet("shaXy", "shA-ali"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        //sharb down, ignore ali
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB"), Sets.newHashSet("shaXy", "shA-ali"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        //shaxy down, ignore ali
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("shaXy"), Sets.newHashSet("shaRB", "shA-ali"));
        Assert.assertEquals(Sets.newHashSet("SHAXY"), excludedDcs);

        //ali down, no available dc
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sha-ALI"), Sets.newHashSet("shaRB", "shaXY"));
        Assert.assertEquals(Sets.newHashSet(), excludedDcs);

        //shaxy and sharb down
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharb","shaxy"), Sets.newHashSet("sha-ali"));
        Assert.assertEquals(Sets.newHashSet(), excludedDcs);

        //multi target dcs
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB"), Sets.newHashSet("sha-ali", "shaXY","shafq"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        // shaali and sharb down
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster3", Sets.newHashSet("sharB", "shA-ali"), Sets.newHashSet("shaXy"));
        Assert.assertEquals(Sets.newHashSet("SHARB", "SHA-ALI"), excludedDcs);

        // shaali and sharb down, no downgrade for shaali
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster1", Sets.newHashSet("sharB", "shA-ali"), Sets.newHashSet("shaXy"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

        //no ignore dcs
        excludedDcs = relationsService.getExcludedDcsForBiCluster("clUster2", Sets.newHashSet("sharB"), Sets.newHashSet("sha-ali", "shaXY"));
        Assert.assertEquals(Sets.newHashSet("SHARB"), excludedDcs);

    }

    @Test
    public void isReachableRegionTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(regionConfigStr);
        Mockito.when(metaCache.getDcZone("SHA_dc")).thenReturn("SHA");
        Mockito.when(metaCache.getDcZone("XREG_dc")).thenReturn("XREG");
        Mockito.when(metaCache.getDcZone("FRA_dc")).thenReturn("FRA");
        Mockito.when(metaCache.getDcZone("JP_dc")).thenReturn("JP");
        relationsService.refresh();

        // biDirection=true 正向 distance=1
        Assert.assertTrue(relationsService.isReachableRegion("SHA_dc", "XREG_dc"));
        // biDirection=true 反向，大小写不敏感
        Assert.assertTrue(relationsService.isReachableRegion("XREG_dc", "SHA_dc"));
        // distance=-1 隔离
        Assert.assertFalse(relationsService.isReachableRegion("SHA_dc", "FRA_dc"));
        Assert.assertFalse(relationsService.isReachableRegion("FRA_dc", "SHA_dc"));
        // distance=0, != -1 可达
        Assert.assertTrue(relationsService.isReachableRegion("SHA_dc", "JP_dc"));
        // biDirection=false 反向未存
        Assert.assertFalse(relationsService.isReachableRegion("JP_dc", "SHA_dc"));
        // 无配置
        Assert.assertFalse(relationsService.isReachableRegion("SHA_dc", "UNKNOWN_dc"));
        // 同 region
        Assert.assertTrue(relationsService.isReachableRegion("SHA_dc", "SHA_dc"));
    }

    @Test
    public void getRegionDelayTest() throws Exception {
        Mockito.when(config.getRelations()).thenReturn(regionConfigStr);
        Mockito.when(metaCache.getDcZone("SHA_dc")).thenReturn("SHA");
        Mockito.when(metaCache.getDcZone("XREG_dc")).thenReturn("XREG");
        Mockito.when(metaCache.getDcZone("FRA_dc")).thenReturn("FRA");
        Mockito.when(metaCache.getDcZone("JP_dc")).thenReturn("JP");
        relationsService.refresh();

        // distance=1, delayPerDistance=3000
        Assert.assertEquals(3000, relationsService.getRegionDelay("SHA_dc", "XREG_dc").intValue());
        // biDirection 反向
        Assert.assertEquals(3000, relationsService.getRegionDelay("XREG_dc", "SHA_dc").intValue());
        // distance=-1 隔离
        Assert.assertNull(relationsService.getRegionDelay("SHA_dc", "FRA_dc"));
        // distance=0，用默认延迟（非 0ms 阈值）
        Assert.assertNull(relationsService.getRegionDelay("SHA_dc", "JP_dc"));
        // biDirection=false 反向未存
        Assert.assertNull(relationsService.getRegionDelay("JP_dc", "SHA_dc"));
        // 无配置
        Assert.assertNull(relationsService.getRegionDelay("SHA_dc", "UNKNOWN_dc"));
        // 同 region
        Assert.assertNull(relationsService.getRegionDelay("SHA_dc", "SHA_dc"));
    }

    private Relations buildRelations() {
        List<Relation> dcLevel = Lists.newArrayList(
                new Relation().setSrc("sharb").setDst("shaxy").setDistance(1),
                new Relation().setSrc("SHA-ALI").setDst("SHAXY").setDistance(15),
                new Relation().setSrc("SHA-ALI").setDst("SHARB").setDistance(15));

        List<ClusterRelations> clusterLevel = Lists.newArrayList(
                new ClusterRelations().setClusterName("Cluster1").setRelations(Lists.newArrayList(
                        new Relation().setSrc("SHARB").setDst("SHAXY").setDistance(1),
                        new Relation().setSrc("sha-ali").setDst("shaxy").setDistance(-1),
                        new Relation().setSrc("SHA-ALI").setDst("SHARB").setDistance(-1))),
                new ClusterRelations().setClusterName("Cluster2").setRelations(Lists.newArrayList(
                        new Relation().setSrc("sharb").setDst("shaxy").setDistance(2),
                        new Relation().setSrc("SHA-ALI").setDst("SHAXY").setDistance(15),
                        new Relation().setSrc("SHA-ALI").setDst("SHARB").setDistance(30))));

        return new Relations().setDcLevel(dcLevel).setClusterLevel(clusterLevel).setDelayPerDistance(3000);
    }

}
