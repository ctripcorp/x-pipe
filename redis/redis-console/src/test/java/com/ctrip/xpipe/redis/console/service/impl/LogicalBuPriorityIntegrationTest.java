package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.LogicalBuModel;
import com.ctrip.xpipe.redis.console.service.LogicalBuService;
import com.ctrip.xpipe.redis.console.service.logicalbu.KeeperOpRequest;
import com.ctrip.xpipe.redis.console.service.logicalbu.KeeperOpResult;
import com.ctrip.xpipe.redis.console.service.logicalbu.LogicalBuKeeperOpsService;
import com.ctrip.xpipe.redis.console.service.logicalbu.LogicalBuOpResult;
import com.ctrip.xpipe.redis.console.service.logicalbu.OpLayer;
import com.ctrip.xpipe.redis.console.service.logicalbu.OpStage;
import com.ctrip.xpipe.redis.console.service.meta.ClusterMetaService;
import com.ctrip.xpipe.redis.console.service.meta.DcMetaService;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * H2: LOGICAL_BU_TBL.priority CAS queries and the TFS keeper priority override on meta handed to MetaServer.
 * jq/cluster1/shard1 has keepers on KC 1 (made TFS of BU 42 here) and KC 2 (BM).
 */
public class LogicalBuPriorityIntegrationTest extends AbstractConsoleIntegrationTest {

    private static final long BU_ID = 42L;
    private static final long TFS_KC = 1L;
    private static final long BM_KC = 2L;

    @Autowired
    private LogicalBuService logicalBuService;

    @Autowired
    private ClusterMetaService clusterMetaService;

    @Autowired
    private DcMetaService dcMetaService;

    @Autowired
    private LogicalBuKeeperOpsService keeperOpsService;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/apptest.sql")
                + "\nupdate KEEPERCONTAINER_TBL set keepercontainer_disk_type='TFS_1', logical_bu_id=42 where keepercontainer_id=1;\n";
    }

    @Test
    public void testNullPriorityKeepsKeeperPriority() {
        Assert.assertNull(logicalBuService.findById(BU_ID).getPriority());
        Assert.assertFalse(logicalBuService.findAllPriorities().containsKey(BU_ID));

        Map<Long, Integer> priorities = shard1KeeperPriorities(clusterMetaService.getClusterMeta("jq", "cluster1"));
        Assert.assertEquals(Integer.valueOf(1), priorities.get(TFS_KC));
        Assert.assertEquals(Integer.valueOf(1), priorities.get(BM_KC));
    }

    @Test
    public void testCasPriority() {
        Assert.assertFalse("column is NULL, expected=2 must not match", logicalBuService.casPriority(BU_ID, 2, 0));
        Assert.assertTrue(logicalBuService.casPriority(BU_ID, null, 2));
        Assert.assertFalse("column is 2, expected=NULL must not match", logicalBuService.casPriority(BU_ID, null, 0));
        Assert.assertTrue(logicalBuService.casPriority(BU_ID, 2, 0));
        Assert.assertEquals(Integer.valueOf(0), logicalBuService.findById(BU_ID).getPriority());
        Assert.assertEquals(Integer.valueOf(0), logicalBuService.findAllPriorities().get(BU_ID));
        Assert.assertFalse("unknown BU", logicalBuService.casPriority(999L, null, 0));

        // restore: back to NULL, then the TFS keeper uses keeper_priority again
        Assert.assertFalse("column is 0, expected=2 must not match", logicalBuService.casPriority(BU_ID, 2, null));
        Assert.assertTrue(logicalBuService.casPriority(BU_ID, 0, null));
        Assert.assertNull(logicalBuService.findById(BU_ID).getPriority());
        Assert.assertFalse(logicalBuService.findAllPriorities().containsKey(BU_ID));
    }

    @Test
    public void testBuPriorityOverridesOnlyTfsKeepers() throws Exception {
        Assert.assertTrue(logicalBuService.casPriority(BU_ID, null, 0));

        Map<Long, Integer> fromClusterMeta = shard1KeeperPriorities(clusterMetaService.getClusterMeta("jq", "cluster1"));
        Assert.assertEquals(Integer.valueOf(0), fromClusterMeta.get(TFS_KC));
        Assert.assertEquals(Integer.valueOf(1), fromClusterMeta.get(BM_KC));

        DcMeta dcMeta = dcMetaService.getDcMeta("jq");
        Map<Long, Integer> fromDcMeta = shard1KeeperPriorities(dcMeta.findCluster("cluster1"));
        Assert.assertEquals(Integer.valueOf(0), fromDcMeta.get(TFS_KC));
        Assert.assertEquals(Integer.valueOf(1), fromDcMeta.get(BM_KC));
    }

    @Test
    public void testPortalUpdateDoesNotTouchPriority() {
        Assert.assertTrue(logicalBuService.casPriority(BU_ID, null, 0));
        LogicalBuModel model = logicalBuService.findById(BU_ID);
        model.setDescription("edited").setPriority(2);
        logicalBuService.update(BU_ID, model);

        LogicalBuModel after = logicalBuService.findById(BU_ID);
        Assert.assertEquals("edited", after.getDescription());
        Assert.assertEquals(Integer.valueOf(0), after.getPriority());
    }

    @Test
    public void testSwitchBMUpdatesDbAndReportsMetaServerLayer() {
        // KC 1 of jq is TFS of BU 42; no MetaServer runs in this test, so the reload step must fail at META_SERVER
        KeeperOpResult result = keeperOpsService.switchBM(new KeeperOpRequest()
                .setLogicalBuIds(java.util.Collections.singletonList(BU_ID)));

        LogicalBuOpResult bu = result.getResults().get(0);
        Assert.assertTrue(bu.getIdcs().contains("jq"));
        Assert.assertEquals(Integer.valueOf(0), logicalBuService.findById(BU_ID).getPriority());
        Assert.assertEquals(bu.getFailures().toString(), LogicalBuOpResult.State.FAILED, bu.getState());
        Assert.assertTrue(bu.getFailures().toString(), bu.getFailures().stream()
                .allMatch(f -> f.getLayer() == OpLayer.META_SERVER && f.getStage() == OpStage.NOTIFY_META));
    }

    private Map<Long, Integer> shard1KeeperPriorities(ClusterMeta clusterMeta) {
        ShardMeta shard = clusterMeta.getAllShards().get("shard1");
        Map<Long, Integer> result = new HashMap<>();
        for (KeeperMeta keeper : shard.getKeepers()) {
            result.put(keeper.getKeeperContainerId(), keeper.getPriority());
        }
        return result;
    }
}
