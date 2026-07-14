package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.dao.ShardDao;
import com.ctrip.xpipe.redis.console.model.DcClusterShardTbl;
import com.ctrip.xpipe.redis.console.model.DcClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.sentinel.SentinelBalanceService;
import com.ctrip.xpipe.redis.console.service.DcClusterService;
import com.ctrip.xpipe.redis.console.service.DcClusterShardService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.unidal.dal.jdbc.DalException;

import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Jan 02, 2018
 */
public class DcClusterShardServiceImplTest extends AbstractServiceImplTest {

    @Autowired
    private DcClusterShardService service;

    @Autowired
    private ShardService shardService;

    @Autowired
    private ShardDao shardDao;

    @Autowired
    private DcClusterService dcClusterService;

    @Autowired
    private SentinelBalanceService sentinelBalanceService;

    @Test
    public void testUpdateDcClusterShard() throws Exception {
        DcClusterShardTbl proto = service.find(dcNames[0], clusterName, shardNames[0]);
        long sentinelId = proto.getSetinelId();
        long expected = sentinelId + 1;
        proto.setSetinelId(expected);
        service.updateDcClusterShard(proto);
        Assert.assertEquals(expected, service.find(dcNames[0], clusterName, shardNames[0]).getSetinelId());
    }

    @Test
    public void testCreateShardSetsDefaultOperatingUntil() {
        String newShardName = "shard_operating_until_create";
        shardService.createShard(clusterName,
                new ShardTbl().setShardName(newShardName).setSetinelMonitorName(newShardName),
                sentinelBalanceService.selectMultiDcSentinels(ClusterType.ONE_WAY, ""));

        for (String dcName : dcNames) {
            DcClusterShardTbl dcClusterShard = service.find(dcName, clusterName, newShardName);
            Assert.assertNotNull(dcClusterShard);
            Assert.assertEquals(DateTimeUtils.DEFAULT_OPERATING_UNTIL, dcClusterShard.getOperatingUntil());
        }
    }

    @Test
    public void testInsertBatchSetsDefaultOperatingUntilWhenNull() {
        String shardName = "shard_operating_until_insert";
        shardDao.createShard(clusterName, new ShardTbl().setShardName(shardName).setSetinelMonitorName(shardName));

        DcClusterTbl dcCluster = dcClusterService.find(dcNames[0], clusterName);
        ShardTbl shard = shardService.find(clusterName, shardName);
        DcClusterShardTbl proto = new DcClusterShardTbl()
                .setDcClusterId(dcCluster.getDcClusterId())
                .setShardId(shard.getId());
        service.insertBatch(Collections.singletonList(proto));

        DcClusterShardTbl inserted = service.find(dcNames[0], clusterName, shardName);
        Assert.assertNotNull(inserted);
        Assert.assertEquals(DateTimeUtils.DEFAULT_OPERATING_UNTIL, inserted.getOperatingUntil());
    }

    @Test
    public void testBatchUpdateOperatingUntil() throws DalException {
        Date until = new Date(System.currentTimeMillis() + 3600_000L);
        List<DcClusterShardTbl> matched = service.findDcClusterShardsByNames(dcNames[0], clusterName,
                Collections.singletonList(shardNames[0]));
        Assert.assertEquals(1, matched.size());
        int affected = service.updateOperatingUntilByIds(
                Collections.singletonList(matched.get(0).getDcClusterShardId()), until);
        Assert.assertEquals(1, affected);

        DcClusterShardTbl updated = service.find(dcNames[0], clusterName, shardNames[0]);
        Assert.assertEquals(until.getTime(), updated.getOperatingUntil().getTime());
    }

    @Test
    public void testInsertBatchKeepsExplicitOperatingUntil() {
        String shardName = "shard_operating_until_explicit";
        shardDao.createShard(clusterName, new ShardTbl().setShardName(shardName).setSetinelMonitorName(shardName));

        DcClusterTbl dcCluster = dcClusterService.find(dcNames[1], clusterName);
        ShardTbl shard = shardService.find(clusterName, shardName);
        Date customUntil = new Date(System.currentTimeMillis() + 3600_000L);
        DcClusterShardTbl proto = new DcClusterShardTbl()
                .setDcClusterId(dcCluster.getDcClusterId())
                .setShardId(shard.getId())
                .setOperatingUntil(customUntil);
        service.insertBatch(Collections.singletonList(proto));

        DcClusterShardTbl inserted = service.find(dcNames[1], clusterName, shardName);
        Assert.assertNotNull(inserted);
        Assert.assertEquals(customUntil.getTime(), inserted.getOperatingUntil().getTime());
    }
}
