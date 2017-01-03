package com.ctrip.xpipe.redis.console.migration.status;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationShard;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.cluster.ClusterStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationMigratingState;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.ShardTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Created by Shyin on 11/12/2016.
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationStatTest extends AbstractConsoleTest {
    private MigrationCluster migrationCluster;

    private MigrationClusterTbl mockedMigrationCluster;
    @Mock
    private DcService mockedDcService;
    @Mock
    private ClusterService mockedClusterService;
    @Mock
    private ShardService mockedShardService;
    @Mock
    private MigrationService mockedMigrationService;
    @Mock
    private RedisService mockedRedisService;

    @Mock
    private MigrationShard mockedMigrationShard;

    @Before
    public void setUp() {
        prepareData();
        migrationCluster = new DefaultMigrationCluster(mockedMigrationCluster, mockedDcService, mockedClusterService,
                mockedShardService, mockedRedisService, mockedMigrationService);
        migrationCluster.addNewMigrationShard(mockedMigrationShard);
    }

    @Test
    public void testInitiatedToChecking() {
        Assert.assertEquals(MigrationStatus.Initiated, migrationCluster.getStatus());

        migrationCluster.process();
        sleep(100);

        verify(mockedMigrationShard, times(1)).doCheck();
        Assert.assertEquals(MigrationStatus.Checking, migrationCluster.getStatus());
    }

    @Test
    public void testCheckingToMigrating() {
    	when(mockedMigrationShard.getCurrentShard()).thenReturn((new ShardTbl()).setShardName("test-shard"));
    	
        Assert.assertEquals(MigrationStatus.Initiated, migrationCluster.getStatus());

        migrationCluster.updateStat(new MigrationMigratingState(migrationCluster));
        Assert.assertEquals(MigrationStatus.Migrating, migrationCluster.getStatus());
        migrationCluster.process();
        sleep(100);

        verify(mockedMigrationShard, times(1)).doMigrate();
    }

    private void prepareData() {
        mockedMigrationCluster = (new MigrationClusterTbl()).setId(1).setEventId(1).setClusterId(1).setDestinationDcId(2)
                .setStatus(MigrationStatus.Initiated.toString());
        when(mockedClusterService.find(1)).thenReturn((new ClusterTbl()).setId(1).setClusterName("test-cluster")
                .setActivedcId(1).setStatus(ClusterStatus.Lock.toString()));
        List<ShardTbl> shards = new LinkedList<>();
        shards.add((new ShardTbl()).setId(1).setClusterId(1).setShardName("test-shard"));
        when(mockedShardService.findAllByClusterName("test-cluster")).thenReturn(shards);
        List<DcTbl> dcs = new LinkedList<>();
        dcs.add((new DcTbl()).setId(1).setDcName("A"));
        dcs.add((new DcTbl()).setId(2).setDcName("B"));
        when(mockedDcService.findClusterRelatedDc("test-cluster")).thenReturn(dcs);
    }

}
