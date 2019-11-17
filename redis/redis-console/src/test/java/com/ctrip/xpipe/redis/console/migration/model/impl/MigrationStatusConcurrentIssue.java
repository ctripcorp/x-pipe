package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.migration.status.migration.MigrationPartialSuccessState;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.RedisService;
import com.ctrip.xpipe.redis.console.service.ShardService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MigrationStatusConcurrentIssue extends AbstractTest {

    @Mock
    private MigrationEvent event;

    @Mock
    private MigrationClusterTbl migrationCluster;

    @Mock
    private DcService dcService;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ShardService shardService;

    @Mock
    private RedisService redisService;

    @Mock
    private MigrationService migrationService;

    @Before
    public void beforeMigrationStatusConcurrentIssue() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testMigrationStateMachineJumpOver() {
        DefaultMigrationCluster cluster = new DefaultMigrationCluster(executors, scheduled,
                event, migrationCluster, dcService, clusterService, shardService, redisService, migrationService) {
            @Override
            protected void setStatus() {
            }

            @Override
            protected void loadMetaInfo() {
            }
        };

        cluster.setCurrentCluster(new ClusterTbl().setClusterName("test-cluster"));

        cluster.setCurrentState(new MigrationPartialSuccessState(cluster));

        cluster.update(
                new DefaultMigrationShard.ShardObserverEvent("test-cluster-shard", ShardMigrationStep.MIGRATE_NEW_PRIMARY_DC),
                null);

        sleep(3000);
    }
}
