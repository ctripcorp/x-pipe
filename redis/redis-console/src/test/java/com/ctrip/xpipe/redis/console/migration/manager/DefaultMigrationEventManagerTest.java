package com.ctrip.xpipe.redis.console.migration.manager;

import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultMigrationEventManagerTest {

    private DefaultMigrationEventManager migrationEventManager = new DefaultMigrationEventManager();

    @Test
    public void testDiffWithNoDiff() {
        MigrationEvent prev = mock(MigrationEvent.class);
        MigrationEvent future = mock(MigrationEvent.class);

        when(prev.isDone()).thenReturn(false);
        when(future.isDone()).thenReturn(false);
        MigrationCluster migrationCluster = mock(MigrationCluster.class);
        when(migrationCluster.clusterName()).thenReturn("cluster1");
        when(migrationCluster.getStatus()).thenReturn(MigrationStatus.Migrating);
        when(prev.getMigrationClusters()).thenReturn(Lists.newArrayList(migrationCluster));
        when(future.getMigrationClusters()).thenReturn(Lists.newArrayList(migrationCluster));
        Assert.assertFalse(migrationEventManager.diff(prev, future));
    }

    @Test
    public void testDiffWithDiff() {
        MigrationEvent prev = mock(MigrationEvent.class);
        MigrationEvent future = mock(MigrationEvent.class);

        when(prev.isDone()).thenReturn(false);
        when(future.isDone()).thenReturn(false);
        MigrationCluster migrationCluster = mock(MigrationCluster.class);
        when(migrationCluster.clusterName()).thenReturn("cluster1");
        when(migrationCluster.getStatus()).thenReturn(MigrationStatus.Migrating);

        MigrationCluster newMigrationCluster = mock(MigrationCluster.class);
        when(newMigrationCluster.clusterName()).thenReturn("cluster1");
        when(newMigrationCluster.getStatus()).thenReturn(MigrationStatus.Success);
        when(prev.getMigrationClusters()).thenReturn(Lists.newArrayList(migrationCluster));
        when(future.getMigrationClusters()).thenReturn(Lists.newArrayList(newMigrationCluster));
        Assert.assertTrue(migrationEventManager.diff(prev, future));
    }
}