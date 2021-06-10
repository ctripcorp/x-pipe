package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock;

import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.status.MigrationState;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/31
 */
public class MockMigrationEventDao extends MigrationEventDao {

    private MigrationEventDao delegate;

    public MockMigrationEventDao(MigrationEventDao migrationEventDao) {
        this.delegate = migrationEventDao;
    }

    private MigrationState mockMigrationState(MigrationStatus status, MigrationCluster cluster) {
        switch (status) {
            case Initiated:
                return new MockMigrationInitiatedState(cluster);
            case Checking:
                return new MockMigrationCheckingState(cluster);
            case Migrating:
                return new MockMigrationMigratingState(cluster);
            case Publish:
                return new MockMigrationPublishState(cluster);
            default:
                return null;
        }
    }

    private void replaceMigrationState(MigrationEvent event) {
        event.getMigrationClusters().forEach(migrationCluster -> {
            MigrationStatus status = migrationCluster.getStatus();
            MigrationState state = mockMigrationState(status, migrationCluster);
            if (null != state) {
                ((DefaultMigrationCluster) migrationCluster).setMigrationState(state);
            }
        });
    }

    @Override
    public MigrationEvent buildMigrationEvent(final long eventId) {
        MigrationEvent event = delegate.buildMigrationEvent(eventId);
        replaceMigrationState(event);
        return event;
    }

    @Override
    public MigrationEvent createMigrationEvent(MigrationRequest migrationRequest) {
        MigrationEvent event = delegate.createMigrationEvent(migrationRequest);
        replaceMigrationState(event);
        return event;
    }

    @Override
    public List<MigrationClusterModel> getMigrationCluster(long eventId) {
        return delegate.getMigrationCluster(eventId);
    }

    @Override
    public List<Long> findAllUnfinished() {
        return delegate.findAllUnfinished();
    }

}
