package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.console.annotation.DalTransaction;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.model.MigrationLock;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author lishanglin
 * date 2020/12/14
 */
public class DefaultMigrationLock implements MigrationLock {

    private final long eventId;

    private final long lockTimeout;

    private MigrationEventDao migrationEventDao;

    private long lastLockUntil;

    private final String currentIdc;

    private static Logger logger = LoggerFactory.getLogger(DefaultMigrationLock.class);

    public DefaultMigrationLock(long eventId, long lockTimeout, MigrationEventDao migrationEventDao) {
        this(eventId, lockTimeout, migrationEventDao, FoundationService.DEFAULT.getDataCenter());
    }

    @VisibleForTesting
    public DefaultMigrationLock(long eventId, long lockTimeout, MigrationEventDao migrationEventDao, String currentIdc) {
        this.eventId = eventId;
        this.lockTimeout = lockTimeout;
        this.migrationEventDao = migrationEventDao;
        this.lastLockUntil = 0;
        this.currentIdc = currentIdc;
    }

    @Override
    @DalTransaction
    public synchronized void updateLock() {
        long lockUntil = System.currentTimeMillis() + lockTimeout;
        migrationEventDao.updateMigrationEventLock(eventId, currentIdc, lockUntil);
        lastLockUntil = lockUntil;
    }

    @Override
    public void releaseLock() {
        try {
            migrationEventDao.releaseMigrationEventLock(eventId, currentIdc, lastLockUntil);
        } catch (Throwable th) {
            logger.info("[releaseLock][{}] release lock fail", eventId, th);
        } finally {
            lastLockUntil = 0;
        }
    }

}
