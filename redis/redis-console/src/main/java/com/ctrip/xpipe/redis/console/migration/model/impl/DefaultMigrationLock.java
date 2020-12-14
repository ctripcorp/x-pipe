package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.retry.RetryTemplate;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.job.retry.RetryCondition;
import com.ctrip.xpipe.redis.console.job.retry.RetryNTimesOnCondition;
import com.ctrip.xpipe.redis.console.migration.model.MigrationLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

/**
 * @author lishanglin
 * date 2020/12/14
 */
public class DefaultMigrationLock implements MigrationLock {

    private final long eventId;

    private final long lockTimeout;

    private MigrationEventDao migrationEventDao;

    private long lastLockUntil;

    private RetryTemplate<Boolean> timeoutRetryTemplate;

    private static final String currentIdc = FoundationService.DEFAULT.getDataCenter();

    private static Logger logger = LoggerFactory.getLogger(DefaultMigrationLock.class);

    public DefaultMigrationLock(long eventId, long lockTimeout, MigrationEventDao migrationEventDao) {
        this.eventId = eventId;
        this.lockTimeout = lockTimeout;
        this.migrationEventDao = migrationEventDao;
        this.lastLockUntil = 0;

        this.timeoutRetryTemplate = new RetryNTimesOnCondition<>(new RetryCondition.DefaultRetryCondition<Boolean>() {
            @Override
            public boolean isExceptionExpected(Throwable th) {
                return th instanceof TimeoutException;
            }
        }, 3);
    }

    @Override
    public synchronized boolean updateLock() {
        try {
            retryOnTimeout(new AbstractCommand<Boolean>() {
                @Override
                protected void doExecute() throws Exception {
                    try {
                        long lockUntil = System.currentTimeMillis() + lockTimeout;
                        migrationEventDao.updateMigrationEventLock(eventId, currentIdc, lockUntil);
                        lastLockUntil = lockUntil;
                        future().setSuccess(true);
                    } catch (Exception e) {
                        future().setFailure(e.getCause());
                    }
                }

                @Override
                protected void doReset() {
                    // do nothing
                }

                @Override
                public String getName() {
                    return "[updateLock]" + eventId;
                }
            });
        } catch (Throwable th) {
            logger.info("[updateLock][{}] lock fail", eventId, th);
            return false;
        }


        return true;
    }

    @Override
    public void releaseLock() {
        try {
            retryOnTimeout(new AbstractCommand<Boolean>() {
                @Override
                protected void doExecute() throws Throwable {
                    try {
                        migrationEventDao.releaseMigrationEventLock(eventId, currentIdc, lastLockUntil);
                        lastLockUntil = 0;
                        future().setSuccess(true);
                    } catch (Exception e) {
                        future().setFailure(e.getCause());
                    }
                }

                @Override
                protected void doReset() {
                    // do nothing
                }

                @Override
                public String getName() {
                    return "[releaseLock]" + eventId;
                }
            });
        } catch (Throwable th) {
            logger.info("[releaseLock][{}] release lock fail", eventId, th);
        }
    }

    private void retryOnTimeout(Command<Boolean> command) throws Exception {
        timeoutRetryTemplate.execute(command);
    }

}
