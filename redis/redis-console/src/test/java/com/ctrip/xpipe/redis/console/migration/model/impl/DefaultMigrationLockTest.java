package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationEventTblEntity;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.unidal.lookup.ContainerLoader;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2020/12/18
 */
public class DefaultMigrationLockTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationEventDao migrationEventDao;

    private DefaultMigrationLock currentDcLock1;
    private DefaultMigrationLock currentDcLock2;

    private DefaultMigrationLock otherDcLock;

    private MigrationEventTblDao migrationEventTblDao;

    private long lockTimeout = 1000;
    private String currentDc = "jq";
    private String otherDc = "oy";

    @Before
    public void setupDefaultMigrationLockTest() throws Exception {
        currentDcLock1 = new DefaultMigrationLock(1, lockTimeout, migrationEventDao, currentDc);
        currentDcLock2 = new DefaultMigrationLock(1, lockTimeout, migrationEventDao, currentDc);
        otherDcLock = new DefaultMigrationLock(1, lockTimeout, migrationEventDao, otherDc);
        migrationEventTblDao = ContainerLoader.getDefaultContainer().lookup(MigrationEventTblDao.class);
    }

    @Test
    @DirtiesContext
    public void testOnlyOneSideLockSuccess() {
        currentDcLock1.updateLock();
        try {
            otherDcLock.updateLock();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof DalUpdateException);
        }
    }

    @Test
    @DirtiesContext
    public void testLockReentrant() {
        currentDcLock1.updateLock();
        currentDcLock2.updateLock();
    }

    @Test
    @DirtiesContext
    public void testOnlyUnlockSelfLock() throws Exception {
        currentDcLock1.updateLock();
        currentDcLock2.updateLock();

        currentDcLock1.releaseLock();
        MigrationEventTbl migrationEventTbl = getEvent();
        Assert.assertEquals(currentDc, migrationEventTbl.getExecLock());

        currentDcLock2.releaseLock();
        migrationEventTbl = getEvent();
        Assert.assertEquals("", migrationEventTbl.getExecLock());
        Assert.assertEquals(0, migrationEventTbl.getLockUntil());
    }

    @Test
    @DirtiesContext
    public void testLockTimeout() throws Exception {
        currentDcLock1.updateLock();
        sleep((int) lockTimeout + 10);
        otherDcLock.updateLock();

        MigrationEventTbl migrationEventTbl = getEvent();
        Assert.assertEquals(otherDc, migrationEventTbl.getExecLock());
        Assert.assertTrue(migrationEventTbl.getLockUntil() > 0);
    }

    private MigrationEventTbl getEvent() throws Exception {
        return migrationEventTblDao.findByPK(1, MigrationEventTblEntity.READSET_FULL);
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-lock-test.sql");
    }

}
