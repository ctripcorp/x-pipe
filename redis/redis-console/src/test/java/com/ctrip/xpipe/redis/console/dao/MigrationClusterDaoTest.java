package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 02, 2017
 */
public class MigrationClusterDaoTest extends AbstractConsoleIntegrationTest{

    @Autowired
    private MigrationClusterDao migrationClusterDao;

    private long clusterId = 100;

    @Test
    public void testUpdatePublishInfoById(){

        long id = randomInsert();

        String random = randomString();
        migrationClusterDao.updatePublishInfoById(id, random);

        MigrationClusterTbl byId = migrationClusterDao.getById(id);

        Assert.assertEquals(random, byId.getPublishInfo());
    }

    @Test
    public void testUpdateStatusAndEndTimeById(){

        long id = randomInsert();
        MigrationClusterTbl before = migrationClusterDao.getById(id);

        sleep(5);

        Date endTime = new Date();
        migrationClusterDao.updateStatusAndEndTimeById(id, MigrationStatus.Checking, endTime);

        MigrationClusterTbl current = migrationClusterDao.getById(id);
        Assert.assertNotEquals(before.getEndTime(), current.getEndTime());
        Assert.assertEquals(MigrationStatus.Checking.toString(), current.getStatus());
    }


    @Test
    public void testUpdateStartTime(){

        long id = randomInsert();

        MigrationClusterTbl before = migrationClusterDao.getById(id);
        logger.debug("[before]{}", before);

        sleep(5);

        Date startTime = new Date();
        logger.debug("{}", startTime);
        migrationClusterDao.updateStartTime(id, startTime);
        MigrationClusterTbl current = migrationClusterDao.getById(id);
        logger.debug("{}", current);
        Assert.assertNotEquals(before.getStartTime(), current.getStartTime());
        Assert.assertEquals(before.getStatus(), current.getStatus());
    }

    @Test
    public void testFindUnfinishedByClusterId() throws SQLException, IOException {

        int count = 0;

        for(MigrationStatus migrationStatus : MigrationStatus.values()){

            if(!migrationStatus.isTerminated()){
                count++;
            }
            MigrationClusterTbl tbl = createMigrationClusterTbl(migrationStatus);
            tbl.setClusterId(clusterId);
            migrationClusterDao.insert(tbl);
        }

        List<MigrationClusterTbl> unfinished = migrationClusterDao.findUnfinishedByClusterId(clusterId);
        logger.debug("{}", unfinished);
        Assert.assertEquals(count, unfinished.size());

        long previousId = Long.MIN_VALUE;
        for(MigrationClusterTbl tbl : unfinished){
            long currentId = tbl.getId();
            Assert.assertTrue(currentId > previousId);
            previousId = currentId;
        }
    }

    private MigrationClusterTbl createMigrationClusterTbl(MigrationStatus migrationStatus) {

        MigrationClusterTbl migrationClusterTbl = new MigrationClusterTbl();
        long clusterId = randomInt(100, 10000);
        long eventId = randomInt(100, 100000);
        migrationClusterTbl.setClusterId(clusterId)
                .setMigrationEventId(eventId)
                .setStatus(migrationStatus.toString())
                .setPublishInfo("");

        return migrationClusterTbl;
    }

    private long randomInsert() {

        MigrationStatus migrationStatus = MigrationStatus.Publish;
        MigrationClusterTbl tbl = createMigrationClusterTbl(migrationStatus);
        tbl.setClusterId(clusterId);
        migrationClusterDao.insert(tbl);
        return tbl.getId();
    }
}
