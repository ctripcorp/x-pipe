package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 02, 2017
 */
public class MigrationClusterDaoTest extends AbstractConsoleIntegrationTest{

    @Autowired
    private MigrationClusterDao migrationClusterDao;


    @Test
    public void testFindUnfinishedByClusterId() throws SQLException, IOException {

        long clusterId = 100;
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
        long clusterId = randomInt();
        long eventId = randomInt();
        migrationClusterTbl.setClusterId(clusterId)
                .setMigrationEventId(eventId)
                .setStatus(migrationStatus.toString())
                .setPublishInfo("");

        return migrationClusterTbl;
    }

}
