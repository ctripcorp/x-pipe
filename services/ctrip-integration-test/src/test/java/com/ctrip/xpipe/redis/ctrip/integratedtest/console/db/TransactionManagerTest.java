package com.ctrip.xpipe.redis.ctrip.integratedtest.console.db;

import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.status.ClusterStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.AbstractCtripConsoleIntegrationTest;
import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author lishanglin
 * date 2021/4/20
 */
public class TransactionManagerTest extends AbstractCtripConsoleIntegrationTest {

    @Autowired
    private MigrationEventDao migrationEventDao;

    @Autowired
    private ClusterService clusterService;

    @Test
    public void testTransactionRollback() {
        MigrationRequest request = new MigrationRequest("test");
        request.setTag("test");
        MigrationRequest.ClusterInfo migrationCluster = new MigrationRequest.ClusterInfo(1899, "cluster-test-transaction",
                1, "jq", 2, "oy");
        request.addClusterInfo(migrationCluster);

        try {
            migrationEventDao.createMigrationEvent(request);
            Assert.fail();
        } catch(Exception e) {
            logger.info("[testTransactionRollback] expected fail", e);
        }

        ClusterTbl clusterTbl = clusterService.find(1899);
        logger.info("[testTransactionRollback] cluster {}", clusterTbl);
        Assert.assertEquals(ClusterStatus.Normal.name(), clusterTbl.getStatus());
        Assert.assertEquals(0, clusterTbl.getMigrationEventId());
    }

    @Override
    // init db with xpipedemodbinitdata.sql first
    protected String prepareDatas() throws IOException {
        return "insert into CLUSTER_TBL " +
                "(id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,migration_event_id,is_xpipe_interested, cluster_org_id) " +
                "values (1899,'cluster-test-transaction',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Normal',0,1, 1);";
    }

    @After
    public void clearData() throws Exception {
        executeSqlScript("delete from CLUSTER_TBL where id = 1899");
    }

}
