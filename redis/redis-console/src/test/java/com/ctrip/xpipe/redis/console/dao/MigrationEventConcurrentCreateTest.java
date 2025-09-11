package com.ctrip.xpipe.redis.console.dao;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.aop.DalTransactionAspect;
import com.ctrip.xpipe.redis.console.exception.ServerException;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.model.ClusterTblDao;
import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.unidal.dal.jdbc.DalRuntimeException;
import org.unidal.dal.jdbc.transaction.TransactionManager;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;

/**
 * @author lishanglin
 * date 2020/12/18
 */
public class MigrationEventConcurrentCreateTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private MigrationEventDao migrationEventDao;

    @Autowired
    private DalTransactionAspect dalTransactionAspect;

    private ClusterTblDao clusterTblDao;

    @Before
    public void setupMigrationEventConcurrentCreateTest() throws Exception {
        clusterTblDao = Mockito.mock(ClusterTblDao.class);
        migrationEventDao.setClusterTblDao(clusterTblDao);
        Mockito.when(clusterTblDao.atomicSetStatus(Mockito.any(), Mockito.any())).thenReturn(1);
    }

    @DirtiesContext
    @Test
    public void testCreateSuccess() {
        MigrationEvent migrationEvent = migrationEventDao.createMigrationEvent(mockEvent());
        List<MigrationClusterModel> migrationClusterModels = migrationEventDao.getMigrationCluster(migrationEvent.getMigrationEventId());
        Assert.assertEquals(1, migrationClusterModels.size());
    }

    @DirtiesContext
    @Test(expected = ServerException.class)
    public void testLockClusterRowZero() throws Exception {
        Mockito.when(clusterTblDao.atomicSetStatus(Mockito.any(), Mockito.any())).thenReturn(0);
        migrationEventDao.createMigrationEvent(mockEvent());
    }

    @DirtiesContext
    @Test(expected = ServerException.class)
    public void testLockClusterCommitFail() throws Exception {
        Mockito.doThrow(new DalRuntimeException("commit fail for data conflict"))
                .when(hackTransaction()).commitTransaction();
        migrationEventDao.createMigrationEvent(mockEvent());
    }

    private TransactionManager hackTransaction() throws Exception {
        Field transactionField = DalTransactionAspect.class.getDeclaredField("transactionManager");
        transactionField.setAccessible(true);
        TransactionManager transactionManager = (TransactionManager)transactionField.get(dalTransactionAspect);

        transactionManager = Mockito.spy(transactionManager);
        transactionField.set(dalTransactionAspect, transactionManager);

        return transactionManager;
    }

    private MigrationRequest mockEvent() {

        MigrationRequest migrationRequest = new MigrationRequest("unit test");
        migrationRequest.setTag("unit test-" + getTestName());

        MigrationRequest.ClusterInfo clusterInfo = new MigrationRequest.ClusterInfo();
        clusterInfo.setClusterId(1);
        clusterInfo.setToDcId(2);
        migrationRequest.addClusterInfo(clusterInfo);
        return migrationRequest;
    }

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/migration-event-create-test.sql");
    }

}
