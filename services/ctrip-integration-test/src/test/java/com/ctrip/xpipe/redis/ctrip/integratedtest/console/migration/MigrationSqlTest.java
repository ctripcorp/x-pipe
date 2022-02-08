package com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration;

import com.ctrip.xpipe.redis.console.dao.MigrationEventDao;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationCluster;
import com.ctrip.xpipe.redis.console.migration.model.impl.DefaultMigrationShard;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationRequest;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.AbstractCtripConsoleIntegrationTest;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock.MockMigrationCommandBuilder;
import com.ctrip.xpipe.redis.ctrip.integratedtest.console.migration.mock.MockMigrationEventDao;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author lishanglin
 * date 2021/10/9
 */
@SpringBootApplication(scanBasePackages = "com.ctrip.xpipe.redis.console")
public class MigrationSqlTest extends AbstractCtripConsoleIntegrationTest {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Autowired
    private MigrationEventDao migrationEventDao;

    private MockMigrationEventDao mockMigrationEventDao;

    private int concurrent = 1000;

    private String fromDc = "jq";

    private String toDc = "oy";

    private Set<String> finishClusters;

    @Before
    public void setupMigrationSqlTest() {
        mockMigrationEventDao = new MockMigrationEventDao(migrationEventDao,
                new MockMigrationCommandBuilder(0, 0, 0, 0)); // no delay
        finishClusters = new HashSet<>();
    }

    @Test
    public void testConcurrentExecSQL() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(concurrent);
        List<ClusterTbl> clusters = clusterService.findActiveClustersByDcName(fromDc);
        if (clusters.size() < concurrent) Assert.fail("too less clusters to do test");
        else if (clusters.size() > concurrent) clusters = clusters.subList(0, concurrent);

        DcTbl fromDcTbl = dcService.findByDcName(fromDc);
        DcTbl toDcTbl = dcService.findByDcName(toDc);
        if (null == fromDcTbl || null == toDcTbl) Assert.fail("unexpected dc");

        clusters.forEach(clusterTbl -> {
            ExecutorService migrationExecutors = Executors.newFixedThreadPool(1, XpipeThreadFactory.create("MIGRATION-" + clusterTbl.getClusterName()));

            migrationExecutors.execute(() -> {
                try {
                    barrier.await();
                } catch (Throwable th) {
                    logger.warn("[testConcurrentExecSQL][{}] barrier wait fail", clusterTbl.getClusterName(), th);
                }
                (new MigrationMockSql(clusterTbl, fromDcTbl, toDcTbl, migrationExecutors)).run();
            });
        });
        waitConditionUntilTimeOut(() -> finishClusters.size() == concurrent, 120000, 1000);
    }

    private MigrationRequest mockMigrationRequest(long clusterId, String clusterName, long fromDcId, String fromDcName,
                                                  long toDcId, String toDcName) {
        MigrationRequest request = new MigrationRequest("test");
        request.setTag("test");
        MigrationRequest.ClusterInfo migrationCluster = new MigrationRequest.ClusterInfo(clusterId, clusterName,
                fromDcId, fromDcName, toDcId, toDcName);
        request.addClusterInfo(migrationCluster);

        return request;
    }

    private class MigrationMockSql implements Runnable {

        private ClusterTbl clusterTbl;

        private DcTbl srcDcTbl;

        private DcTbl destDcTbl;

        private ExecutorService migrationExecutors;

        public MigrationMockSql(ClusterTbl clusterTbl, DcTbl srcDcTbl, DcTbl destDcTbl, ExecutorService migrationExecutors) {
            this.clusterTbl = clusterTbl;
            this.srcDcTbl = srcDcTbl;
            this.destDcTbl = destDcTbl;
            this.migrationExecutors = migrationExecutors;
        }

        @Override
        public void run() {
            logger.info("[run][{}] begin", clusterTbl.getClusterName());
            MigrationEvent event = mockMigrationEventDao.createMigrationEvent(mockMigrationRequest(clusterTbl.getId(), clusterTbl.getClusterName(),
                    srcDcTbl.getId(), srcDcTbl.getDcName(), destDcTbl.getId(), destDcTbl.getDcName()));
            ((DefaultMigrationCluster)(event.getMigrationCluster(clusterTbl.getId()))).setMigrationExecutors(migrationExecutors);
            event.getMigrationCluster(clusterTbl.getId()).getMigrationShards().forEach(migrationShard -> {
                ((DefaultMigrationShard)migrationShard).setExecutors(migrationExecutors);
            });

            event.addObserver((args, observable) -> {
                if (MigrationStatus.Success.equals(event.getMigrationCluster(clusterTbl.getId()).getStatus())) finishClusters.add(clusterTbl.getClusterName());
                logger.info("[run][{}] end {}, already finish {}", clusterTbl.getClusterName(), event.getMigrationCluster(clusterTbl.getId()).getStatus(), finishClusters.size());
            });
            event.process();
        }


    }

}
