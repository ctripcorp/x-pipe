package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareRequest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareResponse;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.redis.console.service.migration.impl.TryMigrateResult;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author wenchao.meng
 *         May 12, 2021
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationApiTest extends AbstractConsoleTest {

    private MigrationApi migrationApi;

    @Mock
    private MigrationService migrationService;

    private String[] clusters = new String[] { "cluster1", "cluster2", "cluster3" };
    private String fromIdc = "shaoy", toIdc = "shajq";
    private CheckPrepareRequest checkPrepareRequest = new CheckPrepareRequest();
    private Long eventId;

    @Before
    public void beforeMigrationApi() {

        eventId = Long.valueOf(randomInt());
        migrationApi = new MigrationApi();
        migrationApi.setMigrationService(migrationService);

        checkPrepareRequest.setClusters(Lists.newArrayList(clusters));
        checkPrepareRequest.setFromIdc(fromIdc);
        checkPrepareRequest.setToIdc(toIdc);
        when(migrationService.createMigrationEvent(any())).thenReturn(eventId);

    }

    @Test
    public void testCheckAndPrepareNewCluster()
            throws MigrationNotSupportException, ToIdcNotFoundException, ClusterMigratingNow, ClusterNotFoundException,
            MigrationSystemNotHealthyException, ClusterActiveDcNotRequest, ClusterMigratingNowButMisMatch {

        for (String cluster : clusters) {
            when(migrationService.tryMigrate(cluster, fromIdc, toIdc))
                    .thenReturn(newTryMigrateResult(cluster, fromIdc, toIdc));
        }
        CheckPrepareResponse checkPrepareResponse = migrationApi.checkAndPrepare(checkPrepareRequest);

        Assert.assertEquals(eventId.longValue(), checkPrepareResponse.getTicketId());
        Assert.assertEquals(3, checkPrepareResponse.getResults().size());
        checkPrepareResponse.getResults().forEach(checkPrepareClusterResponse -> {
            Assert.assertEquals(true, checkPrepareClusterResponse.isSuccess());
        });

    }

    @Test
    public void testCheckAndPrepareAlreadyMigratingCluster()
            throws MigrationNotSupportException, ToIdcNotFoundException, ClusterMigratingNow, ClusterNotFoundException,
            MigrationSystemNotHealthyException, ClusterActiveDcNotRequest, ClusterMigratingNowButMisMatch {

        int i = 0;
        for (String cluster : clusters) {
            when(migrationService.tryMigrate(cluster, fromIdc, toIdc))
                    .thenThrow(new ClusterMigratingNow(cluster, fromIdc, toIdc, eventId + i));
            i++;
        }

        Set<Long> eventIds = new HashSet<>();
        for (i = 0; i < clusters.length * 10; i++) {

            CheckPrepareResponse checkPrepareResponse = migrationApi.checkAndPrepare(checkPrepareRequest);
            eventIds.add(checkPrepareResponse.getTicketId());

            Assert.assertEquals(3, checkPrepareResponse.getResults().size());
            AtomicInteger count = new AtomicInteger();
            checkPrepareResponse.getResults().forEach(checkPrepareClusterResponse -> {
                if (checkPrepareClusterResponse.isSuccess()) {
                    count.getAndIncrement();
                }
            });
            Assert.assertEquals(1, count.get());
        }

        // choose randomly
        for (i = 0; i < clusters.length; i++) {
            Assert.assertTrue(eventIds.contains(eventId + i));
        }

    }

    @Test
    public void testCheckAndPrepareException()
            throws MigrationNotSupportException, ToIdcNotFoundException, ClusterMigratingNow, ClusterNotFoundException,
            MigrationSystemNotHealthyException, ClusterActiveDcNotRequest, ClusterMigratingNowButMisMatch {

        int i = 0;
        for (String cluster : clusters) {

            when(migrationService.tryMigrate(cluster, fromIdc, toIdc)).thenThrow(new ClusterMigratingNowButMisMatch(
                    cluster, fromIdc + "R", toIdc + "R", eventId, fromIdc, toIdc));
        }

        CheckPrepareResponse checkPrepareResponse = migrationApi.checkAndPrepare(checkPrepareRequest);
        checkPrepareResponse.getResults().forEach(checkPrepareClusterResponse -> {
            Assert.assertFalse(checkPrepareClusterResponse.isSuccess());
        });

    }

    private TryMigrateResult newTryMigrateResult(String cluster, String fromIdc, String toIdc) {

        TryMigrateResult result = new TryMigrateResult();
        result.setClusterId(cluster.hashCode());
        result.setClusterName(cluster);
        result.setFromDcId(fromIdc.hashCode());
        result.setFromDcName(fromIdc);
        result.setToDcId(toIdc.hashCode());
        result.setToDcName(toIdc);
        return result;
    }

}
