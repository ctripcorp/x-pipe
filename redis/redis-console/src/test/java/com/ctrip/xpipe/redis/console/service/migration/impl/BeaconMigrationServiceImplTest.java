package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.BeaconMigrationRequest;
import com.ctrip.xpipe.redis.console.dao.MigrationClusterDao;
import com.ctrip.xpipe.redis.console.migration.exception.MigrationUnderProcessingException;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.beacon.BeaconGroupModel;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.service.meta.impl.BeaconMetaServiceImpl;
import com.ctrip.xpipe.redis.console.service.migration.exception.*;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author lishanglin
 * date 2020/12/31
 */
public class BeaconMigrationServiceImplTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private BeaconMigrationServiceImpl migrationService;

    @Autowired
    private BeaconMetaServiceImpl beaconMetaService;

    @Autowired
    private MigrationClusterDao migrationClusterDao;

    private MetaCache metaCache;

    @Override
    protected String prepareDatas() throws IOException {
        return prepareDatasFromFile("src/test/resources/beacon-migration-test.sql");
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "beacon-migration-test.xml";
    }

    @Before
    public void setupBeaconMigrationServiceImplTest() throws Exception {
        metaCache = Mockito.mock(MetaCache.class);
        Mockito.when(metaCache.getXpipeMeta()).thenReturn(getXpipeMeta());
        Mockito.doAnswer(invocation -> {
            String activeDc = invocation.getArgumentAt(0, String.class);
            String backupDc = invocation.getArgumentAt(1, String.class);
            XpipeMeta xpipeMeta = getXpipeMeta();
            return !xpipeMeta.getDcs().get(activeDc).getZone().equals(xpipeMeta.getDcs().get(backupDc).getZone());
        }).when(metaCache).isCrossRegion(Mockito.anyString(), Mockito.anyString());

        beaconMetaService.setMetaCache(metaCache);
    }

    @Test
    public void testActiveDcDown() throws Exception {
        long eventId = migrationService.buildMigration(buildBeaconMigrationRequest("cluster1", Sets.newHashSet("jq")));
        MigrationClusterTbl migrationClusterTbl = migrationClusterDao.findByEventIdAndClusterId(eventId, 1);

        Assert.assertEquals(1, migrationClusterTbl.getSourceDcId());
        Assert.assertEquals(2, migrationClusterTbl.getDestinationDcId());
        Assert.assertEquals(MigrationStatus.Initiated.name(), migrationClusterTbl.getStatus());

        testDoFailover(eventId, 1);
    }

    @Test(expected = MigrationNoNeedException.class)
    public void testBackupDcDown() throws Exception {
        migrationService.buildMigration(buildBeaconMigrationRequest("cluster1", Sets.newHashSet("oy")));
    }

    @Test(expected = WrongClusterMetaException.class)
    public void testRequestWithErrMeta() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet("jq"));
        request.getGroups().iterator().next().getNodes().iterator().next().setPort(1111);
        migrationService.buildMigration(request);
    }

    @Test(expected = ClusterNotFoundException.class)
    public void testRequestWithUnknownCluster() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet("jq"));
        request.setClusterName("cluster-x");
        migrationService.buildMigration(request);
    }

    @Test(expected = NoAvailableDcException.class)
    public void testAllDcDown() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet("jq", "oy"));
        migrationService.buildMigration(request);
    }

    @Test
    public void testMigrationExisted() throws Exception {
        long eventId = migrationService.buildMigration(buildBeaconMigrationRequest("cluster2", Sets.newHashSet("jq")));
        logger.info("[testMigrationExisted] {}", eventId);
        testDoFailover(eventId, 2);
    }

    @Test(expected = MigrationUnderProcessingException.class)
    public void testMigrationUnfinished() throws Exception {
        long eventId = migrationService.buildMigration(buildBeaconMigrationRequest("cluster3", Sets.newHashSet("jq")));
        migrationService.doMigration(eventId, 3).get(); // lock for exec fail
    }

    @Test(expected = MigrationConflictException.class)
    public void testMigrationConflict() throws Exception {
        migrationService.buildMigration(buildBeaconMigrationRequest("cluster4", Sets.newHashSet("jq")));
    }

    @Test
    public void testForceMigration() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet());
        request.setIsForced(true);
        request.setTargetIDC("oy");

        long eventId = migrationService.buildMigration(request);
        testDoFailover(eventId, 1);
    }

    @Test(expected = UnknownTargetDcException.class)
    public void testForceMigrationToUnknownDc() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet());
        request.setIsForced(true);
        request.setTargetIDC("fq");

        migrationService.buildMigration(request);
    }

    @Test(expected = MigrationCrossZoneException.class)
    public void testForceMigrationCrossZone() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet());
        request.setIsForced(true);
        request.setTargetIDC("fra");

        migrationService.buildMigration(request);
    }

    @Test(expected = MigrationNoNeedException.class)
    public void testForceMigrationToActiveDc() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet("jq"));
        request.setIsForced(true);
        request.setTargetIDC("jq");
        migrationService.buildMigration(request);
    }

    @Test
    public void testForceMigrationWithErrMeta() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet());
        request.getGroups().iterator().next().getNodes().iterator().next().setPort(1111);
        request.setIsForced(true);
        request.setTargetIDC("oy");

        // ignore meta wrong in force migration
        long eventId = migrationService.buildMigration(request);
        testDoFailover(eventId, 1);
    }

    @Test
    public void testForceMigrationWithoutAvailableDc() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster1", Sets.newHashSet("jq", "oy"));
        request.setIsForced(true);
        request.setTargetIDC("oy");

        // ignore available dc check in force migration
        long eventId = migrationService.buildMigration(request);
        testDoFailover(eventId, 1);
    }

    @Test
    public void testForceMigrationWithExistMigration() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster2", Sets.newHashSet());
        request.setIsForced(true);
        request.setTargetIDC("oy");

        long eventId = migrationService.buildMigration(request);
        testDoFailover(eventId, 2);
    }

    @Test(expected = MigrationConflictException.class)
    public void testForceMigrationWithConflictMigration() throws Exception {
        BeaconMigrationRequest request = buildBeaconMigrationRequest("cluster4", Sets.newHashSet());
        request.setIsForced(true);
        request.setTargetIDC("oy");

        migrationService.buildMigration(request);
    }

    private void testDoFailover(long eventId, long clusterId) throws Exception {
        try {
            migrationService.doMigration(eventId, clusterId).get();
            Assert.fail();
        } catch (ExecutionException e) {
            Assert.assertEquals(ClusterMigrationNotSuccessException.class, e.getCause().getClass());
        }
        MigrationClusterTbl migrationClusterTbl = migrationClusterDao.findByEventIdAndClusterId(eventId, clusterId);
        Assert.assertEquals(MigrationStatus.CheckingFail.name(), migrationClusterTbl.getStatus());
    }

    private BeaconMigrationRequest buildBeaconMigrationRequest(String clusterName, Set<String> failDcs) {
        BeaconMigrationRequest request = new BeaconMigrationRequest();
        Set<BeaconGroupModel> groups = beaconMetaService.buildBeaconGroups(clusterName);
        Set<String> failoverGroups = new HashSet<>();

        request.setClusterName(clusterName);

        groups.forEach(group -> {
            if (failDcs.contains(group.getIdc())) {
                group.setDown(true);
                failoverGroups.add(group.getName());
            } else {
                group.setDown(false);
            }
        });

        request.setFailoverGroups(failoverGroups);
        request.setGroups(groups);

        return request;
    }

}
