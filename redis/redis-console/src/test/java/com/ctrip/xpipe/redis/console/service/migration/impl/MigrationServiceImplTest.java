package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.DefaultMigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.migration.AbstractMigrationTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ToIdcNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 05, 2017
 */
public class MigrationServiceImplTest extends AbstractMigrationTest{

    @Autowired
    private MigrationServiceImpl migrationService;


    @Before
    public void prepare(){

    }


    @Test
    public void testFindToDc() throws ToIdcNotFoundException {

        List<DcTbl> relatedDcs = new LinkedList<>();

        try{
            Assert.assertEquals("dc2", migrationService.findToDc("dc1", null, relatedDcs).getDcName());
            Assert.fail();
        }catch (ToIdcNotFoundException e){
        }

        try{
            Assert.assertEquals("dc2", migrationService.findToDc("dc1", "dc2", relatedDcs).getDcName());
            Assert.fail();
        }catch (ToIdcNotFoundException e){
        }

        relatedDcs.add(new DcTbl().setDcName("dc1"));
        relatedDcs.add(new DcTbl().setDcName("dc2"));
        relatedDcs.add(new DcTbl().setDcName("dc3"));

        Assert.assertEquals("dc2", migrationService.findToDc("dc1", null, relatedDcs).getDcName());
        Assert.assertEquals("dc2", migrationService.findToDc("dc1", "dc2", relatedDcs).getDcName());
        Assert.assertEquals("dc3", migrationService.findToDc("dc1", "dc3", relatedDcs).getDcName());

        try {
            Assert.assertEquals("dc3", migrationService.findToDc("dc1", "dc1", relatedDcs).getDcName());
            Assert.fail();
        }catch (ToIdcNotFoundException e){
        }


        try{
            Assert.assertEquals("dc2", migrationService.findToDc("dc1", "dc4", relatedDcs).getDcName());
            Assert.fail();
        }catch (ToIdcNotFoundException e){
        }
    }

    @Test(expected = ToIdcNotFoundException.class)
    public void testFindToDcWithNotSameZone() throws ToIdcNotFoundException {
        List<DcTbl> relatedDcs = new LinkedList<>();

        relatedDcs.add(new DcTbl().setDcName("dc1").setZoneId(1L));
        relatedDcs.add(new DcTbl().setDcName("dc2").setZoneId(1L));
        relatedDcs.add(new DcTbl().setDcName("dc3").setZoneId(2L));
        migrationService.findToDc("dc1", "dc3", relatedDcs);
    }

    @Test(expected = DalUpdateException.class)
    public void testUpdateLogById() {
        migrationService.updateMigrationShardLogById(887L, "test");
    }

    @Test
//    @Ignore
    public void testOverDueMigrationSysCheck() {
        migrationService = new MigrationServiceImpl();
        AlertManager alertManager = mock(AlertManager.class);
        migrationService.setAlertManager(alertManager);
        DefaultMigrationSystemAvailableChecker checker = new DefaultMigrationSystemAvailableChecker();
        migrationService.setChecker(checker);
        sleep(2 * 60 * 1000);
        MigrationSystemAvailableChecker.MigrationSystemAvailability availability = migrationService.getMigrationSystemAvailability();
        Assert.assertTrue(availability.isAvaiable());
        Assert.assertTrue(availability.isWarning());
        verify(alertManager, atLeastOnce()).alert(eq(""), eq(""), eq(new HostPort()), eq(ALERT_TYPE.MIGRATION_SYSTEM_CHECK_OVER_DUE), anyString());
    }
}
