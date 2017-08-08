package com.ctrip.xpipe.redis.console.service.migration.impl;

import com.ctrip.xpipe.redis.console.migration.AbstractMigrationTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.migration.exception.ToIdcNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.LinkedList;
import java.util.List;

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

}
