package com.ctrip.xpipe.redis.console.migration.impl;

import com.ctrip.xpipe.redis.console.migration.AbstractMigrationTest;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationServiceImpl;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;

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


}
