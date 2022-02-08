package com.ctrip.xpipe.redis.console.migration.status;

import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationCluster;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author wenchao.meng
 *
 * Mar 17, 2017
 */
@RunWith(MockitoJUnitRunner.class)
public class MigrationStatusTest extends AbstractConsoleTest{
	
	@Mock
	private MigrationCluster migrationCluster;
	
	@Test
	public void testcreateMigrationState(){
		
		for(MigrationStatus migrationStatus : MigrationStatus.values()){
			migrationStatus.createMigrationState(migrationCluster);
		}
		
	}

}
