package com.ctrip.xpipe.redis.console.dao;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.migration.model.MigrationEvent;

public class MigrationEventDaoTest extends AbstractConsoleIntegrationTest {
	
	@Autowired
	MigrationEventDao migrationEventDao;
	
	@Override
	public String prepareDatas() {
		try {
			return prepareDatasFromFile("src/test/resources/migration-test.sql");
		} catch (Exception ex) {
			logger.error("Prepare data from file failed",ex);
		}
		return "";
	}
	
	@Test
	@DirtiesContext
	public void testBuildMigrationEvent() {

		MigrationEvent event = migrationEventDao.buildMigrationEvent(2);
		
		Assert.assertNotNull(event);
		Assert.assertEquals(2, event.getMigrationCluster(2).getMigrationShards().size());
	}

}
