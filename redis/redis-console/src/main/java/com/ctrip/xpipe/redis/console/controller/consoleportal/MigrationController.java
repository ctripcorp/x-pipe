package com.ctrip.xpipe.redis.console.controller.consoleportal;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;

/**
 * @author shyin
 *
 * Dec 12, 2016
 */
@RestController
@RequestMapping("console")
public class MigrationController extends AbstractConsoleController {
	
	@Autowired
	private MigrationService migrationService;
	
	@RequestMapping(value = "/migration/events", method = RequestMethod.POST)
	public Map<String, Long> createEvent(@RequestBody MigrationEventModel event) {
		Map<String, Long> res = new HashMap<>();
		logger.info("[Create Event]{}", event);
		res.put("value", migrationService.createMigrationEvent(event));
		logger.info("[Create Event][Done]{}", event);
		return res;
	}
	
	@RequestMapping(value = "/migration/events/all", method = RequestMethod.GET) 
	public List<MigrationEventTbl> getAllEvents() {
		return migrationService.findAll();
	}
	
	@RequestMapping(value = "/migration/events/{eventId}", method = RequestMethod.GET) 
	public List<MigrationClusterModel> getEventDetailsWithEventId(@PathVariable Long eventId) {
		List<MigrationClusterModel> res = new LinkedList<>();
		if (null != eventId) {
			res = migrationService.getMigrationClusterModel(eventId);
		} else {
			logger.error("[GetEvent][fail]Cannot get with null event id.");
		}
		return res;
	}
	
	@RequestMapping(value = "/migration/events/{eventId}/clusters/{clusterId}", method = RequestMethod.POST)
	public void continueMigrationCluster(@PathVariable Long eventId, @PathVariable Long clusterId) {
		migrationService.continueMigrationCluster(eventId, clusterId);
	}
	
	@RequestMapping(value = "/migration/events/{eventId}/clusters/{clusterId}/cancel", method = RequestMethod.POST)
	public void cancelMigrationCluster(@PathVariable Long eventId, @PathVariable Long clusterId) {
		migrationService.cancelMigrationCluster(eventId, clusterId);
	}

	@RequestMapping(value = "/migration/events/{eventId}/clusters/{clusterId}/rollback", method = RequestMethod.POST)
	public void rollbackMigrationCluster(@PathVariable Long eventId, @PathVariable Long clusterId) {
		migrationService.rollbackMigrationCluster(eventId, clusterId);
	}
	
}
