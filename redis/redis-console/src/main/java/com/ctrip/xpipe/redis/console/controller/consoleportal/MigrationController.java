package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.MigrationClusterModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventModel;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author shyin
 *
 * Dec 12, 2016
 */
@RestController
@RequestMapping(AbstractConsoleController.CONSOLE_PREFIX)
public class MigrationController extends AbstractConsoleController {
	
	@Autowired
	private MigrationService migrationService;
	
	@RequestMapping(value = "/migration/events", method = RequestMethod.POST)
	public Map<String, Long> createEvent(@RequestBody MigrationEventModel event) {

		Map<String, Long> res = new HashMap<>();
		logger.info("[Create Event]{}", event);

		String user = userInfoHolder.getUser().getUserId();
		String tag = generateUniqueEventTag(user);

		Long migrationEventId = migrationService.createMigrationEvent(event.createMigrationRequest(user, tag));

		res.put("value", migrationEventId);
		logger.info("[Create Event][Done]{}", migrationEventId);
		return res;
	}

	private String generateUniqueEventTag(String user) {
		StringBuilder sb = new StringBuilder();
		sb.append(DataModifiedTimeGenerator.generateModifiedTime());
		sb.append("-");
		sb.append(user);
		return sb.toString();
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
			logger.error("[GetEvent][fail]Cannot findRedisHealthCheckInstance with null event id.");
		}
		return res;
	}
	
	@RequestMapping(value = "/migration/events/{eventId}/clusters/{clusterId}", method = RequestMethod.POST)
	public void continueMigrationCluster(@PathVariable Long eventId, @PathVariable Long clusterId) {
		logger.info("[continueMigrationCluster]{}, {}", eventId, clusterId);
		migrationService.continueMigrationCluster(eventId, clusterId);
	}
	
	@RequestMapping(value = "/migration/events/{eventId}/clusters/{clusterId}/cancel", method = RequestMethod.POST)
	public void cancelMigrationCluster(@PathVariable Long eventId, @PathVariable Long clusterId) {
		logger.info("[cancelMigrationCluster]{}, {}", eventId, clusterId);
		migrationService.cancelMigrationCluster(eventId, clusterId);
	}

	@RequestMapping(value = "/migration/events/{eventId}/clusters/{clusterId}/tryRollback", method = RequestMethod.POST)
	public void rollbackMigrationCluster(@PathVariable Long eventId, @PathVariable Long clusterId) throws ClusterNotFoundException {
		logger.info("[rollbackMigrationCluster]{}, {}", eventId, clusterId);
		migrationService.rollbackMigrationCluster(eventId, clusterId);
	}
	
	@RequestMapping(value = "/migration/events/{eventId}/clusters/{clusterId}/forcePublish", method = RequestMethod.POST)
	public void forcePublishMigrationCluster(@PathVariable Long eventId, @PathVariable Long clusterId) {
		logger.info("[forcePublishMigrationCluster]{}, {}", eventId, clusterId);
		migrationService.forcePublishMigrationCluster(eventId, clusterId);
	}
	
	@RequestMapping(value = "/migration/events/{eventId}/clusters/{clusterId}/forceEnd", method = RequestMethod.POST)
	public void forceEndMigrationCluster(@PathVariable Long eventId, @PathVariable Long clusterId) {
		logger.info("[forceEndMigrationCluster]{}, {}", eventId, clusterId);
		migrationService.forceEndMigrationClsuter(eventId, clusterId);
	}
	
}
