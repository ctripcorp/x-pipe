package com.ctrip.xpipe.redis.console.controller.consoleportal;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.RetMessage;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.model.*;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.console.service.migration.exception.ClusterNotFoundException;
import com.ctrip.xpipe.redis.console.util.DataModifiedTimeGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
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

	@Autowired
	private ConfigService configService;

	@Autowired
	private ClusterService clusterService;

	@Autowired
	private ConsoleConfig consoleConfig;

	@Autowired
	private DcService dcService;
	
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
		logger.info("[getEventDetailsWithEventId][begin] eventId: {}", eventId);
		List<MigrationClusterModel> res = new LinkedList<>();
		if (null != eventId) {
			res = migrationService.getMigrationClusterModel(eventId);
		} else {
			logger.error("[GetEvent][fail]Cannot findRedisHealthCheckInstance with null event id.");
		}
		logger.info("[getEventDetailsWithEventId][end] eventId: {}", eventId);
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

	@RequestMapping(value = "/migration/system/health/status", method = RequestMethod.GET)
	public RetMessage getMigrationSystemHealthStatus() {
		logger.info("[getMigrationSystemHealthStatus][begin]");
		try {
			return migrationService.getMigrationSystemHealth();
		} catch (Exception e) {
			logger.error("[getMigrationSystemHealthStatus]", e);
			return RetMessage.createFailMessage(e.getMessage());
		}
	}

	@RequestMapping(value = "/migration/default/cluster", method = RequestMethod.GET)
	public ClusterTbl getDefaultMigrationCluster() {
		String clusterName = consoleConfig.getClusterShardForMigrationSysCheck().getKey();
		ClusterTbl clusterTbl = clusterService.findClusterAndOrg(clusterName);
		if(clusterTbl == null || clusterTbl.getClusterName() == null) {
			logger.warn("[getDefaultMigrationCluster]not found default cluster: {}", clusterName);
		}
		List<DcTbl> dcs = dcService.findClusterRelatedDc(clusterTbl.getClusterName());

		return clusterTbl;
	}
}
