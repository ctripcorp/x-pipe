package com.ctrip.xpipe.redis.console.controller.consoleportal;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.model.MigrationEventModel;
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
	public void createEvent(@RequestBody MigrationEventModel event) {
		logger.info("[Create Event]{}", event);
		migrationService.createMigrationEvent(event);
		logger.info("[Create Event][Done]{}", event);
	}
	
}
