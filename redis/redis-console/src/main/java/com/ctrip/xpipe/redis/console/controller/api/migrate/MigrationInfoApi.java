package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationProgress;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationSystemStatus;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.migration.MigrationSystemAvailableChecker;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

/**
 * @author lishanglin
 * date 2021/5/5
 */
@RestController
@RequestMapping("/api")
public class MigrationInfoApi extends AbstractConsoleController {

    @Autowired
    private MigrationService migrationService;

    @Autowired
    private MigrationSystemAvailableChecker migrationSystemAvailableChecker;

    @Autowired
    private ConsoleConfig config;

    private TimeBoundCache<MigrationProgress> cachedProgress;

    private static final int DEFAULT_HOURS = 1;

    @PostConstruct
    public void postConstruct() {
        cachedProgress = new TimeBoundCache<>(config::getCacheRefreshInterval, () -> migrationService.buildMigrationProgress(DEFAULT_HOURS));
    }

    @GetMapping("/migration/system/health")
    public MigrationSystemStatus getMigrationSystemHealth() {
        MigrationSystemAvailableChecker.MigrationSystemAvailability systemAvailability = migrationSystemAvailableChecker.getResult();

        String status = systemAvailability.isAvaiable() ? "HEALTH" : systemAvailability.isWarning() ? "WARNING" : "UNHEALTH";
        MigrationSystemStatus migrationSystemStatus = new MigrationSystemStatus(status, systemAvailability.getMessage(), systemAvailability.getTimestamp());
        systemAvailability.getCheckResults().forEach((title, result) -> migrationSystemStatus.checkUseTimeMill.put(title, result.getCheckTimeMilli()));

        return migrationSystemStatus;
    }

    @GetMapping("/dr/progress")
    public MigrationProgress getCurrentMigrationProgress(@RequestParam(defaultValue = "false") Boolean disableCache,
                                                         @RequestParam(required = false) Integer hours) {
        if (null != hours && disableCache) {
            return migrationService.buildMigrationProgress(hours);
        }
        return cachedProgress.getData(false);
    }

}
