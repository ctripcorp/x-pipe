package com.ctrip.xpipe.redis.console.controller.api.migrate;

import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationProgress;
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
@RequestMapping("/api/migration/info")
public class MigrationInfoApi extends AbstractConsoleController {

    @Autowired
    private MigrationService migrationService;

    @Autowired
    private ConsoleConfig config;

    private TimeBoundCache<MigrationProgress> cachedProgress;

    private static final int DEFAULT_HOURS = 1;

    @PostConstruct
    public void postConstruct() {
        cachedProgress = new TimeBoundCache<>(config::getCacheRefreshInterval, () -> migrationService.buildMigrationProgress(DEFAULT_HOURS));
    }

    @GetMapping("/progress")
    public MigrationProgress getCurrentMigrationProgress(@RequestParam(defaultValue = "false") Boolean disableCache,
                                                         @RequestParam(required = false) Integer hours) {
        if (null != hours && disableCache) {
            return migrationService.buildMigrationProgress(hours);
        }
        return cachedProgress.getData(false);
    }

}
