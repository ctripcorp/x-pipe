package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationProgress;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultMigrationProcessReporter extends AbstractCrossDcIntervalAction implements MigrationReporter{

    @Autowired
    private MigrationService migrationService;

    private static final int DEFAULT_HOURS = 1;

    private static final String DEFAULT_SERVICE = "redis";

    public static String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static String DEFAULT_OPERATOR = "XPIPE";

    private static String REPORT_EVENT = "migration.process.report";

    private DefaultHttpService httpService = new DefaultHttpService();

    @Override
    protected void doAction() {
        EventMonitor.DEFAULT.logEvent(REPORT_EVENT, "begin");
        MigrationProcessReportModel model = new MigrationProcessReportModel();
        MigrationProgress migrationProgress = migrationService.buildMigrationProgress(DEFAULT_HOURS);
        if (migrationProgress.getTotal() > 0) {
            if (migrationProgress.getSuccess() > migrationProgress.getTotal()) {
                EventMonitor.DEFAULT.logEvent(REPORT_EVENT, "wrong migrationProgress");
                logger.warn("[DefaultMigrationReporter] build migration progress fail: success {} is greater than total {}",
                        migrationProgress.getSuccess(), migrationProgress.getTotal());
                return;
            }
            model.setObjectCount(migrationProgress.getTotal())
                    .setProcess((100 * migrationProgress.getSuccess() / migrationProgress.getTotal()));
        } else {
            model.setProcess(0).setObjectCount(0);
        }

        model.setService(DEFAULT_SERVICE).setTimestamp(DateTimeUtils.currentTimeAsString(DEFAULT_TIME_FORMAT)).setOperator(DEFAULT_OPERATOR);
        httpService.getRestTemplate().postForEntity(consoleConfig.getKeyMigrationProcessReportUrl(), model, MigrationProcessReportResponseModel.class);
    }

    @Override
    protected boolean shouldDoAction() {
        logger.debug("[DefaultMigrationReporter]get switch {}", consoleConfig.isMigrationProcessReportOpen());
        return consoleConfig.isMigrationProcessReportOpen() && super.shouldDoAction();
    }

    @Override
    protected long getIntervalMilli() {
        return consoleConfig.getMigrationProcessReportIntervalMill();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    protected long getLeastIntervalMilli() {
        return consoleConfig.getMigrationProcessReportIntervalMill();
    }
}
