package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractSiteLeaderIntervalAction;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultMigrationProcessReporter extends AbstractSiteLeaderIntervalAction implements MigrationReporter{

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    private static final int DEFAULT_HOURS = 1;

    private static final String DEFAULT_SERVICE = "redis";

    public static String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static String DEFAULT_OPERATOR = "XPIPE";

    private static String REPORT_EVENT = "migration.process.report";

    private DefaultHttpService httpService = new DefaultHttpService();

    private long totalClusters = 0;

    @Override
    protected void doAction() {
        EventMonitor.DEFAULT.logEvent(REPORT_EVENT, "begin");
        MigrationProcessReportModel model = new MigrationProcessReportModel();

        Long nonMigrateClustersNum = clusterService.getCountByActiveDc(dcService.find(consoleConfig.getBreakDownDc()).getId());
        if (totalClusters == 0) {
            totalClusters = nonMigrateClustersNum;
        }

        if (nonMigrateClustersNum > totalClusters) {
            EventMonitor.DEFAULT.logEvent(REPORT_EVENT, "wrong migrationProgress");
            logger.warn("[DefaultMigrationReporter] build migration progress fail: nonMigrationClustersNum {} is greater than total {}", nonMigrateClustersNum, totalClusters);
            return;
        } else {
            model.setObjectCount((int)totalClusters).setProcess((int)((100 * (totalClusters - nonMigrateClustersNum) / totalClusters)));
        }

        model.setService(DEFAULT_SERVICE).setTimestamp(DateTimeUtils.currentTimeAsString(DEFAULT_TIME_FORMAT)).setOperator(DEFAULT_OPERATOR);
        logger.info("[DefaultMigrationReporter] send migration report model: {}，migration clusters:{}", model, totalClusters - nonMigrateClustersNum);

        ResponseEntity<MigrationProcessReportResponseModel> responseEntity
                = httpService.getRestTemplate().postForEntity(consoleConfig.getKeyMigrationProcessReportUrl(), model, MigrationProcessReportResponseModel.class);
        if (responseEntity.getBody().getCode() != 200) {
            logger.warn("[DefaultMigrationReporter] send migration report fail! migration model: {}, result:{}", model, responseEntity.getBody());
        }
    }

    @Override
    protected boolean shouldDoAction() {
        logger.debug("[DefaultMigrationReporter]get switch {}", consoleConfig.isMigrationProcessReportOpen());
        if (!consoleConfig.isMigrationProcessReportOpen()) totalClusters = 0;
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
