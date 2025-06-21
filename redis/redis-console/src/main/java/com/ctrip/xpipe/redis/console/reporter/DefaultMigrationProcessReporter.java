package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractSiteLeaderIntervalAction;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultMigrationProcessReporter extends AbstractSiteLeaderIntervalAction implements MigrationReporter{

    @Autowired
    private MigrationReporterConfig migrationReporterConfig;

    @Autowired
    private MetaCache metaCache;

    private static final String DEFAULT_SERVICE = "redis";

    public static String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static String DEFAULT_OPERATOR = "XPIPE";

    private static String REPORT_EVENT = "migration.process.report";

    private DefaultHttpService httpService = new DefaultHttpService();

    private long totalClusters = 0;

    private Set<String> lastBreakDownDc;

    @Override
    protected void doAction() {
        EventMonitor.DEFAULT.logEvent(REPORT_EVENT, "begin");
        MigrationProcessReportModel model = new MigrationProcessReportModel();
        if (lastBreakDownDc == null) {
            lastBreakDownDc = migrationReporterConfig.getBreakDownDc();
        }
        if (lastBreakDownDc != null && !lastBreakDownDc.equals(migrationReporterConfig.getBreakDownDc()) ) {
            lastBreakDownDc = migrationReporterConfig.getBreakDownDc();
            totalClusters = 0;
        }

        Long nonMigrateClustersNum = 0L;
        for (String breakDownDc : migrationReporterConfig.getBreakDownDc()) {
            nonMigrateClustersNum += metaCache.getMigratableClustersCountByActiveDc(breakDownDc);
        }
        if (totalClusters == 0 || nonMigrateClustersNum > totalClusters) {
            totalClusters = nonMigrateClustersNum;
        }

        if (totalClusters == 0) return;

        model.setObjectCount((int)totalClusters).setProcess((int)((100 * (totalClusters - nonMigrateClustersNum) / totalClusters)));

        model.setService(DEFAULT_SERVICE).setTimestamp(DateTimeUtils.currentTimeAsString(DEFAULT_TIME_FORMAT)).setOperator(DEFAULT_OPERATOR);
        logger.info("[DefaultMigrationReporter] send migration report model: {}，migration clusters:{}", model, totalClusters - nonMigrateClustersNum);

        ResponseEntity<NocReportResponseModel> responseEntity
                = httpService.getRestTemplate().postForEntity(migrationReporterConfig.getKeyMigrationProcessReportUrl(), model, NocReportResponseModel.class);
        if (responseEntity != null && responseEntity.getBody() != null &&  responseEntity.getBody().getCode() != 200) {
            logger.warn("[DefaultMigrationReporter] send migration report fail! migration model: {}, result:{}", model, responseEntity.getBody());
        }
    }

    @Override
    protected boolean shouldDoAction() {
        logger.debug("[DefaultMigrationReporter]get switch {}", migrationReporterConfig.isMigrationProcessReportOpen());
        if (!migrationReporterConfig.isMigrationProcessReportOpen()) totalClusters = 0;
        return migrationReporterConfig.isMigrationProcessReportOpen() && super.shouldDoAction();
    }

    @Override
    protected long getIntervalMilli() {
        return migrationReporterConfig.getMigrationProcessReportIntervalMill();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }

    @Override
    protected long getLeastIntervalMilli() {
        return migrationReporterConfig.getMigrationProcessReportIntervalMill();
    }
}
