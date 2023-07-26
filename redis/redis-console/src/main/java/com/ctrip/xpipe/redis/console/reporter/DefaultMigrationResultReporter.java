package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.DO_STATUS;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DefaultMigrationResultReporter extends AbstractCrossDcIntervalAction implements MigrationReporter  {

    @Autowired
    private MigrationService migrationService;

    @Autowired
    private ConsoleConfig config;

    @Autowired
    private DcService dcService;

    @Autowired
    private OrganizationService organizationService;

    public static String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static String REPORT_EVENT = "migration.result.report";

    private static String DEFAULT_CATEGORY = "Operation";

    private static String DEFAULT_TOOL = "Credis";

    private static String DEFAULT_SUBJECT = "redis DR集群主机房切换";

    private static int DEFAULT_STATISTICAL_INTERVAL = 60;

    private static String DEFAULT_MIGRATION_DETAIL_URL = "/#/migration_event_details/%d/details";

    private Map<Long, String> dcIdNameMap;

    private Map<Long, String> orgIdNameMap;

    private DefaultHttpService httpService;

    @PostConstruct
    public void init() {
        httpService = new DefaultHttpService();
        dcIdNameMap = dcService.dcNameMap();
        orgIdNameMap = organizationService.getAllOrganizations().stream().collect(Collectors.toMap(OrganizationTbl::getId, OrganizationTbl::getOrgName));
    }

    @Override
    protected void doAction() {
        EventMonitor.DEFAULT.logEvent(REPORT_EVENT, "begin");
        List<MigrationClusterTbl> latestMigrationClusters = migrationService.getLatestMigrationClusters(DEFAULT_STATISTICAL_INTERVAL);
        List<MigrationResultModel> migrationResult = new ArrayList<>();

        latestMigrationClusters.forEach(migrationClusterTbl -> {
            MigrationResultModel model = new MigrationResultModel().setCategory(DEFAULT_CATEGORY)
                    .setTool(DEFAULT_TOOL).setSubject(DEFAULT_SUBJECT).setOperator(migrationClusterTbl.getOperator())
                    .setTimestamp(migrationClusterTbl.getStartTime().toString()).setClusterName(migrationClusterTbl.getCluster().getClusterName())
                    .setBu(orgIdNameMap.get(migrationClusterTbl.getCluster().getClusterOrgId()))
                    .setUrl(config.getConsoleDomain() + String.format(DEFAULT_MIGRATION_DETAIL_URL, migrationClusterTbl.getMigrationEventId()))
                    .setOriginMasterDc(dcIdNameMap.get(migrationClusterTbl.getSourceDcId()))
                    .setTargetMasterDc(dcIdNameMap.get(migrationClusterTbl.getDestinationDcId()))
                    .setTaskId(migrationClusterTbl.getMigrationEventId());
            model.setResult(DO_STATUS.fromMigrationStatus(MigrationStatus.valueOf(migrationClusterTbl.getStatus())).toString());
        });

        MigrationResultReportModel model = new MigrationResultReportModel().setRequest_body(migrationResult)
                                                .setAccess_token(config.getKeyMigrationResultReportToken());
        ResponseEntity<MigrationResultReportResponseModel> response = httpService.getRestTemplate()
                .postForEntity(config.getKeyMigrationResultReportUrl(), model, MigrationResultReportResponseModel.class);
        if (response != null && response.getBody() != null && response.getBody().getCode() != 200) {
            logger.warn("[reportToNoc] send migration result fail! migration result: {}, result:{}", migrationResult, response.getBody());
        }
    }

    @Override
    protected boolean shouldDoAction() {
        logger.debug("[DefaultMigrationResultReporter]get switch {}", consoleConfig.isMigrationProcessReportOpen());
        return consoleConfig.isMigrationProcessReportOpen() && super.shouldDoAction();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }
}
