package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractSiteLeaderIntervalAction;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.DO_STATUS;
import com.ctrip.xpipe.redis.console.migration.status.MigrationStatus;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultMigrationResultReporter extends AbstractSiteLeaderIntervalAction implements MigrationReporter  {

    @Autowired
    private MigrationService migrationService;

    @Autowired
    private OrganizationService organizationService;

    public static String DEFAULT_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private static String REPORT_EVENT = "migration.result.report";

    private static String DEFAULT_CATEGORY = "Operation";

    private static String DEFAULT_TOOL = "Credis";

    private static String DEFAULT_SUBJECT = "redis DR集群主机房切换";

    private static int DEFAULT_STATISTICAL_INTERVAL = 60;

    private static Object DEFAULT_META_DATA = new Object();

    private static String DEFAULT_MIGRATION_DETAIL_URL = "http://%s/#/migration_event_details/%d/details";

    private Map<Long, String> dcIdNameMap;

    private Map<Long, String> orgIdNameMap;

    private DefaultHttpService httpService =  new DefaultHttpService();

    @Autowired
    private MigrationReporterConfig migrationReporterConfig;

    @Autowired
    private MetaCache metaCache;

    private boolean isInited = false;

    @PostConstruct
    public void init() {
        dcIdNameMap = buildDcNameMap();
        if(dcIdNameMap == null) {
            isInited = false;
            return;
        }
        orgIdNameMap = organizationService.getAllOrganizations().stream().collect(Collectors.toMap(OrganizationTbl::getId, OrganizationTbl::getOrgName));
        isInited = true;
    }

    private Map<Long, String> buildDcNameMap() {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if(xpipeMeta == null) {
            return null;
        }
        Map<String, DcMeta> allDcs = xpipeMeta.getDcs();
        Map<Long, String> result = new HashMap<>();
        allDcs.values().forEach(dc -> result.put(dc.getDcId(), dc.getId()));
        return result;
    }

    @Override
    protected void doAction() {
        if(!isInited) {
            init();
        }
        EventMonitor.DEFAULT.logEvent(REPORT_EVENT, "begin");
        List<MigrationClusterTbl> latestMigrationClusters = migrationService.getLatestMigrationClusters(DEFAULT_STATISTICAL_INTERVAL);
        if (latestMigrationClusters == null || latestMigrationClusters.size() == 0) return;

        List<MigrationResultModel> migrationResult = new ArrayList<>();
        latestMigrationClusters.forEach(migrationClusterTbl -> {
            MigrationResultModel model = new MigrationResultModel().setCategory(DEFAULT_CATEGORY)
                    .setTool(DEFAULT_TOOL).setSubject(DEFAULT_SUBJECT).setOperator(migrationClusterTbl.getMigrationEvent().getOperator())
                    .setTimestamp(DateTimeUtils.timeAsString(migrationClusterTbl.getStartTime(), DEFAULT_TIME_FORMAT))
                    .setClusterName(migrationClusterTbl.getCluster().getClusterName())
                    .setBu(orgIdNameMap.get(migrationClusterTbl.getCluster().getClusterOrgId()))
                    .setUrl(String.format(DEFAULT_MIGRATION_DETAIL_URL, consoleConfig.getConsoleDomain(), migrationClusterTbl.getMigrationEventId()))
                    .setOriginMasterDc(dcIdNameMap.get(migrationClusterTbl.getSourceDcId()))
                    .setTargetMasterDc(dcIdNameMap.get(migrationClusterTbl.getDestinationDcId()))
                    .setTaskId(migrationClusterTbl.getId());
            model.setResult(DO_STATUS.fromMigrationStatus(MigrationStatus.valueOf(migrationClusterTbl.getStatus())).toString().toUpperCase());

            model.setDescription(DEFAULT_SUBJECT).setMetaData(DEFAULT_META_DATA);
            migrationResult.add(model);
        });

        MigrationResultReportModel migrationResultReportModel = new MigrationResultReportModel().setRequest_body(migrationResult)
                                                .setAccess_token(migrationReporterConfig.getKeyMigrationResultReportToken());

        logger.debug("[DefaultMigrationResultReporter] report to noc {}", JsonCodec.DEFAULT.encode(migrationResultReportModel));
        ResponseEntity<NocReportResponseModel> response
                = httpService.getRestTemplate().postForEntity(migrationReporterConfig.getKeyMigrationResultReportUrl(), migrationResultReportModel, NocReportResponseModel.class);
        if (response != null && response.getBody() != null && response.getBody().getCode() != 200) {
            logger.warn("[reportToNoc] send migration result fail! migration result: {}, result:{}", migrationResult, response.getBody());
        }
    }

    @Override
    protected boolean shouldDoAction() {
        logger.debug("[DefaultMigrationResultReporter]get switch {}", migrationReporterConfig.isMigrationResultReportOpen());
        return migrationReporterConfig.isMigrationResultReportOpen() && super.shouldDoAction();
    }

    @Override
    protected long getIntervalMilli() {
        return migrationReporterConfig.getMigrationProcessReportIntervalMill();
    }

    @Override
    protected long getLeastIntervalMilli() {
        return migrationReporterConfig.getMigrationProcessReportIntervalMill();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Lists.newArrayList();
    }
}
