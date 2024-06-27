package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationEventTbl;
import com.ctrip.xpipe.redis.console.model.OrganizationTbl;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.OrganizationService;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.util.*;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMigrationResultReporterTest {

    @InjectMocks
    DefaultMigrationResultReporter migrationResultReporter;

    @Mock
    private DcService dcService;

    @Mock
    private MigrationService migrationService;

    @Mock
    private OrganizationService organizationService;

    @Mock
    RestOperations restTemplate;

    @Mock
    DefaultHttpService httpService;

    @Mock
    protected ConsoleConfig consoleConfig;

    @Mock
    protected MigrationReporterConfig migrationReporterConfig;

    @Captor
    ArgumentCaptor<MigrationResultReportModel> migrationResultReportModelArgumentCaptor;


    @Before
    public void before() throws NoSuchFieldException, IllegalAccessException {
        Map<Long, String> dcIdNameMap = new HashMap<>();
        dcIdNameMap.put(1L, "jq");
        dcIdNameMap.put(2L, "oy");
        dcIdNameMap.put(3L, "fra");
        Mockito.when(dcService.dcNameMap()).thenReturn(dcIdNameMap);

        List<OrganizationTbl> organizationTbls = new ArrayList<>();
        organizationTbls.add(new OrganizationTbl().setId(11L).setOrgName("org-1"));
        organizationTbls.add(new OrganizationTbl().setId(12L).setOrgName("org-2"));
        organizationTbls.add(new OrganizationTbl().setId(13L).setOrgName("org-3"));
        organizationTbls.add(new OrganizationTbl().setId(14L).setOrgName("org-4"));
        organizationTbls.add(new OrganizationTbl().setId(15L).setOrgName("org-5"));
        Mockito.when(organizationService.getAllOrganizations()).thenReturn(organizationTbls);

        List<MigrationClusterTbl> migrationClusterTbls = new ArrayList<>();
        Date startTime = new Date();
        Date endTime = DateTimeUtils.getSecondsLaterThan(startTime, 10);
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster1").setClusterOrgId(11L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(113).setId(1).setStatus("Initiated").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster2").setClusterOrgId(12L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(113).setId(2).setStatus("Checking").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster3").setClusterOrgId(13L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(113).setId(3).setStatus("CheckingFail").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster4").setClusterOrgId(14L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(114).setId(4).setStatus("Migrating").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster5").setClusterOrgId(15L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(114).setId(5).setStatus("PartialSuccess").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpie")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster7").setClusterOrgId(15L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(114).setId(7).setStatus("Publish").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster8").setClusterOrgId(15L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(114).setId(8).setStatus("PublishFail").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster9").setClusterOrgId(15L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(114).setId(9).setStatus("RollBack").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster11").setClusterOrgId(15L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(114).setId(11).setStatus("Aborted").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster12").setClusterOrgId(15L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(114).setId(12).setStatus("Success").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        migrationClusterTbls.add(new MigrationClusterTbl().setCluster(new ClusterTbl().setClusterName("cluster13").setClusterOrgId(15L)).setOperator("xpipe").setSourceDcId(1).setDestinationDcId(2).setMigrationEventId(114).setId(13).setStatus("ForceEnd").setStartTime(startTime).setEndTime(endTime).setMigrationEvent(new MigrationEventTbl().setOperator("xpipe")));
        Mockito.when(migrationService.getLatestMigrationClusters(Mockito.anyInt())).thenReturn(migrationClusterTbls);

        Mockito.when(migrationReporterConfig.getKeyMigrationResultReportUrl()).thenReturn("127.0.0.1:8080");
        Mockito.when(migrationReporterConfig.getKeyMigrationResultReportToken()).thenReturn("bbbb");
        Mockito.when(consoleConfig.getConsoleDomain()).thenReturn("127.0.0.1:8080");


        Mockito.when(httpService.getRestTemplate()).thenReturn(restTemplate);
        Mockito.when(restTemplate.postForEntity(Mockito.anyString(),
                migrationResultReportModelArgumentCaptor.capture(), Mockito.eq(NocReportResponseModel.class)))
                .thenReturn(new ResponseEntity<NocReportResponseModel>(new NocReportResponseModel().setCode(200), HttpStatus.OK));
    }

    @Test
    public void testReport() {
        migrationResultReporter.init();
        migrationResultReporter.doAction();
        Mockito.verify(restTemplate, Mockito.times(1)).postForEntity(Mockito.anyString(), migrationResultReportModelArgumentCaptor.capture(), Mockito.eq(NocReportResponseModel.class));
        MigrationResultReportModel value = migrationResultReportModelArgumentCaptor.getValue();

        Assert.assertEquals(11, value.getRequest_body().size());
        Mockito.when(migrationService.getLatestMigrationClusters(Mockito.anyInt())).thenReturn(new ArrayList<>());
        Mockito.verify(restTemplate, Mockito.times(1)).postForEntity(Mockito.anyString(), migrationResultReportModelArgumentCaptor.capture(), Mockito.eq(NocReportResponseModel.class));

    }

}
