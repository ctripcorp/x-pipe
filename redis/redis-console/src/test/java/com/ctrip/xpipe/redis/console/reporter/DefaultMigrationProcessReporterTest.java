package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.MigrationProgress;
import com.ctrip.xpipe.redis.console.service.migration.MigrationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.web.client.RestOperations;


@RunWith(MockitoJUnitRunner.class)
public class DefaultMigrationProcessReporterTest {

    @InjectMocks
    DefaultMigrationProcessReporter migrationReporter;

    @Mock
    MigrationService migrationService;

    @Captor
    ArgumentCaptor<MigrationProcessReportModel> migrationProcessReportModelArgumentCaptor;

    @Captor
    ArgumentCaptor<MigrationProcessReportResponseModel> responseModelArgumentCaptor;

    @Mock
    RestOperations restTemplate;

    @Mock
    private CrossDcClusterServer clusterServer;

    @Mock
    protected ConsoleConfig consoleConfig;

    @Before
    public void before() {
        Mockito.when(consoleConfig.getKeyMigrationProcessReportUrl()).thenReturn("127.0.0.1:8080");
        Mockito.when(restTemplate.postForEntity(Mockito.anyString(),
                migrationProcessReportModelArgumentCaptor.capture(), Mockito.eq(MigrationProcessReportResponseModel.class)))
                .thenReturn(null);
    }

    @Test
    public void testReportSuccess() {
        MigrationProgress migrationProgress = new MigrationProgress();
        migrationProgress.setSuccess(11);
        migrationProgress.setTotal(20);

        Mockito.when(migrationService.buildMigrationProgress(1)).thenReturn(migrationProgress);

        migrationReporter.doReport();
        Mockito.verify(restTemplate, Mockito.times(1))
                .postForEntity(Mockito.anyString(),
                        migrationProcessReportModelArgumentCaptor.capture(), Mockito.eq(MigrationProcessReportResponseModel.class));
        MigrationProcessReportModel value = migrationProcessReportModelArgumentCaptor.getValue();
        Assert.assertEquals(55, value.getProcess());
        Assert.assertEquals(20, value.getObjectCount());
        Assert.assertEquals("redis", value.getService());
    }
}