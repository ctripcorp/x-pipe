package com.ctrip.xpipe.redis.console.reporter;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.util.Collections;
import java.util.HashSet;


@RunWith(MockitoJUnitRunner.class)
public class DefaultMigrationProcessReporterTest {

    @InjectMocks
    DefaultMigrationProcessReporter migrationReporter;

    @Captor
    ArgumentCaptor<MigrationProcessReportModel> migrationProcessReportModelArgumentCaptor;

    @Captor
    ArgumentCaptor<NocReportResponseModel> responseModelArgumentCaptor;

    @Mock
    RestOperations restTemplate;

    @Mock
    DefaultHttpService httpService;

    @Mock
    protected ConsoleConfig consoleConfig;

    @Mock
    protected MigrationReporterConfig migrationReporterConfig;

    @Mock
    private MetaCache metaCache;

    @Before
    public void before() {
        Mockito.when(migrationReporterConfig.getKeyMigrationProcessReportUrl()).thenReturn("127.0.0.1:8080");
        Mockito.when(migrationReporterConfig.getBreakDownDc()).thenReturn(new HashSet<>(Collections.singleton("jq")));
        Mockito.when(httpService.getRestTemplate()).thenReturn(restTemplate);
        Mockito.when(restTemplate.postForEntity(Mockito.anyString(),
                migrationProcessReportModelArgumentCaptor.capture(), Mockito.eq(NocReportResponseModel.class)))
                .thenReturn(new ResponseEntity<NocReportResponseModel>(new NocReportResponseModel().setCode(200), HttpStatus.OK));
        DcTbl dcTbl = new DcTbl();
        dcTbl.setDcName("jq");
    }

    @Test
    public void testReportSuccess() {
        Mockito.when(metaCache.getMigratableClustersCountByActiveDc(Mockito.anyString())).thenReturn(2000);
        migrationReporter.doAction();
        Mockito.verify(restTemplate, Mockito.times(1))
                .postForEntity(Mockito.anyString(),
                        migrationProcessReportModelArgumentCaptor.capture(), Mockito.eq(NocReportResponseModel.class));
        MigrationProcessReportModel value = migrationProcessReportModelArgumentCaptor.getValue();
        Assert.assertEquals(0, value.getProcess());
        Assert.assertEquals(2000, value.getObjectCount());
        Assert.assertEquals("redis", value.getService());

        Mockito.when(metaCache.getMigratableClustersCountByActiveDc(Mockito.anyString())).thenReturn(2002);
        migrationReporter.doAction();
        Mockito.verify(restTemplate, Mockito.times(2))
                .postForEntity(Mockito.anyString(),
                        migrationProcessReportModelArgumentCaptor.capture(), Mockito.eq(NocReportResponseModel.class));
        value = migrationProcessReportModelArgumentCaptor.getValue();
        Assert.assertEquals(0, value.getProcess());
        Assert.assertEquals(2002, value.getObjectCount());

        Mockito.when(metaCache.getMigratableClustersCountByActiveDc(Mockito.anyString())).thenReturn(800);
        migrationReporter.doAction();
        Mockito.verify(restTemplate, Mockito.times(3))
                .postForEntity(Mockito.anyString(),
                        migrationProcessReportModelArgumentCaptor.capture(), Mockito.eq(NocReportResponseModel.class));
        value = migrationProcessReportModelArgumentCaptor.getValue();
        Assert.assertEquals(60, value.getProcess());
        Assert.assertEquals(2002, value.getObjectCount());
    }
}