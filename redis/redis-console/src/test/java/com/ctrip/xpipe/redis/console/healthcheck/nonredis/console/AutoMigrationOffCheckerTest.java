package com.ctrip.xpipe.redis.console.healthcheck.nonredis.console;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static com.ctrip.xpipe.redis.console.service.ConfigService.KEY_ALLOW_AUTO_MIGRATION;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/5/13
 */
@RunWith(MockitoJUnitRunner.class)
public class AutoMigrationOffCheckerTest extends AbstractConsoleTest {

    @InjectMocks
    private AutoMigrationOffChecker checker;

    @Mock
    private ConfigService configService;

    @Mock
    private CrossDcClusterServer clusterServer;

    @Mock
    private AlertManager alertManager;

    @Test
    public void testStop() {
        when(configService.allowAutoMigration()).thenReturn(true);
        Assert.assertTrue(checker.stop());

        when(configService.allowAutoMigration()).thenReturn(false);
        Assert.assertFalse(checker.stop());
    }

    @Test
    public void testAlert() {
        checker.alert();
        Mockito.verify(alertManager, times(1))
                .alert(anyString(), anyString(), any(), eq(ALERT_TYPE.AUTO_MIGRATION_NOT_ALLOW), anyString());
    }

}
