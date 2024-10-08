package com.ctrip.xpipe.redis.console.healthcheck.nonredis.metacache;

import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class MetaCacheCheckTest {

    private MetaCacheCheck metaCacheCheck;

    @Mock
    private AlertManager alertManager;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private MetaCache metaCache;

    @Before
    public void beforeMetaCacheCheckTest() {
        MockitoAnnotations.initMocks(this);
        metaCacheCheck = new MetaCacheCheck();
        metaCacheCheck.setAlertManager(alertManager);
        metaCacheCheck.setMetaCache(metaCache);
        metaCacheCheck.setConfig(consoleConfig);
    }

    @Test
    public void doCheck() {
        doNothing().when(alertManager).alert(any(), any(), any());
        when(metaCache.getLastUpdateTime()).thenReturn(System.currentTimeMillis());
        metaCacheCheck.doAction();
        verify(alertManager, never()).alert(any(), any(), any());
    }

    @Test
    public void doCheckWithAlert() {
        doNothing().when(alertManager).alert(any(), any(), any());
        when(metaCache.getLastUpdateTime()).thenReturn(System.currentTimeMillis() - 15 * 1000);
        metaCacheCheck.doAction();
        verify(alertManager, times(1)).alert(any(), any(), any(), any(), any());

        when(consoleConfig.disableDb()).thenReturn(true);
        metaCacheCheck.doAction();
        verify(alertManager, never()).alert(any(), any(), any());
    }
}