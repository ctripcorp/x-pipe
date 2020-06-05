package com.ctrip.xpipe.redis.console.healthcheck.nonredis.monitor;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.ConsoleDbConfig;
import com.ctrip.xpipe.redis.console.redis.SentinelManager;
import com.ctrip.xpipe.redis.core.entity.SentinelMeta;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.IntStream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Feb 23, 2018
 */
public class DefaultSentinelMonitorsCheckTest {

    @InjectMocks
    private DefaultSentinelMonitorsCheckCrossDc checker = new DefaultSentinelMonitorsCheckCrossDc();

    @Mock
    private AlertManager alertManager;

    @Mock
    private SentinelManager sentinelManager;

    @Mock
    private ConsoleDbConfig consoleDbConfig;

    private final String[] monitorNames = new String[] {
            "xpipe-auto-build-69-shard-2",
            "xpipe-auto-build-48-shard-2",
            "xpipe-auto-build-84-shard-3",
            "xpipe-auto-build-79-shard-2",
            "xpipe-auto-build-92-shard-1"
    };

    private final String sentinelMastersHeader = "sentinel_masters:82\n" +
            "sentinel_tilt:0\n" +
            "sentinel_running_scripts:0\n" +
            "sentinel_scripts_queue_length:0";

    @Before
    public void beforeDefaultSentinelMonitorsCheckTest() {
        MockitoAnnotations.initMocks(this);
        StringBuilder sb = new StringBuilder(sentinelMastersHeader);
        IntStream.range(0, monitorNames.length).forEach(i -> {
            sb.append(String.format("\nmaster%d:name=%s,status=ok,address=127.0.0.1:%d,slaves=2,sentinels=5", i, monitorNames[i], 6379+i));
        });

        when(sentinelManager.infoSentinel(any())).thenReturn(sb.toString());
        when(alertManager.shouldAlert(any())).thenReturn(true);
        when(consoleDbConfig.isSentinelAutoProcess()).thenReturn(true);
    }

    @Test
    public void checkSentinel() {
        checker.checkSentinel(new SentinelMeta().setAddress("127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002"),
                new HostPort("127.0.0.1", 5000));
        verify(alertManager, times(monitorNames.length)).alert(eq(null), eq(null), eq(null), eq(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS), anyString());
        verify(sentinelManager, times(monitorNames.length)).removeSentinelMonitor(any(), any());
    }

    @Test
    public void checkSentinel2() {
        checker.setMonitorNames(new HashSet<>(Arrays.asList(monitorNames)));
        checker.checkSentinel(new SentinelMeta().setAddress("127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002"),
                new HostPort("127.0.0.1", 5000));
        verify(alertManager, never()).alert(eq(null), eq(null), eq(null), eq(ALERT_TYPE.SENTINEL_MONITOR_INCONSIS), anyString());
        verify(sentinelManager, never()).removeSentinelMonitor(any(), any());
    }

}