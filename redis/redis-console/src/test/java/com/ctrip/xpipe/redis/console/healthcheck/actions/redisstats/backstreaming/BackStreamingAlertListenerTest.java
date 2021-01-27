package com.ctrip.xpipe.redis.console.healthcheck.actions.redisstats.backstreaming;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * @author lishanglin
 * date 2021/1/26
 */
@RunWith(MockitoJUnitRunner.class)
public class BackStreamingAlertListenerTest extends AbstractConsoleTest {

    @Mock
    private AlertManager alertManager;

    private BackStreamingAlertListener listener;

    private RedisHealthCheckInstance instance;

    @Before
    public void setupBackStreamingAlertListenerTest() throws Exception {
        listener = new BackStreamingAlertListener(alertManager);
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, randomPort());
    }

    @Test
    public void testAlertOnBackStream() {
        Mockito.doAnswer(invocation -> {
            ALERT_TYPE alert_type = invocation.getArgumentAt(1, ALERT_TYPE.class);
            Assert.assertEquals(ALERT_TYPE.CRDT_BACKSTREAMING, alert_type);
            return null;
        }).when(alertManager).alert(Mockito.any(), Mockito.any(), Mockito.anyString());

        BackStreamingContext context = new BackStreamingContext(instance, true);
        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);

        Mockito.verify(alertManager).alert(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testNoBackStream() {
        BackStreamingContext context = new BackStreamingContext(instance, false);
        listener.onAction(context);
        Mockito.verify(alertManager, Mockito.never()).alert(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testHandleNull() {
        BackStreamingContext context = new BackStreamingContext(instance, null);
        listener.onAction(context);
        Mockito.verify(alertManager, Mockito.never()).alert(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

}
