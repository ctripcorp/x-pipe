package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtinforeplication.listener;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.CrdtInfoReplicationContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.crdtInforeplication.listener.BackStreamingAlertListener;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackStreamingAlertListenerTest extends AbstractCheckerTest {
    @Mock
    private AlertManager alertManager;

    private BackStreamingAlertListener listener;

    private RedisHealthCheckInstance instance;

    @Before
    public void setupBackStreamingAlertListenerTest() throws Exception {
        listener = new BackStreamingAlertListener(alertManager);
        instance = newRandomRedisHealthCheckInstance(FoundationService.DEFAULT.getDataCenter(), ClusterType.BI_DIRECTION, randomPort());
        Mockito.doAnswer(invocation -> {
            ALERT_TYPE alert_type = invocation.getArgumentAt(1, ALERT_TYPE.class);
            Assert.assertEquals(ALERT_TYPE.CRDT_BACKSTREAMING, alert_type);
            return null;
        }).when(alertManager).alert(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    final String TMP_REPLICATION = "# CRDT Replication\r\n" +
            "ovc:1:0;2:0\r\n" +
            "gcvc:1:0;2:0\r\n" +
            "gid:1\r\n" +
            "backstreaming:%s\r\n";
    
    @Test
    public void testAlertOnBackStream() {
        String info = String.format(TMP_REPLICATION, "1");
        InfoResultExtractor executors = new InfoResultExtractor(info);
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, info);
        Assert.assertTrue(listener.worksfor(context));
        listener.onAction(context);

        Mockito.verify(alertManager).alert(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testNoBackStream() {
        String info = String.format(TMP_REPLICATION, "0");
        InfoResultExtractor executors = new InfoResultExtractor(info);
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, info);
        listener.onAction(context);
        Mockito.verify(alertManager, Mockito.never()).alert(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    @Test
    public void testHandleNull() {
        String info = "";
        CrdtInfoReplicationContext context = new CrdtInfoReplicationContext(instance, info);
        listener.onAction(context);
        Mockito.verify(alertManager, Mockito.never()).alert(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

}
