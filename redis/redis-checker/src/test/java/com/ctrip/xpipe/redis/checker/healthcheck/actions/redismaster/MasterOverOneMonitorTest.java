package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.simpleserver.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/11/19
 */
@RunWith(MockitoJUnitRunner.class)
public class MasterOverOneMonitorTest extends AbstractCheckerTest {

    private static final String ROLE_MASTER = "*3\r\n" + "$6\r\n" + "master\r\n" + ":0\r\n" + "*0\r\n";

    private static final String ROLE_SLAVE = "*5\r\n" + "$5\r\n" + "slave\r\n" + "$9\r\n" +
            "127.0.0.1\r\n" + ":6379\r\n" + "$9\r\n" + "connected\r\n" + ":14\r\n";

    @InjectMocks
    private MasterOverOneMonitor masterOverOneMonitor;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private MasterRole masterRole;

    private Server redis1;

    private Server redis2;

    private volatile boolean redis1Master;

    private volatile boolean redis2Master;

    private RedisMeta redis1Meta;

    private RedisMeta redis2Meta;

    @Before
    public void setupMasterOverOneMonitorTest() throws Exception {
        masterOverOneMonitor.setScheduled(scheduled);
        masterOverOneMonitor.setKeyedObjectPool(getXpipeNettyClientKeyedObjectPool());
        redis1Master = true;
        redis2Master = false;
        redis1 = startServer(randomPort(), () -> redis1Master ? ROLE_MASTER : ROLE_SLAVE);
        redis2 = startServer(randomPort(), () -> redis2Master ? ROLE_MASTER : ROLE_SLAVE);

        redis1Meta = new RedisMeta().setIp("127.0.0.1").setPort(redis1.getPort());
        redis2Meta = new RedisMeta().setIp("127.0.0.1").setPort(redis2.getPort()).setMaster("127.0.0.1:"+redis1.getPort());
        when(metaCache.getRedisOfDcClusterShard(anyString(), anyString(), anyString())).thenReturn(Arrays.asList(
                redis1Meta, redis2Meta
        ));
        when(masterRole.getServerRole()).thenReturn(com.ctrip.xpipe.api.server.Server.SERVER_ROLE.MASTER);
    }

    @After
    public void afterMasterOverOneMonitorTest() throws Exception {
        if (null != redis1) redis1.stop();
        if (null != redis2) redis2.stop();
        redis1 = redis2 = null;
    }

    @Test
    public void testTwoMaster_doAlert() throws Exception {
        redis2Meta.setMaster(null);
        redis1Master = true;
        redis2Master = true;
        masterOverOneMonitor.onAction(mockMasterRoleContext(redis1.getPort()));
        waitConditionUntilTimeOut(() ->
                1 == mockingDetails(alertManager).getInvocations()
                        .stream().filter(inv -> inv.getMethod().getName().equals("alert"))
                        .count(), 1000);
    }

    @Test(expected = TimeoutException.class)
    public void testOldMasterDown_doNothing() throws Exception {
        redis2Meta.setMaster(null);
        redis1Master = true;
        redis2.stop();
        redis2 = null;
        masterOverOneMonitor.onAction(mockMasterRoleContext(redis1.getPort()));
        waitConditionUntilTimeOut(() ->
                1 == mockingDetails(alertManager).getInvocations()
                        .stream().filter(inv -> inv.getMethod().getName().equals("alert"))
                        .count(), 1000);
    }

    @Test(expected = TimeoutException.class)
    public void testOnlyMaster_doNothing() throws Exception {
        masterOverOneMonitor.onAction(mockMasterRoleContext(redis1.getPort()));
        waitConditionUntilTimeOut(() ->
                1 == mockingDetails(alertManager).getInvocations()
                        .stream().filter(inv -> inv.getMethod().getName().equals("alert"))
                        .count(), 1000);
    }

    @Test(expected = TimeoutException.class)
    public void testMasterSwitch_doNothing() throws Exception {
        redis2Master = false;
        redis2Meta.setMaster(null);
        masterOverOneMonitor.onAction(mockMasterRoleContext(redis1.getPort()));
        waitConditionUntilTimeOut(() ->
                1 == mockingDetails(alertManager).getInvocations()
                        .stream().filter(inv -> inv.getMethod().getName().equals("alert"))
                        .count(), 1000);
    }

    private RedisMasterActionContext mockFailRoleContext(int port) throws Exception {
        return new RedisMasterActionContext(newRandomRedisHealthCheckInstance(port), new Throwable("do role fail"));
    }

    private RedisMasterActionContext mockMasterRoleContext(int port) throws Exception {
        return new RedisMasterActionContext(newRandomRedisHealthCheckInstance(port), masterRole);
    }

}
