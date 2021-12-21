package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.AbstractCheckerTest;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

/**
 * @author lishanglin
 * date 2021/11/19
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class RedisWrongSlaveMonitorTest extends AbstractCheckerTest {

    @InjectMocks
    private RedisWrongSlaveMonitor wrongSlaveMonitor;

    @Mock
    private MetaCache metaCache;

    @Mock
    private AlertManager alertManager;

    @Mock
    private PingService pingService;

    private int masterPort = 6379;

    private int slavePort = 7379;

    @Mock
    private MasterRole masterRole;

    @Mock
    private SlaveRole slaveRole;

    @Before
    public void setupRedisWrongSlaveMonitorTest() {
        when(slaveRole.getServerRole()).thenReturn(Server.SERVER_ROLE.SLAVE);
        when(masterRole.getServerRole()).thenReturn(Server.SERVER_ROLE.MASTER);
        when(masterRole.getSlaveHostPorts()).thenReturn(Collections.singletonList(new HostPort("127.0.0.1", slavePort)));
        when(metaCache.getRedisOfDcClusterShard(anyString(), anyString(), anyString()))
                .thenReturn(Arrays.asList(new RedisMeta().setIp("127.0.0.1").setPort(masterPort),
                        new RedisMeta().setIp("127.0.0.1").setPort(slavePort).setMaster("127.0.0.1:"+masterPort)));
        when(pingService.isRedisAlive(any())).thenReturn(true);
    }

    @Test
    public void testWrongSlave_doAlert() throws Exception {
        when(masterRole.getSlaveHostPorts()).thenReturn(Collections.emptyList());
        wrongSlaveMonitor.onAction(mockRoleContext(masterRole));
        Mockito.verify(alertManager)
                .alert(anyString(), anyString(), anyString(),
                        eq(new HostPort("127.0.0.1", slavePort)),
                        eq(ALERT_TYPE.REPL_WRONG_SLAVE), anyString());
    }

    @Test
    public void testSlaveDown_doNothing() throws Exception {
        when(pingService.isRedisAlive(any())).thenReturn(false);
        when(masterRole.getSlaveHostPorts()).thenReturn(Collections.emptyList());
        wrongSlaveMonitor.onAction(mockRoleContext(masterRole));
        Mockito.verify(alertManager, never()).alert(anyString(), anyString(), anyString(), any(), any(), anyString());
    }

    @Test
    public void testSlaveRole_doNothing() throws Exception {
        when(masterRole.getSlaveHostPorts()).thenReturn(Collections.emptyList());
        wrongSlaveMonitor.onAction(mockRoleContext(slaveRole));
        Mockito.verify(alertManager, never()).alert(anyString(), anyString(), anyString(), any(), any(), anyString());
    }

    @Test
    public void testRoleFail_doNothing() throws Exception {
        wrongSlaveMonitor.onAction(mockFailRoleContext());
        Mockito.verify(alertManager, never()).alert(anyString(), anyString(), anyString(), any(), any(), anyString());
    }

    @Test
    public void testSlaveAllRight_doNothing() throws Exception {
        wrongSlaveMonitor.onAction(mockRoleContext(masterRole));
        Mockito.verify(alertManager, never()).alert(anyString(), anyString(), anyString(), any(), any(), anyString());
    }

    private RedisMasterActionContext mockFailRoleContext() throws Exception {
        return new RedisMasterActionContext(newRandomRedisHealthCheckInstance(masterPort), new Throwable("do role fail"));
    }

    private RedisMasterActionContext mockRoleContext(Role role) throws Exception {
        return new RedisMasterActionContext(newRandomRedisHealthCheckInstance(masterPort), role);
    }

}
