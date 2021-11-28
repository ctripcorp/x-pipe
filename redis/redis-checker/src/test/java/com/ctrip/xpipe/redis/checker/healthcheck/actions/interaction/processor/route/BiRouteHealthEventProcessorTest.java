package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.processor.route;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

/**
 * @author Slight
 * <p>
 * Nov 28, 2021 7:32 PM
 */
public class BiRouteHealthEventProcessorTest {

    private BiRouteHealthEventProcessor processor = new BiRouteHealthEventProcessor(null, null) {
        @Override
        protected long getHoldingMillis() {
            return 100;
        }
    };

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        processor = spy(processor);
    }

    @Test
    public void testFullSyncJudgement() throws InterruptedException, ExecutionException, TimeoutException {
        RedisHealthCheckInstance instance = mock(RedisHealthCheckInstance.class);
        RedisSession redisSession = mock(RedisSession.class);
        doReturn(redisSession).when(instance).getRedisSession();
        InfoResultExtractor info = mock(InfoResultExtractor.class);
        doReturn("127.0.0.1").when(info).extract("peer0_host");
        doReturn(6379).when(info).extractAsInteger("peer0_port");
        doReturn(0).when(info).extractAsInteger("peer0_sync_in_progress");

        doReturn("127.0.0.2").when(info).extract("peer1_host");
        doReturn(6380).when(info).extractAsInteger("peer1_port");
        doReturn(1).when(info).extractAsInteger("peer1_sync_in_progress");

        doReturn(info).when(redisSession).syncCRDTInfo(any());

        assertFalse(processor.isRedisInFullSyncTo(instance, new HostPort("127.0.0.1", 6379)));
        assertFalse(processor.isRedisInFullSyncTo(instance, new HostPort("127.0.0.10", 6379)));
        assertTrue(processor.isRedisInFullSyncTo(instance, new HostPort("127.0.0.2", 6380)));
    }
}