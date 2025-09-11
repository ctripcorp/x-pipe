package com.ctrip.xpipe.redis.checker.healthcheck.actions.ping;

import com.ctrip.xpipe.redis.checker.healthcheck.session.PingCallback;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author chen.zhu
 *         <p>
 *         Sep 13, 2018
 */
public class PingActionTest extends AbstractRedisTest {

    private PingAction action;

    private RedisSession session;

    @Before
    public void beforePingActionTest() {
        RedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();
        session = mock(RedisSession.class);
        ((DefaultRedisHealthCheckInstance) instance).setSession(session);
        action = new PingAction(scheduled, instance, executors);
    }

    @Test
    public void testDoScheduledTask() throws TimeoutException {
        when(session.ping(any(PingCallback.class))).thenAnswer(new Answer<PingCallback>() {
            @Override
            public PingCallback answer(InvocationOnMock invocation) throws Throwable {
                PingCallback callback = invocation.getArgument(0, PingCallback.class);
                callback.pong("PONG");
                return null;
            }
        });
        AtomicBoolean result = new AtomicBoolean(false);
        action.addListener(new HealthCheckActionListener() {
            @Override
            public void onAction(ActionContext context) {
                PingActionContext pingActionContext = (PingActionContext) context;
                logger.info("[context] {}: {}",
                        DateTimeUtils.timeAsString(context.getRecvTimeMilli()),
                        context.getResult());
                result.set(pingActionContext.getResult());
            }

            @Override
            public boolean worksfor(ActionContext t) {
                return t instanceof PingActionContext;
            }

            @Override
            public void stopWatch(HealthCheckAction action) {

            }
        });
        action.doTask();
        waitConditionUntilTimeOut(() -> result.get(), 1000);
    }
}