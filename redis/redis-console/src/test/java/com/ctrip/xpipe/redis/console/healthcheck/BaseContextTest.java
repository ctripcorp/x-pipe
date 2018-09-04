package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Sep 04, 2018
 */
public class BaseContextTest extends AbstractRedisTest {

    @Test
    public void testBaseContext() throws Exception {
        String expected = "expected";
        BaseContext context = new TestBaseContext(scheduled, new DefaultRedisHealthCheckInstance(), expected);
        context.initialize();
        context.start();
        Thread.sleep(60);
        logger.info("{}", ((TestBaseContext) context).getResult().get());
        Assert.assertEquals(expected, ((TestBaseContext) context).getResult().get());
    }

    class TestBaseContext extends BaseContext {

        private String target;

        private AtomicReference<String> result = new AtomicReference<>();

        public TestBaseContext(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, String target) {
            super(scheduled, instance);
            this.target = target;
        }

        @Override
        protected void doScheduledTask() {
            result.set(target);
        }

        @Override
        protected int getBaseCheckInterval() {
            return 10;
        }

        public AtomicReference<String> getResult() {
            return result;
        }

    }

}