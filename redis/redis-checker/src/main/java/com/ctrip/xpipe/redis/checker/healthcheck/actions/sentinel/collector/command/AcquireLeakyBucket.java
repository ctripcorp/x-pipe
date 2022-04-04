package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelLeakyBucket;
import com.ctrip.xpipe.redis.core.exception.NoResourceException;

import java.util.concurrent.TimeUnit;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class AcquireLeakyBucket extends AbstractSentinelHelloCollectCommand {

    private SentinelLeakyBucket leakyBucket;

    public AcquireLeakyBucket(SentinelHelloCollectContext context, SentinelLeakyBucket leakyBucket) {
        super(context);
        this.leakyBucket = leakyBucket;
    }

    @Override
    protected void doExecute() throws Throwable {
        // add rate limit logic to reduce frequently sentinel operations
        if (!leakyBucket.tryAcquire()) {
            logger.warn("[{}-{}][acquire failed]", LOG_TITLE, context.getSentinelMonitorName());
            future().setFailure(new NoResourceException("leakyBucket.tryAcquire failed"));
        } else {
            // I got the lock, remember to release it
            leakyBucket.delayRelease(1000, TimeUnit.MILLISECONDS);
            future().setSuccess();
        }
    }

}
