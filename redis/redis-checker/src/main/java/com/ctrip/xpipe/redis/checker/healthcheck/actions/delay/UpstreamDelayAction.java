package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class UpstreamDelayAction extends DelayAction {

    public UpstreamDelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors, PingService pingService, FoundationService foundationService) {
        super(scheduled, instance, executors, pingService, foundationService);
    }

    protected String[] getSubscribeChannel() {
        return instance.getCheckInfo().getActiveDcAllShardIds().stream().map(shardId -> "xpipe-health-check-" + foundationService.getLocalIp() + "-" + shardId).toArray(String[]::new);
    }

    @Override
    protected void onMessage(String channel, String message) {
        if (!getLifecycleState().isStarted()) {
            return;
        }
        long currentTime = System.nanoTime();
        long lastDelayPubTimeNano = Long.parseLong(message, 16);

        String[] channelParts = channel.split("-");
        Long shardId = Long.parseLong(channelParts[channelParts.length - 1]);
        if (context.get().equals(INIT_CONTEXT)) {
            this.context.set(new DelayActionContext(instance, shardId, currentTime - lastDelayPubTimeNano));
        } else {
            context.get().addUpstreamShardDelay(shardId, currentTime - lastDelayPubTimeNano);
        }
    }

    @Override
    protected void notifyDelay() {
        if (INIT_CONTEXT.equals(context.get())) {
            // no receive any messages but not expire just on init time
            logger.info("[expire][{}] init but not expire", instance.getCheckInfo().getHostPort());
            return;
        }
        notifyListeners(context.get());
    }

}
