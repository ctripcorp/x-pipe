package com.ctrip.xpipe.redis.console.healthcheck.actions.redismaster;

import com.ctrip.xpipe.redis.console.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.console.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.stereotype.Component;

@Component
public class CurrentDcRedisMasterController extends CurrentDcCheckController implements RedisMasterController, BiDirectionSupport {

}
