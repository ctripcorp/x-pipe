package com.ctrip.xpipe.redis.checker.healthcheck.actions.redismaster;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class CurrentDcRedisMasterController extends CurrentDcCheckController implements RedisMasterController, BiDirectionSupport {

    @Autowired
    public CurrentDcRedisMasterController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }
}
