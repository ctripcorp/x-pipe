package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RedisStatsCheckController extends CurrentDcCheckController {

    @Autowired
    public RedisStatsCheckController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }
}
