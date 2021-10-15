package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisinfo;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.BiDirectionSupport;
import com.ctrip.xpipe.redis.checker.healthcheck.OneWaySupport;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction.CurrentDcCheckController;
import org.springframework.stereotype.Component;

/**
 * @author Slight
 * <p>
 * Jun 01, 2021 4:28 PM
 */
@Component
public class CurrentDcInfoController extends CurrentDcCheckController implements InfoActionController, BiDirectionSupport, OneWaySupport {
    public CurrentDcInfoController(FoundationService foundationService) {
        super(foundationService.getDataCenter());
    }
}
