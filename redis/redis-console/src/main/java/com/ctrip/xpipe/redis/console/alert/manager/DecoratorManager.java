package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.tuple.Pair;
import org.springframework.stereotype.Component;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class DecoratorManager {

    public Pair<String, String> generateTitleAndContent(AlertEntity alert, boolean isAlert) {
        return null;
    }
}
