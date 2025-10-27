package com.ctrip.xpipe.kafka;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.KafkaService;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author TB
 * <p>
 * 2025/10/24 10:47
 */
public class DefaultKafkaService implements KafkaService {
    private static Logger logger = LoggerFactory.getLogger(DefaultFoundationService.class);

    @Override
    public void sendKafka(GtidKeyItem gtidKeyItem) {

    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
