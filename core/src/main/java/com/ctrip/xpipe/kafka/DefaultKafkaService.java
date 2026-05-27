package com.ctrip.xpipe.kafka;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.KafkaService;

/**
 * @author TB
 * <p>
 * 2025/10/24 10:47
 */
public class DefaultKafkaService implements KafkaService {

    @Override
    public void sendKafka(GtidKeyItem gtidKeyItem) {

    }

    @Override
    public void startProducer() {

    }

    @Override
    public void forceStopProducer() {

    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
