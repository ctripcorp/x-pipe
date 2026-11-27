package com.ctrip.xpipe.kafka;

import com.ctrip.xpipe.api.kafka.GtidKeyItem;
import com.ctrip.xpipe.api.kafka.GtidTimestampItem;
import com.ctrip.xpipe.api.kafka.KafkaService;

/**
 * @author TB
 * <p>
 * 2025/10/24 10:47
 */
public class DefaultKafkaService implements KafkaService {

    @Override
    public boolean sendKafka(GtidKeyItem gtidKeyItem) {
        return true;
    }

    @Override
    public boolean sendTimestampKafka(GtidTimestampItem gtidTimestampItem) {
        return true;
    }

    @Override
    public void startProducer() {

    }

    @Override
    public void forceStopProducer() {

    }

    @Override
    public boolean initSuccess() {
        return true;
    }

    @Override
    public boolean timestampInitSuccess() {
        return true;
    }

    @Override
    public int getOrder() {
        return LOWEST_PRECEDENCE;
    }
}
