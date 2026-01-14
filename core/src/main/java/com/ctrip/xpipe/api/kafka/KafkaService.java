package com.ctrip.xpipe.api.kafka;


import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author TB
 * <p>
 * 2025/10/23 14:11
 */
public interface KafkaService extends Ordered {
    KafkaService DEFAULT = ServicesUtil.getKafkaService();

    void sendKafka(GtidKeyItem gtidKeyItem);

    void startProducer();

    void forceStopProducer();
}
