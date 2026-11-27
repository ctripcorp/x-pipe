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

    /** @return false if the item could not be handed to the producer (not ready or send threw) */
    boolean sendKafka(GtidKeyItem gtidKeyItem);

    /** @return false if the item could not be handed to the producer (not ready or send threw) */
    boolean sendTimestampKafka(GtidTimestampItem gtidTimestampItem);

    void startProducer();

    void forceStopProducer();

    /** key topic producer pool initialized */
    boolean initSuccess();

    /** timestamp topic producer initialized */
    boolean timestampInitSuccess();
}
