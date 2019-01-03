package com.ctrip.xpipe.redis.console.alert.policy.timing;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public class RecoveryTimeSlotControl implements TimeSlotControl {

    private static final Logger logger = LoggerFactory.getLogger(RecoveryTimeSlotControl.class);

    private ConsoleConfig consoleConfig;

    private RecoveryTimeAlgorithm calculator = new NaiveRecoveryTimeAlgorithm();

    private Map<ALERT_TYPE, LongSupplier> checkIntervals = Maps.newHashMap();

    public RecoveryTimeSlotControl(ConsoleConfig consoleConfig) {
        this.consoleConfig = consoleConfig;
    }

    @Override
    public long durationMilli(AlertEntity alert) {
        ALERT_TYPE type = alert.getAlertType();
        if(checkIntervals.containsKey(alert.getAlertType())) {
            long checkInterval = checkIntervals.get(type).getAsLong();
            return calculator.calculate(checkInterval);
        } else {
            return TimeUnit.MINUTES.toMillis(consoleConfig.getAlertSystemRecoverMinute());
        }
    }

    @Override
    public void mark(ALERT_TYPE alertType, LongSupplier checkInterval) {
        logger.info("[mark]{}, {}ms", alertType, checkInterval.getAsLong());
        checkIntervals.put(alertType, checkInterval);
    }

    @Override
    public boolean supports(Class<? extends AlertPolicy> clazz) {
        return clazz.isAssignableFrom(RecoveryTimeSlotControl.class);
    }
}
