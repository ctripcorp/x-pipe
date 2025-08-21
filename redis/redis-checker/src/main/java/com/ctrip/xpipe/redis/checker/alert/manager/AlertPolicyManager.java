package com.ctrip.xpipe.redis.checker.alert.manager;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertChannel;
import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.alert.policy.channel.ChannelSelector;
import com.ctrip.xpipe.redis.checker.alert.policy.channel.DefaultChannelSelector;
import com.ctrip.xpipe.redis.checker.alert.policy.receiver.*;
import com.ctrip.xpipe.redis.checker.alert.policy.timing.RecoveryTimeSlotControl;
import com.ctrip.xpipe.redis.checker.alert.policy.timing.TimeSlotControl;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2017
 */
@Component
public class AlertPolicyManager {

    @Autowired
    private AlertConfig alertConfig;

    @Autowired
    private CheckerDbConfig checkerDbConfig;

    @Autowired
    private MetaCache metaCache;

    private EmailReceiver emailReceiver;

    private GroupEmailReceiver groupEmailReceiver;

    private ChannelSelector channelSelector;

    private TimeSlotControl recoveryTimeController;

    @PostConstruct
    public void initPolicies() {
        emailReceiver = new DefaultEmailReceiver(alertConfig, checkerDbConfig, metaCache);
        groupEmailReceiver = new DefaultGroupEmailReceiver(alertConfig, checkerDbConfig, metaCache);
        channelSelector = new DefaultChannelSelector();
        if(recoveryTimeController == null) {
            recoveryTimeController = new RecoveryTimeSlotControl(alertConfig);
        }
    }

    public List<AlertChannel> queryChannels(AlertEntity alert) {
        return channelSelector.alertChannels(alert);
    }

    public long queryRecoverMilli(AlertEntity alert) {
        return recoveryTimeController.durationMilli(alert);
    }

    public long querySuspendMilli(AlertEntity alert) {
        return TimeUnit.MINUTES.toMillis(alertConfig.getAlertSystemSuspendMinute());
    }

    public EmailReceiverModel queryEmailReceivers(AlertEntity alert) {
        return emailReceiver.receivers(alert);
    }

    public void markCheckInterval(ALERT_TYPE alertType, LongSupplier checkInterval) {
        if(recoveryTimeController == null) {
            recoveryTimeController = new RecoveryTimeSlotControl(alertConfig);
        }
        recoveryTimeController.mark(alertType, checkInterval);
    }

    public Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> queryGroupedEmailReceivers(
            AlertEntityHolderManager alerts) {

        return groupEmailReceiver.getGroupedEmailReceiver(alerts);
    }

}
