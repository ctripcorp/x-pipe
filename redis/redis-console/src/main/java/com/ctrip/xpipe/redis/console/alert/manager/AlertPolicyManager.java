package com.ctrip.xpipe.redis.console.alert.manager;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertChannel;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.channel.ChannelSelector;
import com.ctrip.xpipe.redis.console.alert.policy.channel.DefaultChannelSelector;
import com.ctrip.xpipe.redis.console.alert.policy.receiver.*;
import com.ctrip.xpipe.redis.console.alert.policy.timing.RecoveryTimeSlotControl;
import com.ctrip.xpipe.redis.console.alert.policy.timing.TimeSlotControl;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
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
    private ConsoleConfig consoleConfig;

    @Autowired
    private ConfigService configService;

    private EmailReceiver emailReceiver;

    private GroupEmailReceiver groupEmailReceiver;

    private ChannelSelector channelSelector;

    private TimeSlotControl recoveryTimeController;

    @PostConstruct
    public void initPolicies() {
        emailReceiver = new DefaultEmailReceiver(consoleConfig, configService);
        groupEmailReceiver = new DefaultGroupEmailReceiver(consoleConfig, configService);
        channelSelector = new DefaultChannelSelector();
        recoveryTimeController = new RecoveryTimeSlotControl(consoleConfig);
    }

    public List<AlertChannel> queryChannels(AlertEntity alert) {
        return channelSelector.alertChannels(alert);
    }

    public long queryRecoverMilli(AlertEntity alert) {
        return recoveryTimeController.durationMilli(alert);
    }

    public long querySuspendMilli(AlertEntity alert) {
        return TimeUnit.MINUTES.toMillis(consoleConfig.getAlertSystemSuspendMinute());
    }

    public EmailReceiverModel queryEmailReceivers(AlertEntity alert) {
        return emailReceiver.receivers(alert);
    }

    public void markCheckInterval(ALERT_TYPE alertType, LongSupplier checkInterval) {
        recoveryTimeController.mark(alertType, checkInterval);
    }

    public Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> queryGroupedEmailReceivers(
            Map<ALERT_TYPE, Set<AlertEntity>> alerts) {

        return groupEmailReceiver.getGroupedEmailReceiver(alerts);
    }

}
