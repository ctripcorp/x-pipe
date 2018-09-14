package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ConfigService;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */


public class DefaultEmailReceiver extends AbstractEmailReceiver {

    public DefaultEmailReceiver(ConsoleConfig consoleConfig, ConfigService configService) {
        super(consoleConfig, configService);
    }

    @Override
    public EmailReceiverModel receivers(AlertEntity alert) {
        // If not urgent and alert system is off, send xpipe admins only
        if(!isAlertSystemOn() && !isUrgent(alert)) {
            return new EmailReceiverModel(getXPipeAdminEmails(), null);
        }

        // Retrieve corresponding paramas(Email DBA or Email XPipe Admin), according to alert type
        Set<EmailReceiverParam> params = getRelatedParams(alert);

        if(params == null || params.isEmpty()) {
            return new EmailReceiverModel(getXPipeAdminEmails(), null);
        }

        List<String> recipients = new LinkedList<>();
        for(EmailReceiverParam param : params) {
            recipients.addAll(param.param());
        }
        // make sure xpipe admin always findRedisHealthCheckInstance the alert email
        List<String> ccers = params.contains(xpipeAdminReceiver()) ? null : getXPipeAdminEmails();

        return new EmailReceiverModel(recipients, ccers);
    }

    private Set<EmailReceiverParam> getRelatedParams(AlertEntity alert) {
        Set<EmailReceiverParam> receiverParams = new HashSet<>();
        if(shouldAlertDBA(alert)) {
            receiverParams.add(dbaReceiver());
        }
        if(shouldAlertXpipeAdmin(alert)) {
            receiverParams.add(xpipeAdminReceiver());
        }
        return receiverParams;
    }

    private boolean isUrgent(AlertEntity alert) {
        return alert.getAlertType().urgent();
    }
}
