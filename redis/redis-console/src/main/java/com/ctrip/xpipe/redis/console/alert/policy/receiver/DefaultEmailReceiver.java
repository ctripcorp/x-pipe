package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */


public class DefaultEmailReceiver extends AbstractEmailReceiver {

    public DefaultEmailReceiver(ConsoleConfig consoleConfig, ConfigService configService, ClusterService clusterService) {
        super(consoleConfig, configService, clusterService);
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

        Set<String> recipients = Sets.newHashSet();
        for(EmailReceiverParam param : params) {
            recipients.addAll(param.param(alert));
        }
        // make sure xpipe admin always findRedisHealthCheckInstance the alert email
        List<String> ccers = params.contains(xpipeAdminReceiver()) ? null : getXPipeAdminEmails();

        return new EmailReceiverModel(Lists.newArrayList(recipients), ccers);
    }

    private Set<EmailReceiverParam> getRelatedParams(AlertEntity alert) {
        Set<EmailReceiverParam> receiverParams = new HashSet<>();
        if(shouldAlertDBA(alert)) {
            receiverParams.add(dbaReceiver());
        }
        if(shouldAlertXpipeAdmin(alert)) {
            receiverParams.add(xpipeAdminReceiver());
        }
        if(shouldAlertClusterAdmin(alert)) {
            receiverParams.add(clusterAdminReceiver());
        }
        return receiverParams;
    }

    private boolean isUrgent(AlertEntity alert) {
        return alert.getAlertType().urgent();
    }
}
