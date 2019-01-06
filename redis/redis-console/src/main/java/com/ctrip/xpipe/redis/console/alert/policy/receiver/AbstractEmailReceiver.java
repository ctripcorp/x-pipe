package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.message.AlertEntityHolder;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.ConfigService;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public abstract class AbstractEmailReceiver implements EmailReceiver {

    private ConfigService configService;

    private EmailReceiverParam.DbaReceiver dbaReceiver;

    private EmailReceiverParam.XPipeAdminReceiver xpipeAdminReceiver;

    private EmailReceiverParam.ClusterAdminReceiver clusterAdminReceiver;

    public AbstractEmailReceiver(ConsoleConfig consoleConfig, ConfigService configService, ClusterService clusterService) {
        this.configService = configService;
        this.dbaReceiver = new EmailReceiverParam.DbaReceiver(consoleConfig);
        this.xpipeAdminReceiver = new EmailReceiverParam.XPipeAdminReceiver(consoleConfig);
        this.clusterAdminReceiver = new EmailReceiverParam.ClusterAdminReceiver(clusterService);
    }

    protected List<String> getDbaEmails() {
        return dbaReceiver.param(null);
    }

    protected List<String> getXPipeAdminEmails() {
        return xpipeAdminReceiver.param(null);
    }

    protected List<String> getClusterAdminEmails(AlertEntity alertEntity) {
        return clusterAdminReceiver.param(alertEntity);
    }

    protected boolean shouldAlertDBA(AlertEntity alert) {
        return (alert.getAlertType().getAlertMethod() & EMAIL_DBA) != 0;
    }

    protected boolean shouldAlertXpipeAdmin(AlertEntity alert) {
        return (alert.getAlertType().getAlertMethod() & EMAIL_XPIPE_ADMIN) != 0;
    }

    protected boolean shouldAlertClusterAdmin(AlertEntity alert) {
        return (alert.getAlertType().getAlertMethod() & EMAIL_CLUSTER_ADMIN) != 0;
    }

    protected EmailReceiverParam.DbaReceiver dbaReceiver() {
        return dbaReceiver;
    }

    protected EmailReceiverParam.XPipeAdminReceiver xpipeAdminReceiver() {
        return xpipeAdminReceiver;
    }

    protected EmailReceiverParam.ClusterAdminReceiver clusterAdminReceiver() {
        return clusterAdminReceiver;
    }

    protected boolean isAlertSystemOn() {
        return configService.isAlertSystemOn();
    }

    public boolean supports(Class<? extends AlertPolicy> clazz) {
        return clazz.isAssignableFrom(AbstractEmailReceiver.class);
    }
}
