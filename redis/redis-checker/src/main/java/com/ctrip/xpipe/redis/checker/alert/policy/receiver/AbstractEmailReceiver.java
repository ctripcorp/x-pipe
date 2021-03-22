package com.ctrip.xpipe.redis.checker.alert.policy.receiver;

import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.policy.AlertPolicy;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public abstract class AbstractEmailReceiver implements EmailReceiver {

    private AlertConfig alertConfig;

    private CheckerDbConfig checkerDbConfig;

    private EmailReceiverParam.DbaReceiver dbaReceiver;

    private EmailReceiverParam.XPipeAdminReceiver xpipeAdminReceiver;

    private EmailReceiverParam.ClusterAdminReceiver clusterAdminReceiver;

    public AbstractEmailReceiver(AlertConfig alertConfig, CheckerDbConfig checkerDbConfig, MetaCache metaCache) {
        this.alertConfig = alertConfig;
        this.checkerDbConfig = checkerDbConfig;
        this.dbaReceiver = new EmailReceiverParam.DbaReceiver(this.alertConfig);
        this.xpipeAdminReceiver = new EmailReceiverParam.XPipeAdminReceiver(this.alertConfig);
        this.clusterAdminReceiver = new EmailReceiverParam.ClusterAdminReceiver(metaCache);
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
        return checkerDbConfig.isAlertSystemOn();
    }

    public boolean supports(Class<? extends AlertPolicy> clazz) {
        return clazz.isAssignableFrom(AbstractEmailReceiver.class);
    }
}
