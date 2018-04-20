package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.alert.policy.AlertPolicy;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ConfigService;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Apr 19, 2018
 */
public abstract class AbstractEmailReceiver implements EmailReceiver {

    private ConsoleConfig consoleConfig;

    private ConfigService configService;

    public AbstractEmailReceiver(ConsoleConfig consoleConfig, ConfigService configService) {
        this.consoleConfig = consoleConfig;
        this.configService = configService;
    }

    protected List<String> getDbaEmails() {
        return EmailReceiverParam.DbaReceiver.getInstance(consoleConfig).param();
    }

    protected List<String> getXPipeAdminEmails() {
        return EmailReceiverParam.XPipeAdminReceiver.getInstance(consoleConfig).param();
    }

    protected boolean shouldAlertDBA(AlertEntity alert) {
        return (alert.getAlertType().getAlertMethod() & EMAIL_DBA) != 0;
    }

    protected boolean shouldAlertXpipeAdmin(AlertEntity alert) {
        return (alert.getAlertType().getAlertMethod() & EMAIL_XPIPE_ADMIN) != 0;
    }

    protected EmailReceiverParam.DbaReceiver dbaReceiver() {
        return EmailReceiverParam.DbaReceiver.getInstance(consoleConfig);
    }

    protected EmailReceiverParam.XPipeAdminReceiver xpipeAdminReceiver() {
        return EmailReceiverParam.XPipeAdminReceiver.getInstance(consoleConfig);
    }

    protected boolean isAlertSystemOn() {
        return configService.isAlertSystemOn();
    }

    public boolean supports(Class<? extends AlertPolicy> clazz) {
        return clazz.isAssignableFrom(AbstractEmailReceiver.class);
    }
}
