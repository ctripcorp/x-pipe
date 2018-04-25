package com.ctrip.xpipe.redis.console.alert.policy.receiver;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertEntity;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Apr 20, 2018
 */
public class DefaultGroupEmailReceiver extends AbstractEmailReceiver implements GroupEmailReceiver {

    public DefaultGroupEmailReceiver(ConsoleConfig consoleConfig, ConfigService configService) {
        super(consoleConfig, configService);
    }

    @Override
    public Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> getGroupedEmailReceiver(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> result = Maps.newHashMap();
        if(!isAlertSystemOn()) {
            EmailReceiverModel receivers = new EmailReceiverModel(getXPipeAdminEmails(), null);
            result.put(receivers, Maps.newHashMap(alerts));
            return result;
        }

        EmailReceiverModel dbaAsReceivers = new EmailReceiverModel(getDbaEmails(), getXPipeAdminEmails());
        EmailReceiverModel xpipeAdminAsReceivers = new EmailReceiverModel(getXPipeAdminEmails(), null);

        result.put(dbaAsReceivers, dbaAsReceiverAlerts(alerts));
        result.put(xpipeAdminAsReceivers, xpipeAdminAsReceiverAlerts(alerts));

        return result;
    }

    private Map<ALERT_TYPE, Set<AlertEntity>> xpipeAdminAsReceiverAlerts(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        Map<ALERT_TYPE, Set<AlertEntity>> filteredMap = Maps.filterEntries(alerts, new Predicate<Map.Entry<ALERT_TYPE, Set<AlertEntity>>>() {
            @Override
            public boolean apply(Map.Entry<ALERT_TYPE, Set<AlertEntity>> input) {
                return !dbaSensitive(input.getKey());
            }
        });
        return Maps.newHashMap(filteredMap);
    }

    private Map<ALERT_TYPE, Set<AlertEntity>> dbaAsReceiverAlerts(Map<ALERT_TYPE, Set<AlertEntity>> alerts) {
        Map<ALERT_TYPE, Set<AlertEntity>> filteredMap =  Maps.filterEntries(alerts, new Predicate<Map.Entry<ALERT_TYPE, Set<AlertEntity>>>() {
            @Override
            public boolean apply(Map.Entry<ALERT_TYPE, Set<AlertEntity>> input) {
                return dbaSensitive(input.getKey());
            }
        });
        return Maps.newHashMap(filteredMap);
    }

    private boolean dbaSensitive(ALERT_TYPE type) {
        return (type.getAlertMethod() & EMAIL_DBA) != 0;
    }

    @Override
    public EmailReceiverModel receivers(AlertEntity alert) {
        throw new UnsupportedOperationException("Not support by " + DefaultGroupEmailReceiver.class.getName());
    }

}
