package com.ctrip.xpipe.redis.checker.alert.policy.receiver;

import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertConfig;
import com.ctrip.xpipe.redis.checker.alert.AlertEntity;
import com.ctrip.xpipe.redis.checker.alert.message.AlertEntityHolderManager;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
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

    public DefaultGroupEmailReceiver(AlertConfig alertConfig, CheckerDbConfig checkerDbConfig, MetaCache metaCache) {
        super(alertConfig, checkerDbConfig, metaCache);
    }

    @Override
    public Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> getGroupedEmailReceiver(AlertEntityHolderManager alerts) {
        Map<EmailReceiverModel, Map<ALERT_TYPE, Set<AlertEntity>>> result = Maps.newHashMap();
        if(!isAlertSystemOn()) {
            EmailReceiverModel receivers = new EmailReceiverModel(getXPipeAdminEmails(), null);
            result.put(receivers, Maps.newHashMap(alerts.convertToMap()));
            return result;
        }

        EmailReceiverModel dbaAsReceivers = new EmailReceiverModel(getDbaEmails(), getXPipeAdminEmails());
        EmailReceiverModel xpipeAdminAsReceivers = new EmailReceiverModel(getXPipeAdminEmails(), null);

        result.put(dbaAsReceivers, dbaAsReceiverAlerts(alerts.convertToMap()));
        result.put(xpipeAdminAsReceivers, xpipeAdminAsReceiverAlerts(alerts.convertToMap()));

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

    private boolean clusterAdminSensitive(ALERT_TYPE type) {
        return (type.getAlertMethod() & EMAIL_CLUSTER_ADMIN) != 0;
    }

    @Override
    public EmailReceiverModel receivers(AlertEntity alert) {
        throw new UnsupportedOperationException("Not support by " + DefaultGroupEmailReceiver.class.getName());
    }

}
