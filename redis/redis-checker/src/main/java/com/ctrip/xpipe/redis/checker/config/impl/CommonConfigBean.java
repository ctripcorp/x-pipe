package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.redis.core.route.RouteChooseStrategyFactory;
import org.springframework.context.annotation.Configuration;

import java.util.*;
import java.util.stream.Collectors;

@Configuration
public class CommonConfigBean extends AbstractConfigBean {

    public static final String KEY_USER_ACCESS_WHITE_LIST = "user.access.white.list";

    public static final String KEY_ALERT_WHITE_LIST = "console.alert.whitelist";

    public static final String KEY_ALERT_CLUSTER_EXCLUDED_REGEX = "console.alert.excluded.regex";

    public static final String KEY_ALERT_MESSAGE_RECOVER_TIME = "alert.message.recover.time";

    public static final String KEY_ALERT_MESSAGE_SUSPEND_TIME = "alert.message.suspend.time";

    public static final String KEY_DBA_EMAILS = "redis.dba.emails";

    public static final String KEY_XPIPE_ADMIN_EMAILS = "xpipe.admin.emails";

    public static final String KEY_DOMAIN = "console.domain";

    public static final String KEY_XPIPE_RUNTIME_ENVIRONMENT = "xpipe.runtime.environment";

    public static final String KEY_HICKWALL_CLUSTER_METRIC_FORMAT = "console.hickwall.cluster.metric.format";

    public static final String KEY_DATASOURCE = "datasource";

    public static final String KEY_HICKWALL_METRIC_INFO = "console.hickwall.metric.info";

    private static final String KEY_NOTIFY_CLUSTER_TYPES = "console.notify.cluster.types";

    private static final String KEY_ROUTE_CHOOSE_STRATEGY_TYPE = "route.choose.strategy.type";

    private static final String KEY_OUTER_CLIENT_TOKEN = "console.outer.client.token";

    private static final String KEY_DCS_RELATIONS = "dcs.relations";

    public static final String KEY_REDIS_ALERT_SENDER_EMAIL = "redis.alert.sender.email";

    public static final String KEY_NO_ALARM_MUNITE_FOR_CLUSTER_UPDATE = "no.alarm.minute.for.cluster.update";

    public static final String KEY_META_SYNC_EXTERNAL_DC = "meta.sync.external.dc";

    private String defaultRouteChooseStrategyType = RouteChooseStrategyFactory.RouteStrategyType.CRC32_HASH.name();

    public CommonConfigBean() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.COMMON_CONFIG));
    }

    public Set<String> getConsoleUserAccessWhiteList() {
        String whiteList = getProperty(KEY_USER_ACCESS_WHITE_LIST, "*");
        return new HashSet<>(Arrays.asList(whiteList.split(",")));
    }

    public Set<String> getExtraSyncDC() {
        String dcs = getProperty(KEY_META_SYNC_EXTERNAL_DC, "");
        return getSplitStringSet(dcs);
    }

    public boolean disableDb() {
        return getExtraSyncDC().stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet())
                .contains(FoundationService.DEFAULT.getDataCenter());
    }

    public Set<String> getAlertWhileList() {
        String whitelist = getProperty(KEY_ALERT_WHITE_LIST, "");
        return getSplitStringSet(whitelist);
    }

    public String getClusterExcludedRegex() {
        return getProperty(KEY_ALERT_CLUSTER_EXCLUDED_REGEX, "");
    }

    public String getDBAEmails() {
        return getProperty(KEY_DBA_EMAILS, "DBA@email.com");
    }

    public String getXPipeAdminEmails() {
        return getProperty(KEY_XPIPE_ADMIN_EMAILS, "XPipeAdmin@email.com");
    }

    public String getConsoleDomain() {
        return getProperty(KEY_DOMAIN, "127.0.0.1");
    }

    public String getXpipeRuntimeEnvironment() {
        return getProperty(KEY_XPIPE_RUNTIME_ENVIRONMENT, "");
    }

    public String getDatasource() {
        return getProperty(KEY_DATASOURCE, "fxxpipe");
    }

    public Map<String,String> getHickwallClusterMetricFormat() {
        String property = getProperty(KEY_HICKWALL_CLUSTER_METRIC_FORMAT, "{}");
        return JsonCodec.INSTANCE.decode(property, Map.class);
    }

    public String getHickwallMetricInfo() {
        return getProperty(KEY_HICKWALL_METRIC_INFO, "{\"domain\": \"http://hickwall.qa.nt.ctripcorp.com/grafanav2/d/UR32kfjWz/fx-xpipe?fullscreen&orgId=1&from=now-1h&to=now\", \"delayPanelId\": 2, \"crossDcDelayPanelId\": 14, \"proxyPingPanelId\": 4, \"proxyTrafficPanelId\": 6, \"proxyCollectionPanelId\": 8, \"outComingTrafficToPeerPanelId\": \"16\", \"inComingTrafficFromPeerPanelId\": \"18\",\"peerSyncFullPanelId\": \"20\",\"peerSyncPartialPanelId\": \"22\"}");
    }

    public Set<String> shouldNotifyClusterTypes() {
        String clusterTypes = getProperty(KEY_NOTIFY_CLUSTER_TYPES, ClusterType.ONE_WAY.toString()+","+ClusterType.BI_DIRECTION.toString());
        return getSplitStringSet(clusterTypes);
    }

    public int getAlertSystemRecoverMinute() {
        return getIntProperty(KEY_ALERT_MESSAGE_RECOVER_TIME, 5);
    }

    public int getAlertSystemSuspendMinute() {
        return getIntProperty(KEY_ALERT_MESSAGE_SUSPEND_TIME, 30);
    }

    public String getChooseRouteStrategyType() { return getProperty(KEY_ROUTE_CHOOSE_STRATEGY_TYPE, defaultRouteChooseStrategyType);}

    public String getOuterClientToken() {
        return getProperty(KEY_OUTER_CLIENT_TOKEN, "");
    }

    public String getDcsRelations() {
        return getProperty(KEY_DCS_RELATIONS, "{}");
    }

    public String getRedisAlertSenderEmail() {
        return getProperty(KEY_REDIS_ALERT_SENDER_EMAIL, "");
    }

    public int getNoAlarmMinutesForClusterUpdate() {
        return getIntProperty(KEY_NO_ALARM_MUNITE_FOR_CLUSTER_UPDATE, 15);
    }

}
