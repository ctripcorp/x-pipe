package com.ctrip.xpipe.redis.console.config.impl;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.config.ConfigChangeListener;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.config.ConfigKeyListener;
import com.ctrip.xpipe.redis.checker.config.impl.CheckConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.CommonConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.ConsoleConfigBean;
import com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.ctrip.xpipe.redis.console.util.HickwallMetricInfo;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Maps;
import io.netty.util.internal.ConcurrentSet;

import java.util.*;

public class CombConsoleConfig implements ConsoleConfig, ConfigChangeListener {

    private CheckConfigBean checkConfigBean;

    private ConsoleConfigBean consoleConfigBean;

    private DataCenterConfigBean dataCenterConfigBean;

    private CommonConfigBean commonConfigBean;

    private List<AbstractConfigBean> configBeans;

    public CombConsoleConfig(CheckConfigBean checkConfigBean,
                             ConsoleConfigBean consoleConfigBean,
                             DataCenterConfigBean dataCenterConfigBean,
                             CommonConfigBean commonConfigBean) {
        this.checkConfigBean = checkConfigBean;
        this.consoleConfigBean = consoleConfigBean;
        this.dataCenterConfigBean = dataCenterConfigBean;
        this.commonConfigBean = commonConfigBean;
        configBeans = new ArrayList<>();
        configBeans.add(consoleConfigBean);
        configBeans.add(dataCenterConfigBean);
        configBeans.add(commonConfigBean);
        configBeans.add(checkConfigBean);
        for (AbstractConfigBean configBean : configBeans) {
            configBean.addConfigChangeListener(this);
        }
    }

    private Map<String, List<ConfigChangeListener>> listeners = Maps.newConcurrentMap();

    private Set<ConfigKeyListener> listenersSet = new ConcurrentSet<>();

    private String hickwallInfo;

    private HickwallMetricInfo info;

    @Override
    public String getServerMode() {
        return checkConfigBean.getServerMode();
    }

    @Override
    public String getDatasource() {
        return commonConfigBean.getDatasource();
    }

    @Override
    public int getConsoleNotifyRetryTimes() {
        return consoleConfigBean.getConsoleNotifyRetryTimes();
    }

    @Override
    public int getConsoleNotifyRetryInterval() {
        return consoleConfigBean.getConsoleNotifyRetryInterval();
    }

    @Override
    public Map<String, String> getMetaservers() {
        return dataCenterConfigBean.getMetaservers();
    }

    @Override
    public int getConsoleNotifyThreads() {
        return consoleConfigBean.getConsoleNotifyThreads();
    }

    @Override
    public Set<String> getConsoleUserAccessWhiteList() {
        return commonConfigBean.getConsoleUserAccessWhiteList();
    }

    @Override
    public int getRedisReplicationHealthCheckInterval() {
        return checkConfigBean.getRedisReplicationHealthCheckInterval();
    }

    @Override
    public int getCheckerCurrentDcAllMetaRefreshIntervalMilli() {
        return checkConfigBean.getCheckerCurrentDcAllMetaRefreshIntervalMilli();
    }

    @Override
    public int getClusterHealthCheckInterval() {
        return checkConfigBean.getClusterHealthCheckInterval();
    }

    @Override
    public Map<String, String> getHickwallClusterMetricFormat() {
        return commonConfigBean.getHickwallClusterMetricFormat();
    }

    @Override
    public HickwallMetricInfo getHickwallMetricInfo() {
        String localInfo = commonConfigBean.getHickwallMetricInfo();
        if(StringUtil.isEmpty(hickwallInfo) || !localInfo.equals(hickwallInfo)) {
            hickwallInfo = localInfo;
            info = JsonCodec.INSTANCE.decode(hickwallInfo, HickwallMetricInfo.class);
        }
        return info;
    }

    @Override
    public int getHealthyDelayMilli() {
        return checkConfigBean.getHealthyDelayMilli();
    }

    @Override
    public long getHealthMarkCompensateIntervalMill() {
        return checkConfigBean.getHealthMarkCompensateIntervalMill();
    }

    @Override
    public int getHealthMarkCompensateThreads() {
        return checkConfigBean.getHealthMarkCompensateThreads();
    }

    @Override
    public int getHealthyDelayMilliThroughProxy() {
        return checkConfigBean.getHealthyDelayMilliThroughProxy();
    }

    @Override
    public int getInstanceLongDelayMilli() {
        return checkConfigBean.getInstanceLongDelayMilli();
    }

    @Override
    public int getDownAfterCheckNums() {
        return checkConfigBean.getDownAfterCheckNums();
    }

    @Override
    public int getDownAfterCheckNumsThroughProxy() {
        return checkConfigBean.getDownAfterCheckNumsThroughProxy();
    }

    @Override
    public int getCacheRefreshInterval() {
        return consoleConfigBean.getCacheRefreshInterval();
    }

    @Override
    public Set<String> getAlertWhileList() {
        return commonConfigBean.getAlertWhileList();
    }

    @Override
    public int getQuorum() {
        return checkConfigBean.getQuorum();
    }

    @Override
    public int getRedisConfCheckIntervalMilli() {
        return checkConfigBean.getRedisConfCheckIntervalMilli();
    }

    @Override
    public int getSentinelCheckIntervalMilli() {
        return checkConfigBean.getSentinelCheckIntervalMilli();
    }

    @Override
    public String getConsoleDomain() {
        return commonConfigBean.getConsoleDomain();
    }

    @Override
    public String getClusterExcludedRegex() {
        return commonConfigBean.getClusterExcludedRegex();
    }

    @Override
    public QuorumConfig getDefaultSentinelQuorumConfig() {
        return checkConfigBean.getDefaultSentinelQuorumConfig();
    }

    @Override
    public int getStableLossAfterRounds() {
        return checkConfigBean.getStableLossAfterRounds();
    }

    @Override
    public int getStableRecoverAfterRounds() {
        return checkConfigBean.getStableRecoverAfterRounds();
    }

    @Override
    public int getStableResetAfterRounds() {
        return checkConfigBean.getStableResetAfterRounds();
    }

    @Override
    public float getSiteStableThreshold() {
        return checkConfigBean.getSiteStableThreshold();
    }

    @Override
    public float getSiteUnstableThreshold() {
        return checkConfigBean.getSiteUnstableThreshold();
    }

    @Override
    public Boolean getSiteStable() {
        return checkConfigBean.getSiteStable();
    }

    @Override
    public String getReplDisklessMinRedisVersion() {
        return checkConfigBean.getReplDisklessMinRedisVersion();
    }

    @Override
    public String getXRedisMinimumRequestVersion() {
        return checkConfigBean.getXRedisMinimumRequestVersion();
    }

    @Override
    public String getXpipeRuntimeEnvironment() {
        return commonConfigBean.getXpipeRuntimeEnvironment();
    }

    @Override
    public String getDBAEmails() {
        return commonConfigBean.getDBAEmails();
    }

    @Override
    public String getRedisAlertSenderEmail() {
        return commonConfigBean.getRedisAlertSenderEmail();
    }

    @Override
    public String getXPipeAdminEmails() {
        return commonConfigBean.getXPipeAdminEmails();
    }

    @Override
    public int getAlertSystemSuspendMinute() {
        return commonConfigBean.getAlertSystemSuspendMinute();
    }

    @Override
    public int getAlertSystemRecoverMinute() {
        return commonConfigBean.getAlertSystemRecoverMinute();
    }

    @Override
    public int getConfigDefaultRestoreHours() {
        return consoleConfigBean.getConfigDefaultRestoreHours();
    }

    @Override
    public int getRebalanceSentinelInterval() {
        return consoleConfigBean.getRebalanceSentinelInterval();
    }

    @Override
    public int getRebalanceSentinelMaxNumOnce() {
        return consoleConfigBean.getRebalanceSentinelMaxNumOnce();
    }

    @Override
    public int getNoAlarmMinutesForClusterUpdate() {
        return commonConfigBean.getNoAlarmMinutesForClusterUpdate();
    }

    @Override
    public int getHealthCheckSuspendMinutes() {
        return consoleConfigBean.getHealthCheckSuspendMinutes();
    }

    @Override
    public Set<String> getIgnoredHealthCheckDc() {
        return checkConfigBean.getIgnoredHealthCheckDc();
    }

    @Override
    public int getClustersPartIndex() {
        return checkConfigBean.getClustersPartIndex();
    }

    @Override
    public int getCheckerReportIntervalMilli() {
        return checkConfigBean.getCheckerReportIntervalMilli();
    }

    @Override
    public int getCheckerMetaRefreshIntervalMilli() {
        return checkConfigBean.getCheckerMetaRefreshIntervalMilli();
    }

    @Override
    public String getConsoleAddress() {
        return checkConfigBean.getConsoleAddress();
    }

    @Override
    public int getCheckerAckIntervalMilli() {
        return checkConfigBean.getCheckerAckIntervalMilli();
    }

    @Override
    public long getConfigCacheTimeoutMilli() {
        return checkConfigBean.getConfigCacheTimeoutMilli();
    }

    @Override
    public int getProxyCheckUpRetryTimes() {
        return checkConfigBean.getProxyCheckUpRetryTimes();
    }

    @Override
    public int getProxyCheckDownRetryTimes() {
        return checkConfigBean.getProxyCheckDownRetryTimes();
    }

    private Set<String> sentinelCheckOuterClientClusters() {
        return checkConfigBean.sentinelCheckOuterClientClusters();
    }

    @Override
    public boolean supportSentinelHealthCheck(ClusterType clusterType, String clusterName) {
        return clusterType.supportHealthCheck()
                ||  checkConfigBean.shouldSentinelCheckOuterClientClusters()
                || sentinelCheckOuterClientClusters().contains(clusterName.toLowerCase());
    }

    @Override
    public void register(List<String> keys, ConfigChangeListener configListener) {
        for(String key : keys) {
            listeners.putIfAbsent(key, new LinkedList<>());
            listeners.get(key).add(configListener);
        }
    }

    @Override
    public String sentinelCheckDowngradeStrategy() {
        return checkConfigBean.sentinelCheckDowngradeStrategy();
    }

    @Override
    public String crossDcSentinelMonitorNameSuffix() {
        return checkConfigBean.crossDcSentinelMonitorNameSuffix();
    }

    @Override
    public int getNonCoreCheckIntervalMilli() {
        return checkConfigBean.getNonCoreCheckIntervalMilli();
    }

    @Override
    public Set<String> getOuterClusterTypes() {
        return consoleConfigBean.getOuterClusterTypes();
    }

    @Override
    public Map<String, String> sentinelMasterConfig() {
        return checkConfigBean.sentinelMasterConfig();
    }

    @Override
    public long subscribeTimeoutMilli() {
        return checkConfigBean.subscribeTimeoutMilli();
    }

    @Override
    public String getDcsRelations() {
        return commonConfigBean.getDcsRelations();
    }

    @Override
    public int maxRemovedDcsCnt() {
        return checkConfigBean.maxRemovedDcsCnt();
    }

    @Override
    public int maxRemovedClustersPercent() {
        return checkConfigBean.maxRemovedClustersPercent();
    }

    @Override
    public int getKeeperCheckerIntervalMilli() {
        return checkConfigBean.getKeeperCheckerIntervalMilli();
    }

    @Override
    public int getPingDownAfterMilli() {
        return checkConfigBean.getPingDownAfterMilli();
    }

    @Override
    public int getPingDownAfterMilliThroughProxy() {
        return checkConfigBean.getPingDownAfterMilliThroughProxy();
    }

    @Override
    public Pair<String, String> getClusterShardForMigrationSysCheck() {
        return consoleConfigBean.getClusterShardForMigrationSysCheck();
    }

    @Override
    public int getProxyInfoCollectInterval() {
        return consoleConfigBean.getProxyInfoCollectInterval();
    }

    @Override
    public int getOutterClientCheckInterval() {
        return checkConfigBean.getOutterClientCheckInterval();
    }

    @Override
    public int getOuterClientSyncInterval() {
        return consoleConfigBean.getOuterClientSyncInterval();
    }

    @Override
    public String getOuterClientToken() {
        return commonConfigBean.getOuterClientToken();
    }

    @Override
    public Map<String, String> getConsoleDomains() {
        return commonConfigBean.getConsoleDomains();
    }

    @Override
    public boolean isSentinelRateLimitOpen() {
        return checkConfigBean.isSentinelRateLimitOpen();
    }

    @Override
    public int getSentinelRateLimitSize() {
        return checkConfigBean.getSentinelRateLimitSize();
    }

    @Override
    public Set<String> getVariablesCheckDataSources() {
        return consoleConfigBean.getVariablesCheckDataSources();
    }

    @Override
    public Set<String> getOwnClusterType() {
        return consoleConfigBean.getOwnClusterType();
    }

    @Override
    public Set<String> shouldNotifyClusterTypes() {
        return commonConfigBean.shouldNotifyClusterTypes();
    }

    @Override
    public String getCrossDcLeaderLeaseName() {
        return dataCenterConfigBean.getCrossDcLeaderLeaseName();
    }

    @Override
    public List<BeaconOrgRoute> getBeaconOrgRoutes() {
        String property = commonConfigBean.getBeaconOrgRoutes();
        return JsonCodec.INSTANCE.decode(property, new GenericTypeReference<List<BeaconOrgRoute>>() {});
    }

    @Override
    public int getClusterDividedParts() {
        List<String> groupList = checkConfigBean.getClustersList();
        return groupList.size();
    }

    @Override
    public int getCheckerAckTimeoutMilli() {
        return dataCenterConfigBean.getCheckerAckTimeoutMilli();
    }

    @Override
    public long getMigrationTimeoutMilli() {
        return consoleConfigBean.getMigrationTimeoutMilli();
    }

    @Override
    public long getServletMethodTimeoutMilli() {
        return consoleConfigBean.getServletMethodTimeoutMilli();
    }

    @Override
    public boolean isRedisConfigCheckMonitorOpen() {
        return checkConfigBean.isRedisConfigCheckMonitorOpen();
    }

    @Override
    public String getRedisConfigCheckRules() {
        return checkConfigBean.getRedisConfigCheckRules();
    }

    @Override
    public String getChooseRouteStrategyType() {
        return commonConfigBean.getChooseRouteStrategyType();
    }

    @Override
    public boolean isAutoMigrateOverloadKeeperContainerOpen() {
        return consoleConfigBean.isAutoMigrateOverloadKeeperContainerOpen();
    }

    @Override
    public long getAutoMigrateOverloadKeeperContainerIntervalMilli() {
        return consoleConfigBean.getAutoMigrateOverloadKeeperContainerIntervalMilli();
    }

    @Override
    public double getKeeperPairOverLoadFactor() {
        return consoleConfigBean.getKeeperPairOverLoadFactor();
    }

    @Override
    public double getKeeperContainerDiskOverLoadFactor() {
        return consoleConfigBean.getKeeperContainerDiskOverLoadFactor();
    }

    @Override
    public double getKeeperContainerIoRate() {
        return consoleConfigBean.getKeeperContainerIoRate();
    }

    @Override
    public long getMetaServerSlotClusterMapCacheTimeOutMilli() {
        return consoleConfigBean.getMetaServerSlotClusterMapCacheTimeOutMilli();
    }

    @Override
    public boolean autoSetKeeperSyncLimit() {
        return consoleConfigBean.autoSetKeeperSyncLimit();
    }

    @Override
    public void addListener(ConfigKeyListener listener) {
        this.listenersSet.add(listener);
    }

    @Override
    public String getZkConnectionString() {
        return dataCenterConfigBean.getZkConnectionString();
    }

    @Override
    public String getZkNameSpace() {
        return dataCenterConfigBean.getZkNameSpace();
    }

    public void onChange(String key, String oldValue, String newValue) {

        for(ConfigKeyListener listener : listenersSet) {
            listener.onChange(key, newValue);
        }

        if(!listeners.containsKey(key)) {
            return;
        }
        for(ConfigChangeListener listener : listeners.get(key)) {
            listener.onChange(key, oldValue, newValue);
        }
    }

    protected String getProperty(String key) {
        for(AbstractConfigBean configBean : configBeans) {
           String val = configBean.getProperty(key);
           if(val != null) {
               return val;
           }
        }
        return null;
    }

    protected String getProperty(String key, String def) {
        String value = this.getProperty(key);
        if(value == null) {
            return def;
        } else {
            return value;
        }
    }
}
