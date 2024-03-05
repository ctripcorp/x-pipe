package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.unidal.dal.jdbc.DalException;

import java.util.Date;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Nov 27, 2017
 */
public interface ConfigService {

    String KEY_SENTINEL_AUTO_PROCESS = "sentinel.auto.process";

    String KEY_ALERT_SYSTEM_ON = "alert.system.on";

    String KEY_IGNORE_MIGRATION_SYSTEM_AVAILABILITY = "ignore.migration.system.avail";

    String KEY_SENTINEL_CHECK_EXCLUDE = "sentinel.check.exclude";

    String KEY_ALLOW_AUTO_MIGRATION = "allow.migration.auto";

    String KEY_CLUSTER_ALERT_EXCLUDE = "alert.cluster.exclude";

    String KEY_KEEPER_CONTAINER_STANDARD = "keeper.container.standard";

    void setKeyKeeperContainerStandard(ConfigModel config) throws Exception;

    void startAlertSystem(ConfigModel config) throws DalException;

    void stopAlertSystem(ConfigModel config, int hours) throws DalException;

    void startSentinelAutoProcess(ConfigModel config) throws DalException;

    void stopSentinelAutoProcess(ConfigModel config, int hours) throws DalException;

    void startClusterAlert(ConfigModel config) throws DalException;

    void stopClusterAlert(ConfigModel config, int minutes) throws DalException;

    void startSentinelCheck(ConfigModel config) throws DalException;

    void stopSentinelCheck(ConfigModel config, int minutes) throws DalException;

    void resetClusterWhitelist(String cluster) throws DalException;

    void updateCrossDcLeader(ConfigModel config, Date until) throws DalException;
    
    String getCrossDcLeader() throws DalException;

    boolean shouldSentinelCheck(String cluster);

    List<ConfigModel> getActiveSentinelCheckExcludeConfig();

    List<ConfigModel> getActiveClusterAlertExcludeConfig();

    boolean isAlertSystemOn();

    boolean isSentinelAutoProcess();

    Date getAlertSystemRecoverTime();

    Date getSentinelAutoProcessRecoverTime();

    boolean ignoreMigrationSystemAvailability();

    void doIgnoreMigrationSystemAvailability(boolean ignore) throws DalException;

    boolean allowAutoMigration();

    void setAllowAutoMigration(boolean allow) throws DalException;

    ConfigModel getConfig(String key);

    ConfigModel getConfig(String key, String subId);

    List<ConfigModel> getConfigs(String key);
}
