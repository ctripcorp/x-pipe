package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.election.CrossDcLeaderElectionAction;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.AlertSystemOffChecker;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.AutoMigrationOffChecker;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.KeeperBalanceInfoCollectOnChecker;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.SentinelAutoProcessChecker;
import com.ctrip.xpipe.redis.console.keeper.entity.KeeperContainerDiskType;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.*;


/**
 * @author chen.zhu
 * <p>
 * Nov 27, 2017
 */
@Service
public class ConfigServiceImpl implements ConfigService {

    @Autowired
    private ConfigDao configDao;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private AlertSystemOffChecker alertSystemOffChecker;

    @Autowired
    private SentinelAutoProcessChecker sentinelAutoProcessChecker;

    @Autowired
    private AutoMigrationOffChecker autoMigrationOffChecker;

    @Autowired
    private KeeperBalanceInfoCollectOnChecker keeperBalanceInfoCollectOnChecker;

    private String crossDcLeaderLeaseName;

    private Logger logger = LoggerFactory.getLogger(ConfigServiceImpl.class);

    private static final String EVENT_CONFIG_CHANGE = "event_config_change";

    @Autowired
    public ConfigServiceImpl(ConsoleConfig consoleConfig) {
        this.crossDcLeaderLeaseName = consoleConfig.getCrossDcLeaderLeaseName();
    }

    @Override
    public void setKeyKeeperContainerStandard(ConfigModel config) throws Exception {
        if (!KEY_KEEPER_CONTAINER_STANDARD.equals(config.getKey())) {
            throw new RuntimeException(String.format("key should be %s !", KEY_KEEPER_CONTAINER_STANDARD));
        }
        try {
            Long.parseLong(config.getVal());
        } catch (NumberFormatException e) {
            throw new RuntimeException(String.format("value %s should be number ", config.getVal()));
        }
        configDao.setConfig(config);
    }

    @Override
    public void setKeyKeeperContainerIoRate(ConfigModel config) throws Exception {
        if (!KEY_KEEPER_CONTAINER_IO_RATE.equals(config.getKey())) {
            throw new RuntimeException(String.format("key should be %s !", KEY_KEEPER_CONTAINER_IO_RATE));
        }
        List<ConfigModel> standardConfigs = getConfigs(KEY_KEEPER_CONTAINER_STANDARD);
        List<String> diskTypes = new ArrayList<>();
        standardConfigs.forEach(configModel -> {
            diskTypes.add(configModel.getSubKey().split(KeeperContainerDiskType.DEFAULT.interval)[0]);
        });
        if (!diskTypes.contains(config.getSubKey())) {
            throw new RuntimeException(String.format("subkey:%s should be in diskTypes %s !", config.getSubKey(), diskTypes));
        }
        try {
            Long.parseLong(config.getVal());
        } catch (NumberFormatException e) {
            throw new RuntimeException(String.format("value %s should be number ", config.getVal()));
        }
        configDao.setConfig(config);
    }

    @Override
    public void startAlertSystem(ConfigModel config) throws DalException {

        logger.info("[startAlertSystem] start alert system, config: {}", config);
        config.setKey(KEY_ALERT_SYSTEM_ON).setVal(String.valueOf(true));
        configDao.setConfig(config);
    }

    @Override
    public void stopAlertSystem(ConfigModel config, int hours) throws DalException {


        Date date = DateTimeUtils.getHoursLaterDate(hours);
        boolean previousStateOn = isAlertSystemOn();

        config.setKey(KEY_ALERT_SYSTEM_ON).setVal(String.valueOf(false));
        logger.info("[stopAlertSystem] stop alert system, config: {}", config);

        configDao.setConfigAndUntil(config, date);
        if(previousStateOn) {
            logger.info("[stopAlertSystem] Alert System was On, alert this operation");
            alertSystemOffChecker.startAlert();
        }
    }

    @Override
    public void startSentinelAutoProcess(ConfigModel config) throws DalException {

        logger.info("[startSentinelAutoProcess] start sentinel auto process, config: {}", config);
        config.setKey(KEY_SENTINEL_AUTO_PROCESS).setVal(String.valueOf(true));
        configDao.setConfig(config);
    }

    @Override
    public void stopSentinelAutoProcess(ConfigModel config, int hours) throws DalException {

        logger.info("[stopSentinelAutoProcess] stop sentinel auto process, config: {}", config);
        Date date = DateTimeUtils.getHoursLaterDate(hours);
        boolean previousStateOn = isSentinelAutoProcess();

        config.setKey(KEY_SENTINEL_AUTO_PROCESS).setVal(String.valueOf(false));
        configDao.setConfigAndUntil(config, date);
        if(previousStateOn) {
            sentinelAutoProcessChecker.startAlert();
        }

    }

    @Override
    public void startKeeperBalanceInfoCollect(ConfigModel config, int hours) throws DalException {
        logger.info("[startKeeperBalanceInfoCollect] start keeper balance info collect, config: {}", config);
        Date date = DateTimeUtils.getHoursLaterDate(hours);
        boolean previousStateOff = !isKeeperBalanceInfoCollectOn();

        config.setKey(KEY_KEEPER_BALANCE_INFO_COLLECT).setVal(String.valueOf(true));
        configDao.setConfigAndUntil(config, date);
        if(previousStateOff) {
            keeperBalanceInfoCollectOnChecker.startAlert();
        }
    }

    @Override
    public void stopKeeperBalanceInfoCollect(ConfigModel config) throws DalException {
        logger.info("[stopKeeperBalanceInfoCollect] stop keeper balance info collect, config: {}", config);
        config.setKey(KEY_KEEPER_BALANCE_INFO_COLLECT).setVal(String.valueOf(false));
        configDao.setConfig(config);
    }

    @Override
    public void startClusterAlert(ConfigModel config) throws DalException {
        logger.info("[startClusterAlert][{}]", config.getSubKey());
        config.setKey(KEY_CLUSTER_ALERT_EXCLUDE)
                .setVal(String.valueOf(false));
        logChangeEvent(config, null);
        configDao.setConfig(config);
    }

    @Override
    public void stopClusterAlert(ConfigModel config, int minutes) throws DalException {
        logger.info("[stopClusterAlert][{}] for {}", config.getSubKey(), minutes);

        Date date = DateTimeUtils.getMinutesLaterThan(new Date(), minutes);
        config.setKey(KEY_CLUSTER_ALERT_EXCLUDE)
                .setVal(String.valueOf(true));
        logChangeEvent(config, date);
        configDao.setConfigAndUntil(config, date);
    }

    @Override
    public void startSentinelCheck(ConfigModel config) throws DalException {
        logger.info("[startSentinelCheck] : turn off sentinel check exclude config {} for cluster {}", config, config.getSubKey());

        config.setKey(KEY_SENTINEL_CHECK_EXCLUDE)
                .setVal(String.valueOf(false));
        logChangeEvent(config, null);
        configDao.setConfig(config);
    }

    @Override
    public void stopSentinelCheck(ConfigModel config, int minutes) throws DalException {
        logger.info("[stopSentinelCheck] : turn on sentinel check exclude config {} for cluster {} till {} minutes later",
                config, config.getSubKey(), minutes);

        Date date = DateTimeUtils.getMinutesLaterThan(new Date(), minutes);
        config.setKey(KEY_SENTINEL_CHECK_EXCLUDE)
                .setVal(String.valueOf(true));
        logChangeEvent(config, date);
        configDao.setConfigAndUntil(config, date);
    }

    @Override
    public void resetClusterWhitelist(String cluster) throws DalException {
        logger.info("[resetClusterWhitelist]: reset all whitelist for cluster {}", cluster);
        ConfigModel configModel = new ConfigModel().setSubKey(cluster)
                .setUpdateIP(FoundationService.DEFAULT.getLocalIp()).setUpdateUser("reset");
        if (!shouldSentinelCheck(cluster)) {
            startSentinelCheck(configModel);
        }
        if (!shouldAlert(cluster)) {
            startClusterAlert(configModel);
        }
    }

    public void updateCrossDcLeader(ConfigModel config, Date until) throws DalException {
        logger.info("[updateCrossDcLeader] update lease to {} until {}", config.getVal(), until);
        config.setKey(CrossDcLeaderElectionAction.KEY_LEASE_CONFIG);
        config.setSubKey(crossDcLeaderLeaseName);

        configDao.setConfig(config, until);
    }

    public String getCrossDcLeader() throws DalException {
        ConfigTbl leaseConfig = configDao.getByKeyAndSubId(CrossDcLeaderElectionAction.KEY_LEASE_CONFIG, crossDcLeaderLeaseName);

        // only return when lease is active
        if (new Date().compareTo(leaseConfig.getUntil()) < 0) return leaseConfig.getValue();
        else return null;
    }

    @Override
    public boolean shouldSentinelCheck(String cluster) {
        return !getConfigBooleanByKeyAndSubKey(KEY_SENTINEL_CHECK_EXCLUDE, cluster, false);
    }

    public boolean shouldAlert(String cluster) {
        return !getConfigBooleanByKeyAndSubKey(KEY_CLUSTER_ALERT_EXCLUDE, cluster, false);
    }

    private boolean getConfigBooleanByKeyAndSubKey(String key, String subKey, boolean defaultVal) {
        try {
            ConfigTbl config = configDao.getByKeyAndSubId(key, subKey);
            if (null == config || (new Date()).after(config.getUntil())) {
                return defaultVal;
            }
            return Boolean.parseBoolean(config.getValue());
        } catch (Exception e) {
            return defaultVal;
        }
    }

    @Override
    public List<ConfigModel> getActiveSentinelCheckExcludeConfig() {
        return getActiveConfig(KEY_SENTINEL_CHECK_EXCLUDE, String.valueOf(true));
    }

    @Override
    public List<ConfigModel> getActiveClusterAlertExcludeConfig() {
        return getActiveConfig(KEY_CLUSTER_ALERT_EXCLUDE, String.valueOf(true));
    }

    private List<ConfigModel> getActiveConfig(String key, String val) {
        List<ConfigModel> models = new ArrayList<>();
        /*
        List<ConfigTbl> configTbls = configDao.findAllByKeyAndValueAndUntilAfter(key, val, new Date());
        if (configTbls.isEmpty()) return Collections.emptyList();

        configTbls.forEach(configTbl -> models.add(new ConfigModel(configTbl)));
       */
        return models;
    }

    @Override
    public boolean isAlertSystemOn() {
        return getAndResetTrueIfExpired(KEY_ALERT_SYSTEM_ON);
    }

    @Override
    public boolean isSentinelAutoProcess() {
        return getAndResetTrueIfExpired(KEY_SENTINEL_AUTO_PROCESS);
    }

    @Override
    public boolean isKeeperBalanceInfoCollectOn() {
        return getAndResetFalseIfExpired(KEY_KEEPER_BALANCE_INFO_COLLECT);
    }

    @Override
    public Date getAlertSystemRecoverTime() {
        try {
            return configDao.getByKey(KEY_ALERT_SYSTEM_ON).getUntil();
        } catch (DalException e) {
            logger.error("[getAlertSystemRecovertIME]", e);
            return null;
        }
    }

    @Override
    public Date getSentinelAutoProcessRecoverTime() {
        try {
            return configDao.getByKey(KEY_SENTINEL_AUTO_PROCESS).getUntil();
        } catch (DalException e) {
            logger.error("[getSentinelAutoProcessRecoverTime]", e);
            return null;
        }
    }

    @Override
    public Date getKeeperBalanceInfoCollectRecoverTime() {
        try {
            return configDao.getByKey(KEY_KEEPER_BALANCE_INFO_COLLECT).getUntil();
        } catch (DalException e) {
            logger.error("[getKeeperBalanceInfoCollectRecoverTime]", e);
            return null;
        }
    }

    @Override
    public boolean ignoreMigrationSystemAvailability() {
        try {
            ConfigModel configModel = getOrCreate(KEY_IGNORE_MIGRATION_SYSTEM_AVAILABILITY, String.valueOf(false));
            return Boolean.parseBoolean(configModel.getVal());
        } catch (Exception e) {
            logger.error("[ignoreMigrationSystemAvailability]", e);
            return false;
        }
    }

    @Override
    public void doIgnoreMigrationSystemAvailability(boolean ignore) {
        try {
            ConfigModel configModel = getOrCreate(KEY_IGNORE_MIGRATION_SYSTEM_AVAILABILITY, String.valueOf(ignore));
            configModel.setVal(String.valueOf(ignore));
            configDao.setConfig(configModel);
        } catch (Exception e) {
            logger.error("[ignoreMigrationSystemAvailability]", e);
            throw new DalUpdateException(e.getMessage());
        }
    }

    @Override
    public boolean allowAutoMigration() {
        try {
            ConfigModel configModel = getOrCreate(KEY_ALLOW_AUTO_MIGRATION, String.valueOf(true));
            return Boolean.parseBoolean(configModel.getVal());
        } catch (Exception e) {
            logger.error("[ignoreMigrationSystemAvailability]", e);
            return true;
        }
    }

    @Override
    public void setAllowAutoMigration(boolean allow) throws DalException {
        try {
            ConfigModel configModel = getOrCreate(KEY_ALLOW_AUTO_MIGRATION, String.valueOf(allow));
            boolean origin = Boolean.parseBoolean(configModel.getVal());
            configModel.setVal(String.valueOf(allow));
            configDao.setConfig(configModel);

            if (origin && !allow) {
                logger.info("[setAllowAutoMigration] Auto Migration stop");
                autoMigrationOffChecker.startAlert();
            }
        } catch (Exception e) {
            logger.error("[setAllowAutoMigration]", e);
            throw new DalUpdateException(e.getMessage());
        }
    }

    @Override
    public ConfigModel getConfig(String key) {
        return getConfig(key, "");
    }

    @Override
    public ConfigModel getConfig(String key, String subId) {
        try {
            ConfigTbl configTbl = configDao.getByKeyAndSubId(key, subId);
            return new ConfigModel(configTbl);
        } catch (DalException e) {
            logger.error("[getConfig]", e);
            return null;
        }
    }

    @Override
    public List<ConfigModel> getConfigs(String key) {
       // try {
            List<ConfigTbl> configTbl = new ArrayList<>();//configDao.getAllByKey(key);
            List<ConfigModel> configModels = new ArrayList<>();
            configTbl.forEach(config -> configModels.add(new ConfigModel(config)));
            return configModels;
       // } catch (DalException e) {
       //     logger.error("[getConfig]", e);
        //    return null;
       // }
    }

    private ConfigModel getOrCreate(String key, String defaultValue) {
        ConfigTbl configTbl = null;
        try {
            configTbl = configDao.getByKey(key);
        } catch (DalException e) {
            logger.error("[getOrCreate]", e);
        }
        if(configTbl == null) {
            return createConfig(key, defaultValue);
        }
        ConfigModel config = new ConfigModel();
        config.setKey(key);
        config.setVal(configTbl.getValue());
        config.setUpdateIP(configTbl.getLatestUpdateIp());
        config.setUpdateUser(configTbl.getLatestUpdateUser());

        return config;
    }

    private ConfigModel createConfig(String key, String defaultValue) {
        ConfigModel config = new ConfigModel();
        config.setKey(key);
        config.setVal(defaultValue);
        try {
            configDao.setConfig(config);
        } catch (DalException e) {
            logger.error("[createConfig]", e);
        }
        return config;
    }

    private boolean getAndResetTrueIfExpired(String key) {
        return getAndResetExpectIfExpired(key, true);
    }

    private boolean getAndResetFalseIfExpired(String key) {
        return getAndResetExpectIfExpired(key, false);
    }

    private boolean getAndResetExpectIfExpired(String key, boolean defaultVal) {
        try {
            ConfigTbl config = new ConfigTbl();
                    //configDao.getByKey(key);
            boolean result = Boolean.parseBoolean(config.getValue());
            if(result != defaultVal) {
                Date expireDate = config.getUntil();
                Date currentDate = new Date();
                ConfigModel configModel = new ConfigModel().setKey(key)
                        .setVal(String.valueOf(defaultVal)).setUpdateUser("System");
                if(currentDate.after(expireDate)) {
                    logger.info("[getAndResetExpectIfExpired] time expired, reset to be {}", defaultVal);
                    configDao.setConfig(configModel);
                    result = defaultVal;
                }
            }
            return result;
        } catch (Exception e) {
            return true;
        }
    }

    private void  logChangeEvent(ConfigModel configModel, Date recoveryDate) {
        StringBuilder sb = new StringBuilder();
        if (!StringUtil.isEmpty(configModel.getKey())) sb.append(configModel.getKey());
        if (!StringUtil.isEmpty(configModel.getSubKey())) sb.append(":").append(configModel.getSubKey());
        sb.append(" is set");
        if (!StringUtil.isEmpty(configModel.getVal())) sb.append(" to ").append(configModel.getVal());
        if (!StringUtil.isEmpty(configModel.getUpdateUser())) sb.append(" by ").append(configModel.getUpdateUser());
        if (!StringUtil.isEmpty(configModel.getUpdateIP())) sb.append(" ip ").append(configModel.getUpdateIP());
        if (null != recoveryDate) sb.append(" until ").append(DateTimeUtils.timeAsString(recoveryDate));
        EventMonitor.DEFAULT.logEvent(EVENT_CONFIG_CHANGE, sb.toString());
    }

    @VisibleForTesting
    public void setAutoMigrationOffChecker(AutoMigrationOffChecker checker) {
        this.autoMigrationOffChecker = checker;
    }

}
