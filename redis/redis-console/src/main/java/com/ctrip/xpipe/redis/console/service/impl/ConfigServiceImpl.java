package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.election.CrossDcLeaderElectionAction;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.AlertSystemOffChecker;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.SentinelAutoProcessChecker;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;


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
    private AlertSystemOffChecker alertSystemOffChecker;

    @Autowired
    private SentinelAutoProcessChecker sentinelAutoProcessChecker;

    private Logger logger = LoggerFactory.getLogger(ConfigServiceImpl.class);

    private static final String EVENT_CONFIG_CHANGE = "event_config_change";

    @Override
    public void startAlertSystem(ConfigModel config) throws DalException {

        logger.info("[startAlertSystem] start alert system, config: {}", config);
        config.setKey(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON).setVal(String.valueOf(true));
        configDao.setConfig(config);
    }

    @Override
    public void stopAlertSystem(ConfigModel config, int hours) throws DalException {


        Date date = DateTimeUtils.getHoursLaterDate(hours);
        boolean previousStateOn = isAlertSystemOn();

        config.setKey(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON).setVal(String.valueOf(false));
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
        config.setKey(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS).setVal(String.valueOf(true));
        configDao.setConfig(config);
    }

    @Override
    public void stopSentinelAutoProcess(ConfigModel config, int hours) throws DalException {

        logger.info("[stopSentinelAutoProcess] stop sentinel auto process, config: {}", config);
        Date date = DateTimeUtils.getHoursLaterDate(hours);
        boolean previousStateOn = isSentinelAutoProcess();

        config.setKey(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS).setVal(String.valueOf(false));
        configDao.setConfigAndUntil(config, date);
        if(previousStateOn) {
            sentinelAutoProcessChecker.startAlert();
        }

    }

    @Override
    public void startSentinelCheck(ConfigModel config) throws DalException {
        logger.info("[startSentinelCheck] : turn off sentinel check exclude config {} for cluster {}", config, config.getSubKey());

        config.setKey(DefaultConsoleDbConfig.KEY_SENTINEL_CHECK_EXCLUDE)
                .setVal(String.valueOf(false));
        logChangeEvent(config, null);
        configDao.setConfig(config);
    }

    @Override
    public void stopSentinelCheck(ConfigModel config, int minutes) throws DalException {
        logger.info("[stopSentinelCheck] : turn on sentinel check exclude config {} for cluster {} till {} minutes later",
                config, config.getSubKey(), minutes);

        Date date = DateTimeUtils.getMinutesLaterThan(new Date(), minutes);
        config.setKey(DefaultConsoleDbConfig.KEY_SENTINEL_CHECK_EXCLUDE)
                .setVal(String.valueOf(true));
        logChangeEvent(config, date);
        configDao.setConfigAndUntil(config, date);
    }

    public void updateCrossDcLeader(ConfigModel config, Date until) throws DalException {
        logger.info("[updateCrossDcLeader] update lease to {} until {}", config.getVal(), until);
        config.setKey(CrossDcLeaderElectionAction.KEY_LEASE_CONFIG);
        config.setSubKey(CrossDcLeaderElectionAction.SUB_KEY_CROSS_DC_LEADER);

        configDao.setConfig(config, until);
    }

    public String getCrossDcLeader() throws DalException {
        ConfigTbl leaseConfig = configDao.getByKeyAndSubId(CrossDcLeaderElectionAction.KEY_LEASE_CONFIG,
                CrossDcLeaderElectionAction.SUB_KEY_CROSS_DC_LEADER);

        // only return when lease is active
        if (new Date().compareTo(leaseConfig.getUntil()) < 0) return leaseConfig.getValue();
        else return null;
    }

    @Override
    public boolean shouldSentinelCheck(String cluster) {
        try {
            ConfigTbl config = configDao.getByKeyAndSubId(DefaultConsoleDbConfig.KEY_SENTINEL_CHECK_EXCLUDE, cluster);
            return null == config || !Boolean.parseBoolean(config.getValue()) || (new Date()).after(config.getUntil());
        } catch (Exception e) {
            return true;
        }
    }


    @Override
    public List<ConfigModel> getActiveSentinelCheckExcludeConfig() {
        List<ConfigTbl> configTbls = configDao.findAllByKeyAndValueAndUntilAfter(
                DefaultConsoleDbConfig.KEY_SENTINEL_CHECK_EXCLUDE, String.valueOf(true), new Date());
        if (configTbls.isEmpty()) return Collections.emptyList();
        List<ConfigModel> models = new ArrayList<>();

        configTbls.forEach(configTbl -> {
            models.add(new ConfigModel(configTbl));
        });

        return models;
    }

    @Override
    public boolean isAlertSystemOn() {
        return getAndResetTrueIfExpired(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON);
    }

    @Override
    public boolean isSentinelAutoProcess() {
        return getAndResetTrueIfExpired(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS);
    }

    @Override
    public Date getAlertSystemRecoverTime() {
        try {
            return configDao.getByKey(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON).getUntil();
        } catch (DalException e) {
            logger.error("[getAlertSystemRecovertIME] {}", e);
            return null;
        }
    }

    @Override
    public Date getSentinelAutoProcessRecoverTime() {
        try {
            return configDao.getByKey(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS).getUntil();
        } catch (DalException e) {
            logger.error("[getAlertSystemRecovertIME] {}", e);
            return null;
        }
    }

    @Override
    public boolean ignoreMigrationSystemAvailability() {
        try {
            ConfigModel configModel = getOrCreate(DefaultConsoleDbConfig.KEY_IGNORE_MIGRATION_SYSTEM_AVAILABILITY, String.valueOf(false));
            return Boolean.parseBoolean(configModel.getVal());
        } catch (Exception e) {
            logger.error("[ignoreMigrationSystemAvailability]", e);
            return false;
        }
    }

    @Override
    public void doIgnoreMigrationSystemAvailability(boolean ignore) {
        try {
            ConfigModel configModel = getOrCreate(DefaultConsoleDbConfig.KEY_IGNORE_MIGRATION_SYSTEM_AVAILABILITY, String.valueOf(ignore));
            configModel.setVal(String.valueOf(ignore));
            configDao.setConfig(configModel);
        } catch (Exception e) {
            logger.error("[ignoreMigrationSystemAvailability]", e);
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
            logger.error("[getConfig] {}", e);
            return null;
        }
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
        try {
            ConfigTbl config = configDao.getByKey(key);
            boolean result = Boolean.valueOf(config.getValue());
            if(!result) {
                Date expireDate = config.getUntil();
                Date currentDate = new Date();
                ConfigModel configModel = new ConfigModel().setKey(key)
                        .setVal(String.valueOf(true)).setUpdateUser("System");
                if(currentDate.after(expireDate)) {
                    logger.info("[getAndResetTrueIfExpired] Off time expired, reset to be true");
                    configDao.setConfig(configModel);
                    result = true;
                }
            }
            return result;
        } catch (Exception e) {
            return true;
        }
    }

    private void logChangeEvent(ConfigModel configModel, Date recoveryDate) {
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
}
