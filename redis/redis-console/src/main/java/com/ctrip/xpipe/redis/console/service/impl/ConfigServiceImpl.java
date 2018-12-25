package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.exception.DalUpdateException;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.AlertSystemOffChecker;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.console.SentinelAutoProcessChecker;
import com.ctrip.xpipe.redis.console.model.ConfigModel;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import java.util.Date;


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
    AlertSystemOffChecker alertSystemOffChecker;

    @Autowired
    SentinelAutoProcessChecker sentinelAutoProcessChecker;

    private Logger logger = LoggerFactory.getLogger(ConfigServiceImpl.class);

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
        try {
            ConfigTbl configTbl = configDao.getByKey(key);

            ConfigModel config = new ConfigModel();
            config.setKey(key);
            config.setVal(configTbl.getValue());
            config.setUpdateIP(configTbl.getLatestUpdateIp());
            config.setUpdateUser(configTbl.getLatestUpdateUser());

            return config;
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
}
