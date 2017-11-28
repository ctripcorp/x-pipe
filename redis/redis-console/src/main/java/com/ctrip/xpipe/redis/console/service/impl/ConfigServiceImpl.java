package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.alert.AlertManager;
import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleDbConfig;
import com.ctrip.xpipe.redis.console.dao.ConfigDao;
import com.ctrip.xpipe.redis.console.model.ConfigTbl;
import com.ctrip.xpipe.redis.console.service.ConfigService;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.utils.DateTimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.unidal.dal.jdbc.DalException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


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
    private AlertManager alertManager;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private Future future;

    private Logger logger = LoggerFactory.getLogger(ConfigServiceImpl.class);

    @PreDestroy
    public void whenShutDown() {
        future.cancel(true);
    }

    @PostConstruct
    public void startUp() {
        future = scheduled.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if(!isAlertSystemOn()) {
                    String message = "Alert System will recover on " + getAlertSystemRecoverTime().toString();
                    alertManager.alert(null, null, null, ALERT_TYPE.ALERT_SYSTEM_OFF, message);
                }
            }
        }, 0, 1, TimeUnit.HOURS);
    }


    @Override
    public void startAlertSystem() throws DalException {

        logger.info("[startAlertSystem] start alert system");
        configDao.setKey(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON, String.valueOf(true));
    }

    @Override
    public void stopAlertSystem(int hours) throws DalException {

        logger.info("[stopAlertSystem] stop alert system");
        Date date = DateTimeUtils.getHoursLaterDate(hours);
        if(isAlertSystemOn()) {
            alertManager.alert(null, null, null, ALERT_TYPE.ALERT_SYSTEM_OFF, ALERT_TYPE.ALERT_SYSTEM_OFF.simpleDesc());
        }
        configDao.setKeyAndUntil(DefaultConsoleDbConfig.KEY_ALERT_SYSTEM_ON,
                String.valueOf(false), date);
    }

    @Override
    public void startSentinelAutoProcess() throws DalException {

        logger.info("[startSentinelAutoProcess] start sentinel auto process");
        configDao.setKey(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS, String.valueOf(true));
    }

    @Override
    public void stopSentinelAutoProcess(int hours) throws DalException {

        logger.info("[stopSentinelAutoProcess] stop sentinel auto process");
        Date date = DateTimeUtils.getHoursLaterDate(hours);
        configDao.setKeyAndUntil(DefaultConsoleDbConfig.KEY_SENTINEL_AUTO_PROCESS,
                String.valueOf(false), date);

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

    private boolean getAndResetTrueIfExpired(String key) {
        try {
            ConfigTbl config = configDao.getByKey(key);
            boolean result = Boolean.valueOf(config.getValue());
            if(!result) {
                Date expireDate = config.getUntil();
                Date currentDate = new Date();
                if(currentDate.after(expireDate)) {
                    logger.info("[getAndResetTrueIfExpired] Off time expired, reset to be true");
                    configDao.setKey(key, String.valueOf(true));
                    result = true;
                }
            }
            return result;
        } catch (Exception e) {
            return true;
        }
    }
}
