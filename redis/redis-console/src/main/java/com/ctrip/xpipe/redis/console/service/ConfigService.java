package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.ConfigModel;
import org.unidal.dal.jdbc.DalException;

import java.util.Date;

/**
 * @author chen.zhu
 * <p>
 * Nov 27, 2017
 */
public interface ConfigService {

    void startAlertSystem(ConfigModel config) throws DalException;

    void stopAlertSystem(ConfigModel config, int hours) throws DalException;

    void startSentinelAutoProcess(ConfigModel config) throws DalException;

    void stopSentinelAutoProcess(ConfigModel config, int hours) throws DalException;

    boolean isAlertSystemOn();

    boolean isSentinelAutoProcess();

    Date getAlertSystemRecoverTime();

    Date getSentinelAutoProcessRecoverTime();

    boolean ignoreMigrationSystemAvailability();

    void doIgnoreMigrationSystemAvailability(boolean ignore) throws DalException;

    ConfigModel getConfig(String key);
}
