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

    void startAlertSystem(ConfigModel config) throws DalException;

    void stopAlertSystem(ConfigModel config, int hours) throws DalException;

    void startSentinelAutoProcess(ConfigModel config) throws DalException;

    void stopSentinelAutoProcess(ConfigModel config, int hours) throws DalException;

    void startSentinelCheck(ConfigModel config) throws DalException;

    void stopSentinelCheck(ConfigModel config, int minutes) throws DalException;

    void updateCrossDcLeader(ConfigModel config, Date until) throws DalException;
    
    String getCrossDcLeader() throws DalException;

    boolean shouldSentinelCheck(String cluster);

    List<ConfigModel> getActiveSentinelCheckExcludeConfig();

    boolean isAlertSystemOn();

    boolean isSentinelAutoProcess();

    Date getAlertSystemRecoverTime();

    Date getSentinelAutoProcessRecoverTime();

    boolean ignoreMigrationSystemAvailability();

    void doIgnoreMigrationSystemAvailability(boolean ignore) throws DalException;

    ConfigModel getConfig(String key);

    ConfigModel getConfig(String key, String subId);
}
