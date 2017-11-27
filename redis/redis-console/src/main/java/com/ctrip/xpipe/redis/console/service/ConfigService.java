package com.ctrip.xpipe.redis.console.service;

import org.unidal.dal.jdbc.DalException;

/**
 * @author chen.zhu
 * <p>
 * Nov 27, 2017
 */
public interface ConfigService {

    void startAlertSystem() throws DalException;

    void stopAlertSystem(int hours) throws DalException;

    void startSentinelAutoProcess() throws DalException;

    void stopSentinelAutoProcess(int hours) throws DalException;

    boolean isAlertSystemOn();

    boolean isSentinelAutoProcess();

}
