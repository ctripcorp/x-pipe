package com.ctrip.xpipe.redis.core.entity;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public interface Cluster {

    String getActiveDc();

    String getBackupDcs();
}
