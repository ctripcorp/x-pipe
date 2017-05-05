package com.ctrip.xpipe.redis.core.entity;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public interface Shard {

    Cluster parent();

    default String getActiveDc(){
        return parent().getActiveDc();
    }

    default String getBackupDcs(){
        return parent().getBackupDcs();
    }
}
