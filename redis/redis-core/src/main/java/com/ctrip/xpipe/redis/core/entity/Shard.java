package com.ctrip.xpipe.redis.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public interface Shard {

    Cluster parent();

    @JsonIgnore
    default String getActiveDc(){
        return parent().getActiveDc();
    }

    @JsonIgnore
    default String getBackupDcs(){
        return parent().getBackupDcs();
    }
}
