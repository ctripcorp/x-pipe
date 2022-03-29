package com.ctrip.xpipe.redis.core.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * @author wenchao.meng
 *         <p>
 *         May 04, 2017
 */
public interface Shard {

    <T> T parent();

    @JsonIgnore
    default String getActiveDc(){
        if (parent() instanceof Cluster) {
            return ((Cluster) parent()).getActiveDc();
        } else {
            return ((Source) parent()).parent().getActiveDc();
        }
    }

    @JsonIgnore
    default String getBackupDcs(){
        if (parent() instanceof Cluster) {
            return ((Cluster) parent()).getBackupDcs();
        } else {
            return ((Source) parent()).parent().getBackupDcs();
        }
    }
}
