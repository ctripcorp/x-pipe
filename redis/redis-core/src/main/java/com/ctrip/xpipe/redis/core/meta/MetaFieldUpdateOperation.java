package com.ctrip.xpipe.redis.core.meta;

/**
 * @author lishanglin
 * date 2022/1/4
 */
public interface MetaFieldUpdateOperation extends ReadWriteSafe {

    default boolean noneKeeperActive(String currentDc, String clusterId, String shardId) { return read(()->doNoneKeeperActive(currentDc, clusterId, shardId)); }
    boolean doNoneKeeperActive(String currentDc, String clusterId, String shardId);

    default void primaryDcChanged(String currentDc, String clusterId, String shardId, String newPrimaryDc) { read(()->doPrimaryDcChanged(currentDc, clusterId, shardId, newPrimaryDc)); }
    void doPrimaryDcChanged(String currentDc, String clusterId, String shardId, String newPrimaryDc);

}
