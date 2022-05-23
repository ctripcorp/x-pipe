package com.ctrip.xpipe.redis.core.entity;

/**
 * @author ayq
 * <p>
 * 2022/4/1 16:10
 */
public interface InstanceNode {

    String getIp();

    Integer getPort();

    default String desc() {
        return String.format("%s(%s:%d)", getClass().getSimpleName(), getIp(), getPort());
    }
}
