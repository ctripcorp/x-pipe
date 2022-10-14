package com.ctrip.xpipe.redis.checker.healthcheck;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.cluster.DcGroupType;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface CheckInfo {

    String getClusterId();

    ClusterType getClusterType();

    String getActiveDc();

    void setActiveDc(String activeDc);

    List<RedisCheckRule> getRedisCheckRules();

    void setDcGroupType(DcGroupType type);

    DcGroupType getDcGroupType();
}
