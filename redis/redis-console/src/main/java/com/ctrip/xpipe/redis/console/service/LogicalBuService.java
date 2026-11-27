package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.LogicalBuModel;

import java.util.List;
import java.util.Map;

public interface LogicalBuService {

    List<LogicalBuModel> findAll();

    LogicalBuModel findById(long id);

    LogicalBuModel create(LogicalBuModel model);

    LogicalBuModel update(long id, LogicalBuModel model);

    void delete(long id);

    /**
     * Resolve logical BU for new cluster: active candidates for org, hash by cluster name.
     * Returns 0 if org unbound or no candidate.
     */
    long resolveLogicalBuIdForCluster(String clusterName, long clusterOrgId);

    /**
     * Logical BU id → priority, only for BUs whose priority is non-NULL. One query, used on meta build paths.
     */
    Map<Long, Integer> findAllPriorities();

    /**
     * Optimistic-lock update of priority. {@code expected == null} matches a NULL column; {@code target == null}
     * clears the override (keepers fall back to REDIS_TBL.keeper_priority).
     * @return true if exactly one row was updated
     */
    boolean casPriority(long id, Integer expected, Integer target);
}
