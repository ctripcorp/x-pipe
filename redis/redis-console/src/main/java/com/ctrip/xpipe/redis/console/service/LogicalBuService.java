package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.console.model.LogicalBuModel;

import java.util.List;

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
}
