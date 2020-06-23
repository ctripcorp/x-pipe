package com.ctrip.xpipe.redis.meta.server.keeper;

import com.ctrip.xpipe.cluster.ClusterType;

import java.util.Set;

public interface ClusterTypeAware {

    Set<ClusterType> getSupportClusterTypes();

}
