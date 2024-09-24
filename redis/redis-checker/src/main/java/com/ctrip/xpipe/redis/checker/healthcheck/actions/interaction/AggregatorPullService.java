package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService.*;
import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Set;

public interface AggregatorPullService {

    Set<HostPortDcStatus> getNeedAdjustInstances(String cluster, Set<HostPort> instances) throws Exception;

    void doMarkInstances(String clusterName, Set<HostPortDcStatus> instances) throws OuterClientException;

}
