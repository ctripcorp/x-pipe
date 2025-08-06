package com.ctrip.xpipe.redis.checker.healthcheck.actions.interaction;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.api.migration.OuterClientService.HostPortDcStatus;
import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Set;

public interface AggregatorPullService {

    Set<HostPortDcStatus> getNeedAdjustInstances(String cluster, Set<HostPort> instances) throws Exception;

    void doMarkInstances(String clusterName, String activeDc, Set<HostPortDcStatus> instances) throws OuterClientException;

    void doMarkInstancesIfNoModifyFor(String clusterName, String activeDc, Set<HostPortDcStatus> instances, long noModifySeconds) throws OuterClientException;

    String dcInstancesAllUp(String clusterName, Set<HostPort> instancesToMarkup);
}
