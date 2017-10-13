package com.ctrip.xpipe.migration;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.migration.OuterClientService;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public abstract class AbstractOuterClientService implements OuterClientService {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public int getOrder() {
		return LOWEST_PRECEDENCE;
	}

	@Override
	public String serviceName() {
		return getClass().getSimpleName();
	}

	@Override
	public void markInstanceUp(ClusterShardHostPort hostPort) throws OuterClientException {

	}

	@Override
	public boolean isInstanceUp(ClusterShardHostPort hostPort) throws OuterClientException {
		return false;
	}

	@Override
	public void markInstanceDown(ClusterShardHostPort hostPort) throws OuterClientException {

	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) throws OuterClientException {
		return null;
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName, InetSocketAddress newMaster) throws OuterClientException {
		return null;
	}

	@Override
	public ClusterInfo getClusterInfo(String clusterName) throws Exception {
		return null;
	}
}
