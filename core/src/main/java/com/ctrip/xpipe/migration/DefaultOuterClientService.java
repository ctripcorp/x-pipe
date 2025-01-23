package com.ctrip.xpipe.migration;

import com.ctrip.xpipe.api.migration.OuterClientException;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.utils.DateTimeUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author shyin
 *
 *         Dec 22, 2016
 */
public class DefaultOuterClientService extends AbstractOuterClientService {

	private Map<HostPort, Boolean> instanceStatus = new ConcurrentHashMap<>();

	private Map<String, Integer> cntMap = new ConcurrentHashMap<>();

	@Override
	public void markInstanceUp(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {
		logger.info("[markInstanceUp]{}", clusterShardHostPort);
		instanceStatus.put(clusterShardHostPort.getHostPort(), true);

	}

	@Override
	public boolean isInstanceUp(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {

		Boolean result = instanceStatus.get(clusterShardHostPort.getHostPort());
		if(result == null){
			return Boolean.parseBoolean(System.getProperty("InstanceUp", "true"));
		}
		return result;
	}

	@Override
	public Map<HostPort, Boolean> batchQueryInstanceStatus(String cluster, Set<HostPort> instances) throws OuterClientException {
		Map<HostPort, Boolean> result = new HashMap<>();
		for (HostPort instance: instances) {
			result.put(instance, isInstanceUp(new ClusterShardHostPort(instance)));
		}
		return result;
	}

	@Override
	public void markInstanceDown(ClusterShardHostPort clusterShardHostPort) throws OuterClientException {
		logger.info("[markInstanceDown]{}", clusterShardHostPort);
		instanceStatus.put(clusterShardHostPort.getHostPort(), false);
	}

	@Override
	public boolean clusterMigratePreCheck(String clusterName) throws OuterClientException {
		logger.info("[clusterMigratePreCheck]{}", clusterName);
		return true;
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) throws OuterClientException{
		logger.info("[doMigrationPublish]Cluster:{}, NewPrimaryDc:{}, Masters:{}", clusterName, primaryDcName,
				newMasters);
		String startTime = DateTimeUtils.currentTimeAsString();
		MigrationPublishResult res = new MigrationPublishResult("default-addr", clusterName, primaryDcName, newMasters);
		String endTime = DateTimeUtils.currentTimeAsString();
		res.setSuccess(true);res.setMessage("default-success");
		res.setStartTime(startTime);
		res.setEndTime(endTime);
		return res;
	}

	@Override
	public MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName,
			InetSocketAddress newMaster) throws OuterClientException{

		logger.info("[doMigrationPublish]Cluster:{}, Shard:{}, NewPrimaryDc:{}, NewMaster:{}", clusterName, shardName,
				primaryDcName, newMaster);
		String startTime = DateTimeUtils.currentTimeAsString();
		MigrationPublishResult res = new MigrationPublishResult("default-addr", clusterName, primaryDcName, Arrays.asList(newMaster));
		String endTime = DateTimeUtils.currentTimeAsString();
		res.setSuccess(true);res.setMessage("default-success");
		res.setStartTime(startTime);
		res.setEndTime(endTime);
		return res;
	}

	@Override
	public ClusterInfo getClusterInfo(String clusterName) {
		ClusterInfo clusterInfo = new ClusterInfo();
		clusterInfo.setName(clusterName);
		clusterInfo.setGroups(Lists.newArrayList(new GroupInfo()));
		return clusterInfo;
	}

	@Override
	public List<ClusterInfo> getActiveDcClusters(String dc) throws Exception {
		return Collections.emptyList();
	}

	@Override
	public DcMeta getOutClientDcMeta(String dc) throws Exception {
		return new DcMeta();
	}

	@Override
	public boolean excludeIdcs(String clusterName, String[] idcs) throws Exception {
		return true;
	}

	@Override
	public boolean batchExcludeIdcs(List<ClusterExcludedIdcInfo> excludedClusterIdcs) throws Exception {
		return true;
	}

	@Override
	public void markInstanceUpIfNoModifyFor(ClusterShardHostPort clusterShardHostPort, long noModifySeconds) throws OuterClientException {
		logger.info("[markInstanceUpIfNoModifyFor]{}", clusterShardHostPort);
		instanceStatus.put(clusterShardHostPort.getHostPort(), true);
	}

	@Override
	public void markInstanceDownIfNoModifyFor(ClusterShardHostPort clusterShardHostPort, long noModifySeconds) throws OuterClientException {
		logger.info("[markInstanceDownIfNoModifyFor]{}", clusterShardHostPort);
		instanceStatus.put(clusterShardHostPort.getHostPort(), false);
	}

	@Override
	public void batchMarkInstance(MarkInstanceRequest markInstanceRequest) throws OuterClientException {
		logger.info("[batchMarkInstance]{}", markInstanceRequest);
		for (HostPortDcStatus hostPortDcStatus : markInstanceRequest.getHostPortDcStatuses()) {
			instanceStatus.put(new HostPort(hostPortDcStatus.getHost(), hostPortDcStatus.getPort()), hostPortDcStatus.isCanRead());
		}
		this.cntMap = markInstanceRequest.getInstanceCnt();
	}

	@Override
	public OuterClientDataResp<List<ClusterExcludedIdcInfo>> getAllExcludedIdcs() throws Exception {
		OuterClientDataResp<List<ClusterExcludedIdcInfo>> resp = new OuterClientDataResp<>();
		resp.setSuccess(true);
		resp.setResult(Collections.emptyList());
		return resp;
	}

	public Map<String, Integer> getCntMap() {
		return cntMap;
	}
}
