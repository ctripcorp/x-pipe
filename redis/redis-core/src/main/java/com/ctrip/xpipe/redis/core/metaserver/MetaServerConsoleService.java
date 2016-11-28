package com.ctrip.xpipe.redis.core.metaserver;


import org.springframework.http.ResponseEntity;
import org.springframework.util.concurrent.ListenableFuture;

import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * used for console
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerConsoleService extends MetaServerService{
	
	public static final String PATH_CLUSTER_CHANGE = "/clusterchange/{clusterId}";

	void clusterAdded(String clusterId, ClusterMeta clusterMeta);

	void clusterModified(String clusterId, ClusterMeta clusterMeta);

	void clusterDeleted(String clusterId);
	
	/**
	 * change primary dc to newdc
	 * @param clusterId
	 * @param shardId
	 * @param primaryDc
	 * @param eventId
	 * @return
	 */
	ChangePrimaryDcResult changePrimaryDc(String clusterId, String shardId, String primaryDc, long eventId);

	ListenableFuture<ResponseEntity<String>> getChangePrimaryDcStatus(long eventId, long offset);
	
	DcMeta getDynamicInfo();

	
	public static class ChangePrimaryDcResult{
		
		private CHANGE_PRIMARY_DC_STATUS  changePrimaryDcStatus;
		private String desc;
		
		public ChangePrimaryDcResult(CHANGE_PRIMARY_DC_STATUS  changePrimaryDcStatus, String desc){
			this.changePrimaryDcStatus = changePrimaryDcStatus;
			this.desc = desc;
		}
		
		public CHANGE_PRIMARY_DC_STATUS getChangePrimaryDcStatus() {
			return changePrimaryDcStatus;
		}
		public String getDesc() {
			return desc;
		}
	}
	
	public static enum CHANGE_PRIMARY_DC_STATUS{
		SUCCESS,
		ALREADY_DOING,
		CLUSTER_ID_NOT_FOUND,
		SHARTD_ID_NOT_FOUND
	}
	
}
