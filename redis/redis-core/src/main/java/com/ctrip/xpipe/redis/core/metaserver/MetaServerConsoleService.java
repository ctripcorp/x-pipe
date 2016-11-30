package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;

/**
 * used for console
 * @author wenchao.meng
 *
 * Aug 2, 2016
 */
public interface MetaServerConsoleService extends MetaServerService{
	
	
	void clusterAdded(String clusterId, ClusterMeta clusterMeta);

	void clusterModified(String clusterId, ClusterMeta clusterMeta);

	void clusterDeleted(String clusterId);
	
	/**
	 * @param clusterId
	 * @param shardId
	 * @param primaryDc
	 * @return 
	 * 0 : success
	 * 1 : already 
	 * other : fail
	 */
	PrimaryDcCheckMessage changePrimaryDcCheck(String clusterId, String shardId, String newPrimaryDc);
	
	/**
	 * just try it
	 * @param clusterId
	 * @param shardId
	 */
	void makeMasterReadOnly(String clusterId, String shardId);

	/**
	 * for new primary: promote redis, sync to redis<br/>
	 * for others: sync to new primary dc's active keeper
	 * @param clusterId
	 * @param shardId
	 * @param newPrimaryDc
	 * @return
	 */
	PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc);

	DcMeta getDynamicInfo();
	
	public static enum PRIMARY_DC_CHECK_RESULT{
		
		SUCCESS,
		PRIMARY_DC_ALREADY_IS_NEW,
		FAIL
	}
	
	public static class PrimaryDcCheckMessage extends ErrorMessage<PRIMARY_DC_CHECK_RESULT>{
		
		public PrimaryDcCheckMessage(){}

		public PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT errorType, String errorMessage) {
			super(errorType, errorMessage);
		}
		
	}
	
	public static enum PRIMARY_DC_CHANGE_RESULT{
		
		SUCCESS,
		FAIL
	}
	
	public static class PrimaryDcChangeMessage extends ErrorMessage<PRIMARY_DC_CHANGE_RESULT>{
		
		public PrimaryDcChangeMessage(){}

		public PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT errorType, String errorMessage) {
			super(errorType, errorMessage);
		}
	}
}
