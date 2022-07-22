package com.ctrip.xpipe.redis.core.metaserver;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.ErrorMessage;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;

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
	 * just try
	 * @param clusterId
	 * @param shardId
	 * @param readOnly  true mark as read only, false writable
	 */
	PreviousPrimaryDcMessage makeMasterReadOnly(String clusterId, String shardId, boolean readOnly);

	/**
	 * for new primary: promote redis, sync to redis<br/>
	 * for others: sync to new primary dc's active keeper
	 * @param clusterId
	 * @param shardId
	 * @param newPrimaryDc
	 * @return
	 */
	PrimaryDcChangeMessage doChangePrimaryDc(String clusterId, String shardId, String newPrimaryDc, PrimaryDcChangeRequest request);

	RedisMeta getCurrentMaster(String clusterId, String shardId);
	
	public static enum PRIMARY_DC_CHECK_RESULT{
		
		SUCCESS,
		PRIMARY_DC_ALREADY_IS_NEW,
		FAIL
	}

	public static class PreviousPrimaryDcMessage{

		private HostPort masterAddr;

		private MasterInfo masterInfo;

		private String message;

		public PreviousPrimaryDcMessage(){
		}

		public PreviousPrimaryDcMessage(HostPort masterAddr, MasterInfo masterInfo, String message){
			this.masterAddr = masterAddr;
			this.masterInfo = masterInfo;
			this.message = message;
		}

		public HostPort getMasterAddr() {
			return masterAddr;
		}

		public void setMasterAddr(HostPort masterAddr) {
			this.masterAddr = masterAddr;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		public void setMasterInfo(MasterInfo masterInfo) {
			this.masterInfo = masterInfo;
		}

		public MasterInfo getMasterInfo() {
			return masterInfo;
		}

		public String getMessage() {
			return message;
		}

		@Override
		public String toString() {
			return String.format("master:%s, masterInfo:%s, log:%s", masterAddr, masterInfo, message);
		}
	}

	
	public static class PrimaryDcCheckMessage extends ErrorMessage<PRIMARY_DC_CHECK_RESULT>{
		
		public PrimaryDcCheckMessage(){}

		public PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT errorType, String errorMessage) {
			super(errorType, errorMessage);
		}

		public PrimaryDcCheckMessage(PRIMARY_DC_CHECK_RESULT errorType) {
			super(errorType, null);
		}

		public boolean isSuccess() {
			return PRIMARY_DC_CHECK_RESULT.SUCCESS.equals(getErrorType());
		}
	}
	
	public static enum PRIMARY_DC_CHANGE_RESULT{
		
		SUCCESS,
		FAIL
	}

	public static class PrimaryDcChangeRequest {

		private MasterInfo masterInfo;

		public PrimaryDcChangeRequest(){
		}

		public PrimaryDcChangeRequest(PreviousPrimaryDcMessage previousPrimaryDcMessage){
			if(previousPrimaryDcMessage != null){
				this.masterInfo = previousPrimaryDcMessage.getMasterInfo();
			}
		}

		public PrimaryDcChangeRequest(MasterInfo masterInfo){
			this.masterInfo = masterInfo;
		}

		public MasterInfo getMasterInfo() {
			return masterInfo;
		}

		public void setMasterInfo(MasterInfo masterInfo) {
			this.masterInfo = masterInfo;
		}

		@Override
		public String toString() {
			return String.format("masterInfo: %s", masterInfo);
		}
	}

	public static class PrimaryDcChangeMessage extends ErrorMessage<PRIMARY_DC_CHANGE_RESULT>{
		
		private String newMasterIp;
		
		private int newMasterPort;
		
		public PrimaryDcChangeMessage(){}

		public PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT errorType, String errorMessage) {
			super(errorType, errorMessage);
		}
		
		public PrimaryDcChangeMessage(String message, String newMasterIp, int newMasterPort){
			super(PRIMARY_DC_CHANGE_RESULT.SUCCESS, message);
			this.newMasterIp = newMasterIp;
			this.newMasterPort = newMasterPort;
		}
		
		public PrimaryDcChangeMessage(PRIMARY_DC_CHANGE_RESULT errorType) {
			super(errorType, null);
		}
		
		public String getNewMasterIp() {
			return newMasterIp;
		}
		
		public int getNewMasterPort() {
			return newMasterPort;
		}

		public boolean isSuccess() {
			return PRIMARY_DC_CHANGE_RESULT.SUCCESS.equals(getErrorType());
		}
		
		@Override
		public String toString() {
			
			if(getErrorType() == PRIMARY_DC_CHANGE_RESULT.SUCCESS){
				return String.format("code:%s, newmaster:%s:%d, message:%s", getErrorType(), newMasterIp, newMasterPort, getErrorMessage());
			}
			return super.toString();
		}
	}
}
