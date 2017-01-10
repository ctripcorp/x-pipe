package com.ctrip.xpipe.api.migration;

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.List;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author shyin
 *
 * Dec 22, 2016
 */
public interface MigrationPublishService extends Ordered{
	
	public static MigrationPublishService DEFAULT = ServicesUtil.getMigrationPublishService();
	
	MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters);
	
	MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName, InetSocketAddress newMaster);
	
	public static class MigrationPublishResult {
		private boolean Success;
		private String Message;
		
		private Date startTime;
		private Date endTime;
		private String publishAddress;
		private String clusterName;
		private String primaryDcName;
		private List<InetSocketAddress> newMasters;
		
		public MigrationPublishResult() {
			
		}
		
		public MigrationPublishResult(String publishAddress, String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) {
			this.publishAddress = publishAddress;
			this.clusterName = clusterName;
			this.primaryDcName = primaryDcName;
			this.newMasters = newMasters;
		}
		
		public boolean isSuccess() {
			return Success;
		}

		public void setSuccess(boolean success) {
			Success = success;
		}

		public String getMessage() {
			return Message;
		}

		public void setMessage(String message) {
			Message = message;
		}
		
		public String getPublishAddress() {
			return publishAddress;
		}

		public void setPublishAddress(String publishAddress) {
			this.publishAddress = publishAddress;
		}

		public String getClusterName() {
			return clusterName;
		}

		public void setClusterName(String clusterName) {
			this.clusterName = clusterName;
		}

		public String getPrimaryDcName() {
			return primaryDcName;
		}

		public void setPrimaryDcName(String primaryDcName) {
			this.primaryDcName = primaryDcName;
		}

		public List<InetSocketAddress> getNewMasters() {
			return newMasters;
		}

		public void setNewMasters(List<InetSocketAddress> newMasters) {
			this.newMasters = newMasters;
		}
		
		public Date getStartTime() {
			return startTime;
		}

		public void setStartTime(Date startTime) {
			this.startTime = startTime;
		}

		public Date getEndTime() {
			return endTime;
		}

		public void setEndTime(Date endTime) {
			this.endTime = endTime;
		}
		
		@Override
		public String toString() {
			return Codec.DEFAULT.encode(this);
		}
	}
}
