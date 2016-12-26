package com.ctrip.xpipe.api.migration;

import java.net.InetSocketAddress;
import java.util.List;

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

		@Override
		public String toString() {
			return String.format("Success:%s, Message:%s", Success, Message);
		}
	}
}
