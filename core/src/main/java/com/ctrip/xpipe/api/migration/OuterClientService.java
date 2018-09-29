package com.ctrip.xpipe.api.migration;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.endpoint.ClusterShardHostPort;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

/**
 * for client router system
 * @author shyin
 *
 * Dec 22, 2016
 */
public interface OuterClientService extends Ordered{
	
	OuterClientService DEFAULT = ServicesUtil.getOuterClientService();

	String serviceName();

	void markInstanceUp(ClusterShardHostPort clusterShardHostPort) throws OuterClientException;

	boolean isInstanceUp(ClusterShardHostPort clusterShardHostPort) throws OuterClientException;

	void markInstanceDown(ClusterShardHostPort clusterShardHostPort) throws OuterClientException;

	MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) throws OuterClientException;
	
	MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName, InetSocketAddress newMaster) throws OuterClientException;

	ClusterInfo getClusterInfo(String clusterName) throws Exception;

	abstract class AbstractInfo {

		protected Logger logger = LoggerFactory.getLogger(getClass());

		@Override
		public String toString() {
			return JsonCodec.INSTANCE.encode(this);
		}
	}

	class MigrationPublishResult extends AbstractInfo{

		private boolean Success;
		private String Message;
		
		private String startTime;
		private String endTime;
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
		
		public String getStartTime() {
			return startTime;
		}

		public void setStartTime(String startTime) {
			this.startTime = startTime;
		}

		public String getEndTime() {
			return endTime;
		}

		public void setEndTime(String endTime) {
			this.endTime = endTime;
		}
		
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	class ClusterInfo extends AbstractInfo {

		public static int READ_ACTIVE_SLAVES = 2;

		private boolean isXpipe;
		private String masterIDC;
		private String name;
		private int rule;
		private String ruleName;
		private boolean usingIdc;
		private List<GroupInfo> groups = new LinkedList<>();

		public void check() {

			if (groups != null) {
				groups.forEach(groupMeta -> groupMeta.check());

			}
		}

		public void mapIdc(DC_TRANSFORM_DIRECTION direction){

			masterIDC = direction.transform(masterIDC);

			if (groups != null) {
				groups.forEach(groupMeta -> groupMeta.mapIdc(direction));

			}
		}

		public boolean isXpipe() {
			return isXpipe;
		}

		public void setIsXpipe(boolean xpipe) {
			isXpipe = xpipe;
		}

		public String getMasterIDC() {
			return masterIDC;
		}

		public void setMasterIDC(String masterIDC) {
			this.masterIDC = masterIDC;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getRule() {
			return rule;
		}

		public void setRule(int rule) {
			this.rule = rule;
		}

		public String getRuleName() {
			return ruleName;
		}

		public void setRuleName(String ruleName) {
			this.ruleName = ruleName;
		}

		public boolean isUsingIdc() {
			return usingIdc;
		}

		public void setUsingIdc(boolean usingIdc) {
			this.usingIdc = usingIdc;
		}

		public List<GroupInfo> getGroups() {
			return groups;
		}

		public void setGroups(List<GroupInfo> groups) {
			this.groups = groups;
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	class GroupInfo extends AbstractInfo {
		private String name;
		private List<InstanceInfo> instances;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<InstanceInfo> getInstances() {
			return instances;
		}

		public void setInstances(List<InstanceInfo> instances) {
			this.instances = instances;
		}

		public void mapIdc(DC_TRANSFORM_DIRECTION direction){

			if (instances != null) {
				instances.forEach(instanceMeta -> instanceMeta.mapIdc(direction));
			}
		}

		public void check() {
			if (instances != null) {
				instances.forEach(instanceMeta -> instanceMeta.check());
			}
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	class InstanceInfo extends AbstractInfo {

		private boolean canRead;
		private String env;
		private String IPAddress;
		private boolean isMaster;
		private int port;
		private boolean status;

		public void check() {

			String addr = String.format("%s:%d", IPAddress, port);
			if (!canRead) {
				throw new IllegalStateException("instance can not read:" + addr);
			}
			if (!status) {
				throw new IllegalStateException("instance not valid:" + addr);
			}

		}
		void mapIdc(DC_TRANSFORM_DIRECTION direction){
			env = direction.transform(env);
		}

		public boolean isCanRead() {
			return canRead;
		}

		public void setCanRead(boolean canRead) {
			this.canRead = canRead;
		}

		public String getEnv() {
			return env;
		}

		public void setEnv(String env) {
			this.env = env;
		}

		public String getIPAddress() {
			return IPAddress;
		}

		public void setIPAddress(String IPAddress) {
			this.IPAddress = IPAddress;
		}

		public boolean isMaster() {
			return isMaster;
		}

		public void setIsMaster(boolean master) {
			isMaster = master;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public boolean isStatus() {
			return status;
		}

		public void setStatus(boolean status) {
			this.status = status;
		}
	}
}
