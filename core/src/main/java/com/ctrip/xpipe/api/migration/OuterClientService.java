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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

	boolean clusterMigratePreCheck(String clusterName) throws OuterClientException;

	MigrationPublishResult doMigrationPublish(String clusterName, String primaryDcName, List<InetSocketAddress> newMasters) throws OuterClientException;
	
	MigrationPublishResult doMigrationPublish(String clusterName, String shardName, String primaryDcName, InetSocketAddress newMaster) throws OuterClientException;

	ClusterInfo getClusterInfo(String clusterName) throws Exception;

	DcMeta getOutClientDcMeta(String dc) throws Exception;

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

	@JsonIgnoreProperties(ignoreUnknown = true)
	class DcMeta extends AbstractInfo{
		private String dcName;

		private long regionId;

		private Map<String, ClusterMeta> clusters = new ConcurrentHashMap<>();

		public String getDcName() {
			return dcName;
		}

		public DcMeta setDcName(String dcName) {
			this.dcName = dcName;
			return this;
		}

		public long getRegionId() {
			return regionId;
		}

		public void setRegionId(long regionId) {
			this.regionId = regionId;
		}

		public Map<String, ClusterMeta> getClusters() {
			return clusters;
		}

		public void setClusters(Map<String, ClusterMeta> clusters) {
			this.clusters = clusters;
		}

		public void mapIdc(DC_TRANSFORM_DIRECTION direction){

			dcName = direction.transform(dcName);
			clusters.values().forEach(clusterMeta -> {
				clusterMeta.mapIdc(direction);
			});
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	class ClusterMeta extends AbstractInfo{

		private String name;

		private String lastModifiedTime;

		private String activeIDC;

		private ClusterType clusterType;

		private Integer orgId;

		private String ownerEmails;

		private Map<String, GroupMeta> groups = new ConcurrentHashMap<>();

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getLastModifiedTime() {
			return lastModifiedTime;
		}

		public void setLastModifiedTime(String lastModifiedTime) {
			this.lastModifiedTime = lastModifiedTime;
		}

		public String getActiveIDC() {
			return activeIDC;
		}

		public void setActiveIDC(String activeIDC) {
			this.activeIDC = activeIDC;
		}

		public ClusterType getClusterType() {
			return clusterType;
		}

		public void setClusterType(ClusterType clusterType) {
			this.clusterType = clusterType;
		}

		public Map<String, GroupMeta> getGroups() {
			return groups;
		}

		public void setGroups(Map<String, GroupMeta> groups) {
			this.groups = groups;
		}

		public Integer getOrgId() {
			return orgId;
		}

		public void setOrgId(Integer orgId) {
			this.orgId = orgId;
		}

		public String getOwnerEmails() {
			return ownerEmails;
		}

		public void setOwnerEmails(String ownerEmails) {
			this.ownerEmails = ownerEmails;
		}

		public void mapIdc(DC_TRANSFORM_DIRECTION direction){

			activeIDC = direction.transform(activeIDC);

			if (groups != null) {
				groups.values().forEach(groupMeta -> groupMeta.mapIdc(direction));
			}
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	class GroupMeta {

		private String clusterName;

		private String groupName;

		private List<RedisMeta> redises;

		public String getClusterName() {
			return clusterName;
		}

		public void setClusterName(String clusterName) {
			this.clusterName = clusterName;
		}

		public String getGroupName() {
			return groupName;
		}

		public void setGroupName(String groupName) {
			this.groupName = groupName;
		}

		public List<RedisMeta> getRedises() {
			return redises;
		}

		public void setRedises(List<RedisMeta> redises) {
			this.redises = redises;
		}

		public RedisMeta getMaster() {
			for (RedisMeta redisMeta : redises) {
				if (redisMeta.master)
					return redisMeta;
			}
			return null;
		}

		public void mapIdc(DC_TRANSFORM_DIRECTION direction){

			if (redises != null) {
				redises.forEach(redisMeta -> redisMeta.mapIdc(direction));
			}
		}

	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	class RedisMeta {

		private String host;

		private int port;

		private boolean master;

		private InstanceStatus status;

		private String idc;

		public void mapIdc(DC_TRANSFORM_DIRECTION direction){
			idc = direction.transform(idc);
		}

		public String getHost() {
			return host;
		}

		public void setHost(String host) {
			this.host = host;
		}

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}

		public boolean isMaster() {
			return master;
		}

		public void setMaster(boolean master) {
			this.master = master;
		}

		public InstanceStatus getStatus() {
			return status;
		}

		public void setStatus(InstanceStatus status) {
			this.status = status;
		}

		public String getIdc() {
			return idc;
		}

		public void setIdc(String idc) {
			this.idc = idc;
		}

	}

	enum ClusterType {
		SINGEL_DC(0),
		LOCAL_DC(2),
		XPIPE_ONE_WAY(3),
		XPIPE_BI_DIRECT(4),
		TROCKS(5);

		private Integer intVal;

		ClusterType(int intVal) {
			this.intVal = intVal;
		}

		public Integer getIntVal() {
			return intVal;
		}

		public static ClusterType valueOf(Integer intVal){
			for (ClusterType type : ClusterType.values()) {
				if (type.getIntVal().equals(intVal)) {
					return type;
				}
			}
			return SINGEL_DC;
		}
	}

	enum InstanceStatus {
		MANUAL_MARKDOWN(-2),
		INACTIVE(0),
		ACTIVE(1);

		private Integer intVal;

		InstanceStatus(int intVal) {
			this.intVal = intVal;
		}

		public Integer intValue() {
			return intVal;
		}

		public static InstanceStatus valueOf(Integer intVal){
			for (InstanceStatus status : InstanceStatus.values()) {
				if (status.intValue().equals(intVal)) {
					return status;
				}
			}
			return InstanceStatus.INACTIVE;
		}
	}

}
