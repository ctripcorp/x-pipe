package com.ctrip.xpipe.redis.keeper;




import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.LifecycleStateAware;
import com.ctrip.xpipe.api.server.PartialAware;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.impl.CreateRdbDumperException;


/**
 * @author wenchao.meng
 *
 * May 20, 2016 3:54:13 PM
 */
public interface RedisMaster extends RedisRole, Lifecycle, LifecycleStateAware, PartialAware, Keeperable {
	
	Endpoint masterEndPoint();
	
	ReplicationStoreManager getReplicationStoreManager();
	
	ReplicationStore getCurrentReplicationStore();

	void reconnect();

	void changeReplAddress(Endpoint address);

	RdbDumper createRdbDumper() throws CreateRdbDumperException;

	MASTER_STATE getMasterState();
	
	void setMasterState(MASTER_STATE masterState);

	String metaInfo();

}
