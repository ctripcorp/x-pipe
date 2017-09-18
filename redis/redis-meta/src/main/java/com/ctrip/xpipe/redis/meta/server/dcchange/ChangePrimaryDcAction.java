package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterInfo;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public interface ChangePrimaryDcAction {
	
	PrimaryDcChangeMessage changePrimaryDc(String clusterId, String shardId, String newPrimaryDc, MasterInfo masterInfo);

}
