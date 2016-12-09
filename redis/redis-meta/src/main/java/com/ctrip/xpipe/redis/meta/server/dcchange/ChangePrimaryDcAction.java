package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService.PrimaryDcChangeMessage;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public interface ChangePrimaryDcAction {
	
	PrimaryDcChangeMessage changePrimaryDc(String clusterId, String shardId, String newPrimaryDc);

}
