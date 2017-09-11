package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 10, 2017
 */
public interface PrimaryDcPrepareToChange {

    MetaServerConsoleService.PreviousPrimaryDcMessage prepare(String clusterId, String shardId);

    MetaServerConsoleService.PreviousPrimaryDcMessage deprepare(String clusterId, String shardId);

}
