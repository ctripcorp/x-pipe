package com.ctrip.xpipe.redis.meta.server.dcchange;

import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 10, 2017
 */
public interface PrimaryDcPrepareToChange {

    MetaServerConsoleService.PreviousPrimaryDcMessage prepare(Long clusterDbId, Long shardDbId);

    MetaServerConsoleService.PreviousPrimaryDcMessage deprepare(Long clusterDbId, Long shardDbId);

}
