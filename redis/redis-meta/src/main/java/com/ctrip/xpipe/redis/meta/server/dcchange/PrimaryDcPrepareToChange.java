package com.ctrip.xpipe.redis.meta.server.dcchange;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 10, 2017
 */
public interface PrimaryDcPrepareToChange {

    void prepare(String clusterId, String shardId);

    void deprepare(String clusterId, String shardId);


}
