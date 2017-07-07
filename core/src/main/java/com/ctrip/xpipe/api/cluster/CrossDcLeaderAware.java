package com.ctrip.xpipe.api.cluster;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 14, 2017
 */
public interface CrossDcLeaderAware {

    void isCrossDcLeader();

    void notCrossDcLeader();

}
