package com.ctrip.xpipe.api.cluster;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
public interface ClusterServer {

    boolean amILeader();

    List<String> getAllServers();

}
