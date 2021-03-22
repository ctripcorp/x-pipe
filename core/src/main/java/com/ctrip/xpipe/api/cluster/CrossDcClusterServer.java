package com.ctrip.xpipe.api.cluster;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 14, 2017
 */
public interface CrossDcClusterServer {

    boolean amILeader();

    List<String> getAllServers();

}
