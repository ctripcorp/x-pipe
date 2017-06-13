package com.ctrip.xpipe.cluster;

import java.util.List;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 13, 2017
 */
public interface ClusterServer {

    boolean amILeader();

    List<String> getAllServers() throws Exception;

}
