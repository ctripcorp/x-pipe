/**
 * 
 */
package com.ctrip.xpipe.api.cluster;

import com.ctrip.xpipe.cluster.ElectContext;

/**
 * @author marsqing
 *
 *         Jun 17, 2016 4:49:11 PM
 */
public interface LeaderElectorManager {

	LeaderElector createLeaderElector(ElectContext ctx);

}
