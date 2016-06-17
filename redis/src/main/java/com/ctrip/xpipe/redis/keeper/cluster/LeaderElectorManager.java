/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.cluster;

/**
 * @author marsqing
 *
 *         Jun 17, 2016 4:49:11 PM
 */
public interface LeaderElectorManager {

	LeaderElector createLeaderElector(ElectContext ctx);

}
