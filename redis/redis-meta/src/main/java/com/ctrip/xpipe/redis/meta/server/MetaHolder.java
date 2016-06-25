/**
 * 
 */
package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;


/**
 * @author marsqing
 *
 *         Jun 12, 2016 3:12:56 PM
 */
public interface MetaHolder extends Lifecycle, Observable {

	XpipeMeta getMeta();

}
