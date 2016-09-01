package com.ctrip.xpipe.api.lifecycle;

/**
 * @author wenchao.meng
 *
 * 2016年4月21日 下午4:59:03
 */
public interface Lifecycle extends Initializable, Startable, Stoppable, Disposable, LifecycleStateAware, Ordered{
	
}
