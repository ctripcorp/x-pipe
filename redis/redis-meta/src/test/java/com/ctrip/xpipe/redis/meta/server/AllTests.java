package com.ctrip.xpipe.redis.meta.server;






import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.meta.server.cluster.impl.ArrangeTaskTriggerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultClusterArrangerTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultCurrentClusterServerTest;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfoEditorTest;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultClusterServersTest;


/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:05:50 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	ArrangeTaskTriggerTest.class,
	DefaultClusterArrangerTest.class,
	DefaultClusterServersTest.class,
	DefaultCurrentClusterServerTest.class, 
	ForwardInfoEditorTest.class
})
public class AllTests {

}
