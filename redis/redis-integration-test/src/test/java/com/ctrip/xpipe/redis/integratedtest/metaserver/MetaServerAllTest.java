package com.ctrip.xpipe.redis.integratedtest.metaserver;


import com.ctrip.xpipe.redis.integratedtest.metaserver.scenes.CrdtPeerMasterChangeTest;
import com.ctrip.xpipe.redis.integratedtest.metaserver.scenes.RouteChangeTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        RouteChangeTest.class,
        CrdtPeerMasterChangeTest.class
})
public class MetaServerAllTest {

}
