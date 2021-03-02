package com.ctrip.xpipe.service;

import com.ctrip.xpipe.service.beacon.BeaconServiceTest;
import com.ctrip.xpipe.service.fireman.XPipeFiremanDependencyTest;
import com.ctrip.xpipe.service.foundation.CtripFoundationServiceTest;
import com.ctrip.xpipe.service.metric.HickwallClientTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        XPipeFiremanDependencyTest.class,
        HickwallClientTest.class,
        BeaconServiceTest.class,
        CtripFoundationServiceTest.class
})
public class AllTests {
}
