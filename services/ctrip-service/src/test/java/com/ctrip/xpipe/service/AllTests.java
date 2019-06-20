package com.ctrip.xpipe.service;

import com.ctrip.xpipe.service.fireman.XPipeFiremanDependencyTest;
import com.ctrip.xpipe.service.fireman.XPipeTemporaryDependencyTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        XPipeTemporaryDependencyTest.class,
        XPipeFiremanDependencyTest.class
})
public class AllTests {
}
