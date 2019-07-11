package com.ctrip.xpipe.service;

import com.ctrip.xpipe.service.fireman.XPipeFiremanDependencyTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses(value = {
        XPipeFiremanDependencyTest.class
})
public class AllTests {
}
