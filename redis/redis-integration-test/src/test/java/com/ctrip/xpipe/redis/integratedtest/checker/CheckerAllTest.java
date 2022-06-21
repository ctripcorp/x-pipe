package com.ctrip.xpipe.redis.integratedtest.checker;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        TwoChecker2SameConsole.class,
        TestConsoleWeb.class,
        TestAllCheckerLeader.class,
        CheckCrdtRedisByProxy.class
})
public class CheckerAllTest {

    /* add -Djdk.attach.allowAttachSelf=true for jdk11*/
}
