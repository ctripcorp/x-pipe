package com.ctrip.xpipe.redis.integratedtest;


import com.ctrip.xpipe.redis.integratedtest.checker.CheckerAllTest;
import com.ctrip.xpipe.redis.integratedtest.metaserver.MetaServerAllTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author wenchao.meng
 *
 * May 17, 2016 2:09:41 PM
 */
@RunWith(Suite.class)
@SuiteClasses({
	CheckerAllTest.class, 
	MetaServerAllTest.class,
})
public class AllTesters {

    /* add -Djdk.attach.allowAttachSelf=true for jdk11*/
}
