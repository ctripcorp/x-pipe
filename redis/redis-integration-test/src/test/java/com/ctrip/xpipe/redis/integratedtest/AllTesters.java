package com.ctrip.xpipe.redis.integratedtest;


import com.ctrip.xpipe.redis.integratedtest.console.TestShutDown;
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
	TestShutDown.class
})
public class AllTesters {

}
