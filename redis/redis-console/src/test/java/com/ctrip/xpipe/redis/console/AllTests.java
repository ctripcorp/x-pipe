package com.ctrip.xpipe.redis.console;

import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.xpipe.redis.console.dao.file.DefaultFileDaoTest;

import org.junit.runner.RunWith;

/**
 * @author wenchao.meng
 *
 * Jun 23, 2016
 */
@RunWith(Suite.class)
@SuiteClasses({
	DefaultFileDaoTest.class
})
public class AllTests {

}
