package com.ctrip.xpipe.simple;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration()
@DirtiesContext(classMode = ClassMode.AFTER_EACH_TEST_METHOD)
public class SpringTest implements ApplicationContextAware{
	
	@Test
	public void test1(){
		System.out.println(this);
	}
	
	@Test
	public void test2(){
		System.out.println(this);
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		System.out.println(applicationContext);
	}

	
	@Configuration
	public static class TestContext{
		
	}
}
