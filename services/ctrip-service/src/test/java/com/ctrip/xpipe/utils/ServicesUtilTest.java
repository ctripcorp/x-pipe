package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.service.email.CtripPlatformEmailService;
import com.ctrip.xpipe.service.organization.CtripOrganizationService;
import org.junit.Assert;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.service.foundation.CtripFoundationService;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class ServicesUtilTest extends AbstractTest{
	
	@Test
	public void test(){
		
		Assert.assertTrue(ServicesUtil.getFoundationService() instanceof CtripFoundationService);
		Assert.assertTrue(ServicesUtil.getOrganizationService() instanceof CtripOrganizationService);
		Assert.assertTrue(ServicesUtil.getEmailService() instanceof CtripPlatformEmailService);
	}

}
