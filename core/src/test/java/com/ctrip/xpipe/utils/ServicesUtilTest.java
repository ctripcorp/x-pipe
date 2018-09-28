package com.ctrip.xpipe.utils;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *
 * Jun 16, 2016
 */
public class ServicesUtilTest {
	
	@Test
	public void testFoundationService(){
		
		FoundationService foundationService = ServicesUtil.getFoundationService();
		Assert.assertTrue(foundationService instanceof DefaultFoundationService);
		
		FoundationService foundationService2 = ServicesUtil.getFoundationService();
		Assert.assertEquals(foundationService, foundationService2);
		
	}

}
