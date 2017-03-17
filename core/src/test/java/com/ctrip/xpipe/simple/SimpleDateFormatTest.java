package com.ctrip.xpipe.simple;

import java.util.Date;

import org.apache.commons.lang3.time.FastDateFormat;
import org.junit.Test;

import com.ctrip.xpipe.AbstractTest;

/**
 * @author wenchao.meng
 *
 * Mar 16, 2017
 */
public class SimpleDateFormatTest extends AbstractTest{
	
	@Test
	public void testFormat(){
		
		
		logger.info("{}", FastDateFormat.getInstance("yyyy-MM-dd'T'HH:mm:ss.SSS").format(new Date()));
		
		
	}

}
