package com.ctrip.xpipe.simple;

import com.ctrip.xpipe.AbstractTest;
import org.apache.commons.lang3.time.FastDateFormat;
import org.junit.Test;

import java.util.Date;

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
