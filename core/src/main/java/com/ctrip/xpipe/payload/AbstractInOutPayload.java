package com.ctrip.xpipe.payload;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.api.payload.InOutPayload;

/**
 * @author wenchao.meng
 *
 * 2016年3月29日 下午4:31:30
 */
public abstract class AbstractInOutPayload implements InOutPayload{
	
	protected Logger logger = LogManager.getLogger(getClass());

	

}
