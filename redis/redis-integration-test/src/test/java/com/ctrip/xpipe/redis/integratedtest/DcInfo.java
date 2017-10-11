package com.ctrip.xpipe.redis.integratedtest;


import com.ctrip.xpipe.redis.core.entity.DcMeta;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 * Jun 24, 2016
 */
public class DcInfo {

	private ApplicationContext applicationContext;
	private AtomicInteger serverId = new AtomicInteger();
	private DcMeta dcMeta;

	public DcInfo(DcMeta dcMeta) {
		this.dcMeta = dcMeta;
	}

	public void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = applicationContext;
	}
	
	public ApplicationContext getApplicationContext() {
		return applicationContext;
	}
	
	public DcMeta getDcMeta() {
		return dcMeta;
	}
	
	public int getIncreaseServerId(){
		return serverId.incrementAndGet();
	}
}
