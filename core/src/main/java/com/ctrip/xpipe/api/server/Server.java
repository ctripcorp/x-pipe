package com.ctrip.xpipe.api.server;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午3:24:19
 */
public interface Server extends Lifecycle{
	
	public static enum SERVER_ROLE{
		MASTER,
		SLAVE,
		KEEPER;
		
		public String toString() {
			return super.toString().toLowerCase();
			
		};
	}

	SERVER_ROLE role();
}
