package com.ctrip.xpipe.redis.meta.server.cluster;

/**
 * @author wenchao.meng
 *
 * Jul 22, 2016
 */
public interface Exportable {
	
	ExportFuture exportSlot(int slot);

}
