package com.ctrip.xpipe.redis.console.health.hickwall;

import org.apache.thrift.TException;

import com.ctrip.hickwall.protocol.BinMultiDataPoint;

/**
 * @author marsqing
 *
 *         Dec 7, 2016 12:04:00 AM
 */
public interface HickwallProxy {

	void writeBinMultiDataPoint(BinMultiDataPoint bmp) throws TException;

}
