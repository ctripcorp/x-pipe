package com.ctrip.xpipe.metric;

import com.ctrip.xpipe.api.lifecycle.Ordered;

/**
 * @author marsqing
 *
 *         Dec 8, 2016 4:40:38 PM
 */
public class DummyMetricProxy implements MetricProxy {

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}

	@Override
	public void writeBinMultiDataPoint(MetricBinMultiDataPoint bmp) throws MetricProxyException {
	}

}
