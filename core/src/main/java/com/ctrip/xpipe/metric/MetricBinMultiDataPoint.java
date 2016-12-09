package com.ctrip.xpipe.metric;

import java.util.LinkedList;
import java.util.List;

/**
 * @author marsqing
 *
 *         Dec 8, 2016 3:44:50 PM
 */
public class MetricBinMultiDataPoint {

	private List<MetricDataPoint> points = new LinkedList<>();

	public void addToPoints(MetricDataPoint point) {
		points.add(point);
	}

	public List<MetricDataPoint> getPoints() {
		return points;
	}

}
