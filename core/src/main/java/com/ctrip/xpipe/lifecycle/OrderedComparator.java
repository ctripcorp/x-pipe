package com.ctrip.xpipe.lifecycle;

import com.ctrip.xpipe.api.lifecycle.Ordered;

import java.util.Comparator;

/**
 * @author wenchao.meng
 *
 *         Aug 22, 2016
 */
public class OrderedComparator implements Comparator<Ordered> {

	@Override
	public int compare(Ordered o1, Ordered o2) {
		return (o1.getOrder() < o2.getOrder()) ? -1 : (o1.getOrder() == o2.getOrder() ? 0 : 1);
	}
}
