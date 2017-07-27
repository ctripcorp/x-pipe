package com.ctrip.xpipe.redis.meta.server.rest.exception;

import com.ctrip.xpipe.redis.meta.server.cluster.SlotInfo;
import com.ctrip.xpipe.redis.meta.server.rest.ForwardInfo;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class MovingTargetException extends MetaRestException{

	private static final long serialVersionUID = 1L;

	public MovingTargetException(ForwardInfo forwardInfo, int currentServerId, SlotInfo slotInfo, String clusterId, int slotId) {
		super(String.format("forwardInfo:%s, currentServerId:%d, currentSlotInfo:%s(%s,%d), cluster:%s", forwardInfo, currentServerId, slotInfo, currentServerId, slotId, clusterId));
	}

}
