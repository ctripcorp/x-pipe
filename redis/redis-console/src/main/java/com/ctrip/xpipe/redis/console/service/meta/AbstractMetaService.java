package com.ctrip.xpipe.redis.console.service.meta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.codec.JsonCodec;

/**
 * @author shyin
 *
 * Sep 1, 2016
 */
public abstract class AbstractMetaService {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	protected static Codec coder = new JsonCodec();
}
