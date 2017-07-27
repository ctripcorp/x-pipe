package com.ctrip.xpipe.redis.meta.server.rest;

import java.beans.PropertyEditorSupport;

import com.ctrip.xpipe.api.codec.Codec;

/**
 * @author wenchao.meng
 *
 * Aug 3, 2016
 */
public class ForwardInfoEditor extends PropertyEditorSupport{
	
	@Override
	public void setAsText(String text) throws IllegalArgumentException {
		setValue(Codec.DEFAULT.decode(text, ForwardInfo.class));
	}
	
	@Override
	public String getAsText() {
		return Codec.DEFAULT.encode(getValue());
	}
}
