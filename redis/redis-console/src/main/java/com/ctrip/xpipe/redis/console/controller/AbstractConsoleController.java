package com.ctrip.xpipe.redis.console.controller;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.codec.Codec;

/**
 * @author shyin
 *
 * Sep 2, 2016
 */
public abstract class AbstractConsoleController {
	protected Logger logger = LoggerFactory.getLogger(getClass());
	protected Codec coder = Codec.DEFAULT;
	
	@SuppressWarnings("unchecked")
	protected <T> T valueOrDefault(Class<T> clazz, T result) {
		try {
			return (null == result) ? clazz.newInstance() : result;
		} catch (InstantiationException e) {
			return (T) new Object();
		} catch (IllegalAccessException e) {
			return (T) new Object();
		}
	}
	
	protected <T> List<T> valueOrEmptySet(Class<T> clazz, List<T> result) {
		return (null == result) ? new LinkedList<T>() : result;
	}
}
