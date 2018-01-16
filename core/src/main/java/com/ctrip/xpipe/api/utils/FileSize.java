package com.ctrip.xpipe.api.utils;

import java.util.function.LongSupplier;

/**
 * control the size returned
 * @author wenchao.meng
 *
 * Jan 5, 2017
 */
public interface FileSize {
	
	long getSize(LongSupplier realSizeProvider) ;

}
