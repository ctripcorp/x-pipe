package com.ctrip.xpipe.source;



import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.api.source.Source;

public abstract class AbstractSource implements Source{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
}
