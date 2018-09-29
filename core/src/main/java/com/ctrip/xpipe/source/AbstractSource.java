package com.ctrip.xpipe.source;


import com.ctrip.xpipe.api.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractSource implements Source{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());
	
}
