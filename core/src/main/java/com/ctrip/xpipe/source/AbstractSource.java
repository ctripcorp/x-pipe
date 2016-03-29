package com.ctrip.xpipe.source;



import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.api.source.Source;

public abstract class AbstractSource implements Source{
	
	protected Logger logger = LogManager.getLogger(getClass());
	
}
