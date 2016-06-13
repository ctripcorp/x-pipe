package com.ctrip.xpipe.foundation;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.foundation.FoundationService;

/**
 * @author wenchao.meng
 *
 * Jun 13, 2016
 */
public class CtripFoundationService implements FoundationService{

	@Override
	public String getDataCenter() {
		
		return Foundation.server().getDataCenter();
	}

}
