package com.ctrip.xpipe.api.migration;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.utils.ServicesUtil;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 07, 2017
 */
public interface DcMapper extends Ordered{

    DcMapper INSTANCE = ServicesUtil.getDcMapperService();

    String getDc(String dcName);

    String reverse(String otherDcName);
}
