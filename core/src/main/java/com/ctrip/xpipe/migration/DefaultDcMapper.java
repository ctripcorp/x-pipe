package com.ctrip.xpipe.migration;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.migration.DcMapper;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 07, 2017
 */
public class DefaultDcMapper implements DcMapper{

    @Override
    public String getDc(String dcName) {
        return dcName;
    }

    @Override
    public String reverse(String otherDcName) {
        return otherDcName;
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
