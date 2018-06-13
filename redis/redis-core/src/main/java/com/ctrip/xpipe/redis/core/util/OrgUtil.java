package com.ctrip.xpipe.redis.core.util;

/**
 * @author chen.zhu
 * <p>
 * Jun 13, 2018
 */
public class OrgUtil {

    public static boolean isDefaultOrg(long orgId) {
        return orgId <= 0L;
    }
}
