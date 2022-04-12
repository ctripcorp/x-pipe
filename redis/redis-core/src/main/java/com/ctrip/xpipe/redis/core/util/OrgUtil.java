package com.ctrip.xpipe.redis.core.util;

/**
 * @author chen.zhu
 * <p>
 * Jun 13, 2018
 */
public class OrgUtil {

    public static boolean isDefaultOrg(Integer orgId) {
        return orgId == null || orgId <= 0L;
    }

    public static boolean isDefaultOrg(Long orgId) {
        return orgId == null || orgId <= 0L;
    }
}
