package com.ctrip.xpipe.redis.console.keeper;

import com.ctrip.xpipe.utils.StringUtil;

public enum KeeperContainerOverloadCause {
    PEER_DATA_OVERLoad,
    INPUT_FLOW_OVERLOAD,
    BOTH;

    public static KeeperContainerOverloadCause findByValue(String value) {
        if(StringUtil.isEmpty(value)) return null;
        String upperValue = value.toUpperCase();
        for (KeeperContainerOverloadCause keeperContainerOverloadCause : KeeperContainerOverloadCause.values()) {
            if (keeperContainerOverloadCause.name().equals(upperValue)) {
                return keeperContainerOverloadCause;
            }
        }
        throw new IllegalArgumentException("no DcGroupType for value " + value);
    }
}
