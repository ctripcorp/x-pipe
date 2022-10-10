package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author ayq
 * <p>
 * 2022/6/29 10:55
 */
public enum DcGroupType {

    MASTER,
    DR_MASTER;

    public static boolean isSameGroupType(String source, DcGroupType dcGroupType) {
        return dcGroupType.toString().equalsIgnoreCase(source);
    }

    public static boolean isNullOrDrMaster(String source) {
        return source == null || DR_MASTER.toString().equalsIgnoreCase(source);
    }

    public static DcGroupType findByValue(String value) {
        if(StringUtil.isEmpty(value)) return null;
        String upperValue = value.toUpperCase();
        for (DcGroupType dcGroupType : DcGroupType.values()) {
            if (dcGroupType.name().equals(upperValue)) {
                return dcGroupType;
            }
        }
        throw new IllegalArgumentException("no DcGroupType for value " + value);
    }
}
