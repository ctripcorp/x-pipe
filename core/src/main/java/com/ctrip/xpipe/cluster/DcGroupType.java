package com.ctrip.xpipe.cluster;

/**
 * @author ayq
 * <p>
 * 2022/6/29 10:55
 */
public enum DcGroupType {

    MASTER(false, "MASTER"),
    DR_MASTER(true, "DRMaster");

    private boolean value;
    private String desc;

    DcGroupType(boolean value, String desc) {
        this.value = value;
        this.desc = desc;
    }

    public boolean isValue() {
        return value;
    }

    public String getDesc() {
        return desc;
    }

    public static DcGroupType findByValue(boolean value) {
        for (DcGroupType dcGroupType : DcGroupType.values()) {
            if (dcGroupType.value == value) {
                return dcGroupType;
            }
        }
        throw new IllegalArgumentException("no DcGroupType for value " + value);
    }


    public static DcGroupType findByDesc(String desc) {
        for (DcGroupType dcGroupType : DcGroupType.values()) {
            if (dcGroupType.desc.equalsIgnoreCase(desc)) {
                return dcGroupType;
            }
        }
        throw new IllegalArgumentException("no DcGroupType for desc " + desc);
    }
}
