package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.utils.StringUtil;

import java.io.File;

/**
 * @author lishanglin
 * date 2023/11/10
 */
public class DiskSpaceUsageInfo {

    public String mountPath;

    public String source;

    public String type;

    public long size;

    public long use;

    public long available;


    public String getDevice() {
        if (StringUtil.isEmpty(source)) {
            return null;
        }

        String[] strs = StringUtil.splitRemoveEmpty(File.separator, source);
        if (0 == strs.length) return null;
        return strs[strs.length - 1];
    }

    @Override
    public String toString() {
        return "DiskSpaceUsageInfo{" +
                "mountPath='" + mountPath + '\'' +
                ", source='" + source + '\'' +
                ", type='" + type + '\'' +
                ", size=" + size +
                ", use=" + use +
                ", available=" + available +
                '}';
    }
}
