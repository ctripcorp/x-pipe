package com.ctrip.xpipe.redis.keeper.health.job;

import com.ctrip.xpipe.redis.core.entity.DiskSpaceUsageInfo;
import com.ctrip.xpipe.utils.AbstractScriptExecutor;

import java.util.List;

/**
 * @author lishanglin
 * date 2023/11/10
 */
public class DiskSpaceUsageCheckJob extends AbstractScriptExecutor<DiskSpaceUsageInfo> {

    private String file;

    public DiskSpaceUsageCheckJob(String path) {
        this.file = path;
    }

    @Override
    public String getScript() {
        return "df --output=source,fstype,size,used,avail,target " + file;
    }

    @Override
    public DiskSpaceUsageInfo format(List<String> result) {
        if (result.size() < 2) return null;
        String[] strs = result.get(1).split("\\s+");
        if (strs.length < 6) return null;

        DiskSpaceUsageInfo info = new DiskSpaceUsageInfo();
        info.source = strs[0];
        info.type = strs[1];
        info.size = Long.parseLong(strs[2]);
        info.use = Long.parseLong(strs[3]);
        info.available = Long.parseLong(strs[4]);
        info.mountPath = strs[5];
        return info;
    }

    @Override
    protected void doReset() {
        // do nothing
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

}
