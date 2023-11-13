package com.ctrip.xpipe.redis.keeper.health.job;

import com.ctrip.xpipe.redis.core.entity.DiskIOStatInfo;
import com.ctrip.xpipe.utils.AbstractScriptExecutor;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.List;

/**
 * @author lishanglin
 * date 2023/11/10
 */
public class DiskIOStatCheckJob extends AbstractScriptExecutor<DiskIOStatInfo> {

    private String device;

    public DiskIOStatCheckJob(String device) {
        this.device = device;
    }

    @Override
    public String getScript() {
        return "iostat -dx 3 2 -N " + device;
    }

    @Override
    public DiskIOStatInfo format(List<String> result) {
        for (int i = result.size() - 1; i > 0; i--) {
            if (StringUtil.isEmpty(result.get(i))) continue;
            if (result.get(i).startsWith("Device: ")) continue;
            getLogger().debug("[format] {}", result.get(i));
            return DiskIOStatInfo.parse(result.get(i));
        }
        return null;
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
