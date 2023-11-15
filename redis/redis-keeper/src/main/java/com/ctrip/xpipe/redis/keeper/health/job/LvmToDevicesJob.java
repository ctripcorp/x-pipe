package com.ctrip.xpipe.redis.keeper.health.job;

import com.ctrip.xpipe.utils.AbstractScriptExecutor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lishanglin
 * date 2023/11/10
 */
public class LvmToDevicesJob extends AbstractScriptExecutor<List<String>> {

    private String lvm;

    public LvmToDevicesJob(String lvm) {
        this.lvm = lvm;
    }

    @Override
    public String getScript() {
        return "lsblk -o NAME,TYPE -s -r -n " + lvm;
    }

    @Override
    public List<String> format(List<String> result) {
        List<String> devices = new ArrayList<>();
        for (String raw: result) {
            String[] strs = raw.split("\\s+");
            if (strs.length < 2) continue;
            if (strs[1].equals("disk")) devices.add(strs[0]);
        }

        return devices;
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
