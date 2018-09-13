package com.ctrip.xpipe.redis.console.healthcheck.meta;

import com.ctrip.xpipe.redis.console.config.impl.DefaultConsoleConfig;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Sep 12, 2018
 */
@Component
public class DcIgnoredConfigListener extends AbstractHealthCheckConfigListener<String[]> {

    @Autowired
    private MetaChangeManager metaChangeManager;

    @Override
    protected String[] convert(String value) {
        return StringUtil.splitRemoveEmpty("\\s*,\\s*", value);
    }

    @Override
    protected void doOnChange(String[] oldValue, String[] newValue) {
        Pair<Set<String>, Set<String>> delAndAdd = getDiff(oldValue, newValue);

        // deleted ignores
        for(String dcId : delAndAdd.getKey()) {
            metaChangeManager.startIfPossible(dcId);
        }

        // add ignores
        for(String dcId : delAndAdd.getValue()) {
            metaChangeManager.ignore(dcId);
        }
    }



    @Override
    public List<String> supportsKeys() {
        return Lists.newArrayList(DefaultConsoleConfig.KEY_IGNORED_DC_FOR_HEALTH_CHECK);
    }
}
