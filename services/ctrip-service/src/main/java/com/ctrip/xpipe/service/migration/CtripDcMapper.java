package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.migration.DcMapper;

import java.util.Map;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 07, 2017
 */
public class CtripDcMapper implements DcMapper{

    @Override
    public String getDc(String dcName) {
        Map<String, String> idsMappingRules = MigrationPublishServiceConfig.INSTANCE.getCredisIdcMappingRules();
        if(idsMappingRules.containsKey(dcName.toUpperCase())) {
            return idsMappingRules.get(dcName.toUpperCase());
        } else {
            return dcName;
        }
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
