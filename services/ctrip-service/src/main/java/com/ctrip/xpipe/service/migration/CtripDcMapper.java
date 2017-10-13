package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.api.lifecycle.Ordered;
import com.ctrip.xpipe.api.migration.DcMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 07, 2017
 */
public class CtripDcMapper implements DcMapper{

    @Override
    public String getDc(String dcName) {

        if(dcName == null){
            return null;
        }
        Map<String, String> idsMappingRules = CredisConfig.INSTANCE.getCredisIdcMappingRules();
        return doMapping(dcName, idsMappingRules);
    }

    private String doMapping(String dcName, Map<String, String> mappingRules) {

        if(mappingRules.containsKey(dcName.toUpperCase())) {
            return mappingRules.get(dcName.toUpperCase());
        }

        return dcName;
    }

    @Override
    public String reverse(String otherDcName) {

        if(otherDcName == null){
            return null;
        }

        Map<String, String> mappingRules = CredisConfig.INSTANCE.getCredisIdcMappingRules();
        return doReverse(otherDcName, mappingRules);
    }

    protected String doReverse(String otherDcName, Map<String, String> mappingRules){

        Map<String, String> reverse = new HashMap<>();

        mappingRules.forEach(new BiConsumer<String, String>() {
            @Override
            public void accept(String key, String value) {
                reverse.put(value, key);
            }
        });
        return doMapping(otherDcName, reverse);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
