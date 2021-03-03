package com.ctrip.xpipe.service.foundation;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/2
 */
public class FoundationConfig extends AbstractConfigBean {

    public static final String KEY_FOUNDATION_GROUP_DC_MAP = "foundation.group.dc.map";

    public Map<String, String> getGroupDcMap() {
        String mappingRule = getProperty(KEY_FOUNDATION_GROUP_DC_MAP, "{}");
        return JsonCodec.INSTANCE.decode(mappingRule, Map.class);
    }

}
