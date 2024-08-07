package com.ctrip.xpipe.service.datasource;

import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.config.AbstractConfigBean;

import static com.ctrip.xpipe.api.config.ConfigProvider.COMMON_CONFIG;

/**
 * @author lishanglin
 * date 2021/4/13
 */
public class DataSourceConfig extends AbstractConfigBean {

    public static final String KEY_DATASOURCE_CLUSTER = "dal.datasource.cluster";

    public DataSourceConfig() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(COMMON_CONFIG));
    }

    public String getClusterName() {
        return getProperty(KEY_DATASOURCE_CLUSTER, "fxxpipedb_dalcluster");
    }

}