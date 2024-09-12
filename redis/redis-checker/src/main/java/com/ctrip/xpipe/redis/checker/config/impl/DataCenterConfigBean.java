package com.ctrip.xpipe.redis.checker.config.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.config.ConfigProvider;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.config.AbstractConfigBean;
import com.ctrip.xpipe.zk.ZkConfig;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Configuration
public class DataCenterConfigBean extends AbstractConfigBean {

    public static final String KEY_ZK_ADDRESS  = "zk.address";

    public static final String KEY_ZK_NAMESPACE  = "zk.namespace";

    public static final String KEY_METASERVERS = "metaservers";

    public static final String KEY_CREDIS_SERVEICE_ADDRESS = "credis.service.address";

    public static final String KEY_CREDIS_IDC_MAPPING_RULE = "credis.service.idc.mapping.rule";

    public static final String KEY_CROSS_DC_LEADER_LEASE_NAME = "console.cross.dc.leader.lease.name";

    public static final String KEY_CHECKER_ACK_TIMEOUT_MILLI = "checker.ack.timeout.milli";

    public static final String KEY_FOUNDATION_GROUP_DC_MAP = "foundation.group.dc.map";

    public static final String KEY_CONSOLE_DOMAINS = "console.domains";

    public static final String KEY_BEACON_ORG_ROUTE = "beacon.org.routes";

    public static final String KEY_CONSOLE_NO_DB_DOMAIN = "console.no.db.domain";

    private AtomicReference<String> zkConnection = new AtomicReference<>();
    private AtomicReference<String> zkNameSpace = new AtomicReference<>();

    public DataCenterConfigBean() {
        super(ConfigProvider.DEFAULT.getOrCreateConfig(ConfigProvider.DATA_CENTER_CONFIG_NAME));
    }

    public String getConsoleNoDbDomain() {
        return getProperty(KEY_CONSOLE_NO_DB_DOMAIN, "");
    }

    public String getZkConnectionString() {
        return getProperty(KEY_ZK_ADDRESS, zkConnection.get() == null ? "127.0.0.1:2181" : zkConnection.get());
    }

    public String getZkNameSpace(){
        return getProperty(KEY_ZK_NAMESPACE, zkNameSpace.get() == null ? ZkConfig.DEFAULT_ZK_NAMESPACE:zkNameSpace.get());
    }

    public Map<String,String> getMetaservers() {
        String property = getProperty(KEY_METASERVERS, "{}");
        return JsonCodec.INSTANCE.decode(property, Map.class);
    }

    public String getCredisServiceAddress() {
        return getProperty(KEY_CREDIS_SERVEICE_ADDRESS, "localhost:8080");
    }

    public Map<String, String> getCredisIdcMappingRules() {
        return Codec.DEFAULT.decode(getProperty(KEY_CREDIS_IDC_MAPPING_RULE, "{}"), Map.class);
    }

    public String getCrossDcLeaderLeaseName() {
        return getProperty(KEY_CROSS_DC_LEADER_LEASE_NAME, "CROSS_DC_LEADER");
    }

    public int getCheckerAckTimeoutMilli() {
        return getIntProperty(KEY_CHECKER_ACK_TIMEOUT_MILLI, 60000);
    }

    public Map<String, String> getGroupDcMap() {
        String mappingRule = getProperty(KEY_FOUNDATION_GROUP_DC_MAP, "{}");
        return JsonCodec.INSTANCE.decode(mappingRule, Map.class);
    }

    public Map<String, String> getConsoleDomains() {
        String property = getProperty(KEY_CONSOLE_DOMAINS, "{}");
        return JsonCodec.INSTANCE.decode(property, Map.class);
    }

    public String getBeaconOrgRoutes() {
        return getProperty(KEY_BEACON_ORG_ROUTE, "[]");
    }
}
